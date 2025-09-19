import os
import json
import sys
import datetime as dt
import requests
from pathlib import Path
import logging
from filelock import FileLock  # For file locking to prevent race conditions

# -------- LOGGING SETUP --------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# -------- CONFIG --------
LAT = float(os.getenv("FISH_LAT", "52.414"))  # Aberystwyth
LON = float(os.getenv("FISH_LON", "-4.082"))
TZ = os.getenv("FISH_TZ", "Europe/London")

# Pushover (required)
PUSHOVER_TOKEN = os.getenv("PUSHOVER_TOKEN", "")
PUSHOVER_USER = os.getenv("PUSHOVER_USER", "")

# WorldTides (optional)
WORLDTIDES_KEY = os.getenv("WORLDTIDES_KEY", "")

# -------- SCORING HELPERS --------
def bass_sst_score(c):
    if c >= 13.0: return 2
    if 12.0 <= c < 13.0: return 1
    return 0

def cod_sst_score(c):
    if 8.0 <= c <= 10.5: return 2
    if 11.0 <= c <= 12.5: return 1
    return 0

def wind_swell_ok(wind_kt, swell_m):
    return 2 if (swell_m <= 1.6 and wind_kt <= 18) else (1 if (swell_m <= 2.2 and wind_kt <= 24) else 0)

def pressure_trend_score(trend_hpa):
    return 2 if trend_hpa < -1.0 else (1 if abs(trend_hpa) <= 1.0 else 0)

def label_from_score(x):
    if x >= 10: return "GREEN"
    if 7 <= x <= 9: return "AMBER"
    if 4 <= x <= 6: return "AMBER-"
    return "RED"

# -------- DATA FETCHERS --------
def fetch_openmeteo():
    start = dt.datetime.utcnow().date()
    end = (dt.datetime.utcnow() + dt.timedelta(days=2)).date()
    url = (
        "https://marine-api.open-meteo.com/v1/marine"
        f"?latitude={LAT}&longitude={LON}"
        "&hourly=sea_surface_temperature,wind_speed_10m,wind_direction_10m,wave_height,wave_period,pressure_msl"
        f"&start_date={start}&end_date={end}&timezone=UTC"
    )
    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        return r.json()
    except requests.RequestException as e:
        logger.error(f"Failed to fetch Open-Meteo data: {e}")
        return {"hourly": {}}

def fetch_worldtides_extremes():
    if not WORLDTIDES_KEY:
        logger.warning("WorldTides API key missing; using approximate tides.")
        return []
    url = f"https://www.worldtides.info/api?extremes&lat={LAT}&lon={LON}&days=2&key={WORLDTIDES_KEY}"
    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        data = r.json()
        highs = []
        for ex in data.get("extremes", []):
            if ex.get("type", "").lower() == "high":
                t = dt.datetime.fromisoformat(ex["date"]).replace(tzinfo=dt.timezone.utc)
                highs.append(t)
        return sorted(highs)
    except requests.RequestException as e:
        logger.error(f"Failed to fetch WorldTides data: {e}")
        return []

def civil_twilight_for_day(date_utc):
    url = (
        f"https://api.open-meteo.com/v1/forecast?latitude={LAT}&longitude={LON}"
        f"&daily=sunrise,sunset&timezone=UTC&start_date={date_utc}&end_date={date_utc}"
    )
    try:
        r = requests.get(url, timeout=20)
        r.raise_for_status()
        d = r.json()["daily"]
        sr = dt.datetime.fromisoformat(d["sunrise"][0]).replace(tzinfo=dt.timezone.utc)
        ss = dt.datetime.fromisoformat(d["sunset"][0]).replace(tzinfo=dt.timezone.utc)
        dawn = (sr - dt.timedelta(minutes=30), sr + dt.timedelta(minutes=30))
        dusk = (ss - dt.timedelta(minutes=30), ss + dt.timedelta(minutes=30))
        return dawn, dusk
    except requests.RequestException as e:
        logger.error(f"Failed to fetch twilight data: {e}")
        return None, None

# -------- UTIL --------
def overlaps(a_start, a_end, b_start, b_end):
    return max(a_start, b_start) < min(a_end, b_end)

def pick_flood_windows(high_tides):
    return [(ht - dt.timedelta(hours=2), ht) for ht in high_tides]

# -------- PUSHOVER --------
def send_push(rec, band):
    if not (PUSHOVER_TOKEN and PUSHOVER_USER):
        logger.error("Pushover not configured: missing token or user key.")
        return False
    now_str = dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    tip = "Bass: lug+squid in coloured surf. Cod: lug/squid wraps."
    
    # Convert wind direction from degrees to cardinal direction
    wind_dir_deg = rec.get("wind_dir", 225)  # Default to SW (225°) if missing
    cardinal_dirs = ["N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE", 
                     "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW"]
    wind_dir = cardinal_dirs[int((wind_dir_deg % 360) / 22.5 + 0.5) % 16]
    
    body = (
        f"Date/Time: {now_str}\n"
        f"SST: {rec['sst']:.1f} °C\n\n"
        f"Next Window ({band}):\n"
        f"• Flood overlap: {rec['start'].strftime('%H:%M')}–{rec['end'].strftime('%H:%M')} ({rec['label']})\n"
        f"• Best mark: Surf beach\n\n"
        f"Wind/Swell/Pressure: {wind_dir} {rec['wind_kt']:.0f} kt, {rec['wave_m']:.1f} m @ ~{rec['wavep']:.0f} s, pressure trend factored\n"
        f"Scores: Bass {rec['bass']}/12 ({label_from_score(rec['bass'])}), Cod {rec['cod']}/12 ({label_from_score(rec['cod'])})\n"
        f"Tip: {tip}"
    )
    try:
        response = requests.post(
            "https://api.pushover.net/1/messages.json",
            data={
                "token": PUSHOVER_TOKEN,
                "user": PUSHOVER_USER,
                "title": "⚓ Aberystwyth Session Alert",
                "message": body
            },
            timeout=15
        )
        response.raise_for_status()
        logger.info(f"Sent {band} push notification for window {rec['start'].strftime('%H:%M')}–{rec['end'].strftime('%H:%M')}")
        return True
    except requests.RequestException as e:
        logger.error(f"Failed to send Pushover notification: {e}")
        return False

# -------- MAIN --------
def main():
    # Load state
    state_path = Path("state.json")
    lock_path = Path("state.json.lock")
    state = {}
    with FileLock(lock_path):
        if state_path.exists():
            try:
                state = json.loads(state_path.read_text())
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse state.json: {e}")
                state = {}

    today = dt.datetime.utcnow().date().isoformat()
    sent_green_today = state.get(today, {}).get("green", False)
    sent_amber_today = state.get(today, {}).get("amber", False)

    # Fetch data
    met = fetch_openmeteo()
    highs = fetch_worldtides_extremes()

    # Hourly data
    tstrs = met["hourly"].get("time", [])
    sst = met["hourly"].get("sea_surface_temperature", [])
    wind = met["hourly"].get("wind_speed_10m", [])
    wind_dir = met["hourly"].get("wind_direction_10m", [])  # Added wind direction
    waveh = met["hourly"].get("wave_height", [])
    wavep = met["hourly"].get("wave_period", [])
    psl = met["hourly"].get("pressure_msl", [])

    hours = [dt.datetime.fromisoformat(t).replace(tzinfo=dt.timezone.utc) for t in tstrs]
    now = dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc)

    # Pressure trend
    try:
        p_now = psl[-1]
        p_prev = psl[len(psl) // 2]
        p_trend = (p_now - p_prev) if (p_now and p_prev) else 0.0
    except:
        p_trend = 0.0
        logger.warning("Failed to calculate pressure trend; using 0.0.")

    # Fallback tides if no API key
    if not highs:
        approx_hts = []
        for d in [now.date(), (now + dt.timedelta(days=1)).date()]:
            approx_hts += [
                dt.datetime(d.year, d.month, d.day, 6, 0, tzinfo=dt.timezone.utc),
                dt.datetime(d.year, d.month, d.day, 18, 0, tzinfo=dt.timezone.utc)
            ]
        highs = approx_hts

    flood_windows = pick_flood_windows(highs)

    # Build candidate windows
    windows = []
    for day in {now.date(), (now + dt.timedelta(days=1)).date()}:
        dawn, dusk = civil_twilight_for_day(day.isoformat())
        if dawn is None or dusk is None:
            continue
        for fw_start, fw_end in flood_windows:
            if fw_start.date() != day:
                continue
            label = None
            if overlaps(fw_start, fw_end, *dawn):
                label = "dawn"
            if overlaps(fw_start, fw_end, *dusk):
                label = "dusk" if label is None else label
            if not label:
                continue
            if fw_end < now or fw_start > now + dt.timedelta(hours=24):
                continue
            windows.append((fw_start, fw_end, label))

    # Score
    scored = []
    for start, end, label in windows:
        mid = start + (end - start) / 2
        idx = min(range(len(hours)), key=lambda i: abs(hours[i] - mid))
        val_sst = sst[idx] if idx < len(sst) else 12.0
        val_wind = wind[idx] if idx < len(wind) else 6.0
        val_wind_dir = wind_dir[idx] if idx < len(wind_dir) else 225  # Default SW
        val_wave = waveh[idx] if idx < len(waveh) else 1.2
        val_wavep = wavep[idx] if idx < len(wavep) else 9.0
        wind_kt = val_wind * 1.94384

        b = bass_sst_score(val_sst) + 2 + wind_swell_ok(wind_kt, val_wave) + pressure_trend_score(p_trend) + 1
        c = cod_sst_score(val_sst)
        c += 2 if (0.8 <= val_wave <= 1.8) else (1 if val_wave > 1.8 else 0)
        c += 2 + pressure_trend_score(p_trend) + 1

        scored.append({
            "start": start, "end": end, "label": label,
            "sst": val_sst, "wind_kt": wind_kt, "wind_dir": val_wind_dir,
            "wave_m": val_wave, "wavep": val_wavep,
            "bass": int(b), "cod": int(c)
        })

    if not scored:
        logger.info("No qualifying windows.")
        return 0

    def best_score(rec): return max(rec["bass"], rec["cod"])
    greens = [r for r in scored if best_score(r) >= 10]
    ambers = [r for r in scored if 7 <= best_score(r) <= 9]

    def pick_best(lst):
        if not lst:
            return None
        lst = sorted(lst, key=lambda r: (-best_score(r), r["start"]))
        return lst[0]

    pick_green = pick_best(greens)
    pick_amber = pick_best(ambers)

    updated = False
    with FileLock(lock_path):
        if pick_green and not sent_green_today:
            if send_push(pick_green, "GREEN"):
                state.setdefault(today, {})["green"] = True
                updated = True
        if pick_amber and not sent_amber_today:
            if send_push(pick_amber, "AMBER"):
                state.setdefault(today, {})["amber"] = True
                updated = True

        if updated:
            try:
                state_path.write_text(json.dumps(state, indent=2))
                logger.info("State updated and saved.")
            except OSError as e:
                logger.error(f"Failed to write state.json: {e}")

    if updated:
        logger.info("Alerts sent.")
    else:
        logger.info("No new alerts.")

    return 0

if __name__ == "__main__":
    sys.exit(main())
