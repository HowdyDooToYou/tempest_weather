import sqlite3
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import math
import streamlit as st

DB_PATH = "data/tempest.db"
LOCAL_TZ = "America/New_York"

st.set_page_config(
    page_title="Tempest Air & Weather",
    layout="centered",
)

st.title("Tempest Air & Weather")
st.caption("Fast view: latest readings only.")


def db_row(query, params=None):
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cur = conn.execute(query, params or {})
        row = cur.fetchone()
        return dict(row) if row else None
    except sqlite3.Error as exc:
        st.warning(f"Database query failed: {exc}")
        return None
    finally:
        if conn is not None:
            conn.close()


def resolve_airlink_table():
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = {row[0] for row in cur.fetchall()}
        conn.close()
    except sqlite3.Error:
        return None
    for name in ("airlink_current_obs", "airlink_obs"):
        if name in tables:
            return name
    return None


def to_local_dt(epoch):
    if epoch is None:
        return None
    return datetime.fromtimestamp(int(epoch), tz=timezone.utc).astimezone(ZoneInfo(LOCAL_TZ))


def format_dt(epoch):
    dt = to_local_dt(epoch)
    if not dt:
        return "--"
    return dt.strftime("%Y-%m-%d %I:%M %p")


def age_minutes(epoch):
    if epoch is None:
        return None
    return (datetime.now(tz=timezone.utc).timestamp() - int(epoch)) / 60.0


def fmt(value, fmt_str="{:.1f}"):
    if value is None:
        return "--"
    try:
        return fmt_str.format(float(value))
    except Exception:
        return "--"


def c_to_f(c):
    if c is None:
        return None
    return (float(c) * 9 / 5) + 32


def hpa_to_inhg(hpa):
    if hpa is None:
        return None
    return float(hpa) * 0.0295299830714


def mps_to_mph(mps):
    if mps is None:
        return None
    return float(mps) * 2.2369362921


def heat_index_f(temp_f, humidity):
    if temp_f is None or humidity is None:
        return None
    t = float(temp_f)
    r = float(humidity)
    simple = 0.5 * (t + 61.0 + ((t - 68.0) * 1.2) + (r * 0.094))
    if t < 80 or r < 40:
        return simple
    hi = (
        -42.379
        + 2.04901523 * t
        + 10.14333127 * r
        - 0.22475541 * t * r
        - 6.83783e-3 * t * t
        - 5.481717e-2 * r * r
        + 1.22874e-3 * t * t * r
        + 8.5282e-4 * t * r * r
        - 1.99e-6 * t * t * r * r
    )
    if r < 13 and 80 <= t <= 112:
        hi -= ((13 - r) / 4) * ((17 - abs(t - 95)) / 17)
    elif r > 85 and 80 <= t <= 87:
        hi += ((r - 85) / 10) * ((87 - t) / 5)
    return hi


def pm25_aqi(pm_value):
    if pm_value is None:
        return None
    pm = float(pm_value)
    breakpoints = [
        (0.0, 12.0, 0, 50),
        (12.1, 35.4, 51, 100),
        (35.5, 55.4, 101, 150),
        (55.5, 150.4, 151, 200),
        (150.5, 250.4, 201, 300),
        (250.5, 350.4, 301, 400),
        (350.5, 500.4, 401, 500),
    ]
    for c_low, c_high, a_low, a_high in breakpoints:
        if c_low <= pm <= c_high:
            return (a_high - a_low) / (c_high - c_low) * (pm - c_low) + a_low
    return 500.0


def aqi_category(aqi):
    if aqi is None or math.isnan(aqi):
        return "--"
    if aqi <= 50:
        return "Good"
    if aqi <= 100:
        return "Moderate"
    if aqi <= 150:
        return "Unhealthy for Sensitive Groups"
    if aqi <= 200:
        return "Unhealthy"
    if aqi <= 300:
        return "Very Unhealthy"
    return "Hazardous"


def status_label(age_min):
    if age_min is None:
        return "No recent data"
    if age_min <= 10:
        return "Live"
    if age_min <= 60:
        return "Delayed"
    return "Stale"


def status_color(age_min):
    if age_min is None:
        return "error"
    if age_min <= 10:
        return "success"
    if age_min <= 60:
        return "warning"
    return "error"


# Latest Tempest

tempest = db_row(
    """
    SELECT
        obs_epoch,
        air_temperature,
        relative_humidity,
        station_pressure,
        wind_avg,
        wind_gust,
        wind_dir,
        rain_accumulated,
        lightning_strike_count,
        battery,
        solar_radiation,
        uv
    FROM obs_st
    ORDER BY obs_epoch DESC
    LIMIT 1
    """
)

# Latest AirLink

AIRLINK_TABLE = resolve_airlink_table()
if AIRLINK_TABLE:
    airlink = db_row(
        f"""
        SELECT
            ts,
            temp_f,
            hum,
            dew_point_f,
            wet_bulb_f,
            heat_index_f,
            pm_1,
            pm_2p5,
            pm_10,
            pm_2p5_nowcast,
            pm_2p5_last_1_hour,
            pm_2p5_last_24_hours,
            pct_pm_data_nowcast,
            pct_pm_data_last_1_hour,
            pct_pm_data_last_24_hours
        FROM {AIRLINK_TABLE}
        ORDER BY ts DESC
        LIMIT 1
        """
    )
else:
    airlink = None

# ------------------------
# Tempest summary
# ------------------------
col1, col2 = st.columns(2)

with col1:
    st.subheader("Tempest Station")
    if not tempest:
        st.info("No Tempest data available.")
    else:
        temp_f = c_to_f(tempest.get("air_temperature"))
        humidity = tempest.get("relative_humidity")
        heat_f = heat_index_f(temp_f, humidity)
        pressure = hpa_to_inhg(tempest.get("station_pressure"))
        wind = mps_to_mph(tempest.get("wind_avg"))
        gust = mps_to_mph(tempest.get("wind_gust"))
        last_epoch = tempest.get("obs_epoch")
        age_min = age_minutes(last_epoch)

        st.caption(f"Last update: {format_dt(last_epoch)} ({status_label(age_min)})")

        st.metric("Temperature (F)", fmt(temp_f))
        st.metric("Heat Index (F)", fmt(heat_f))
        st.metric("Humidity (%)", fmt(humidity, "{:.0f}"))
        st.metric("Pressure (inHg)", fmt(pressure, "{:.2f}"))
        st.metric("Wind (mph)", f"{fmt(wind)} / gust {fmt(gust)}")
        st.metric("Rain (mm)", fmt(tempest.get("rain_accumulated")))
        st.metric("Lightning (48h)", fmt(tempest.get("lightning_strike_count"), "{:.0f}"))
        st.metric("Battery (V)", fmt(tempest.get("battery"), "{:.2f}"))
        st.metric("Solar (W/m2)", fmt(tempest.get("solar_radiation"), "{:.0f}"))
        st.metric("UV Index", fmt(tempest.get("uv"), "{:.1f}"))

# ------------------------
# AirLink summary
# ------------------------
with col2:
    st.subheader("AirLink (Outdoor Air)")
    if not airlink:
        st.info("No AirLink data available.")
    else:
        last_epoch = airlink.get("ts")
        age_min = age_minutes(last_epoch)
        aqi = pm25_aqi(airlink.get("pm_2p5"))
        aqi_nowcast = pm25_aqi(airlink.get("pm_2p5_nowcast"))
        hi = airlink.get("heat_index_f")
        if hi is None:
            hi = heat_index_f(airlink.get("temp_f"), airlink.get("hum"))

        st.caption(f"Last update: {format_dt(last_epoch)} ({status_label(age_min)})")

        st.metric("Temperature (F)", fmt(airlink.get("temp_f")))
        st.metric("Heat Index (F)", fmt(hi))
        st.metric("Humidity (%)", fmt(airlink.get("hum"), "{:.0f}"))
        st.metric("PM2.5 (ug/m3)", fmt(airlink.get("pm_2p5"), "{:.1f}"))
        st.metric("AQI (PM2.5)", fmt(aqi, "{:.0f}"))
        st.metric("AQI NowCast", fmt(aqi_nowcast, "{:.0f}"))
        st.metric("PM1 / PM10", f"{fmt(airlink.get('pm_1'))} / {fmt(airlink.get('pm_10'))}")
        st.metric("Dew / Wet Bulb (F)", f"{fmt(airlink.get('dew_point_f'))} / {fmt(airlink.get('wet_bulb_f'))}")
        st.metric("PM2.5 1h / 24h", f"{fmt(airlink.get('pm_2p5_last_1_hour'))} / {fmt(airlink.get('pm_2p5_last_24_hours'))}")
        st.metric("Data Coverage %", f"{fmt(airlink.get('pct_pm_data_nowcast'), '{:.0f}')}/{fmt(airlink.get('pct_pm_data_last_1_hour'), '{:.0f}')}/{fmt(airlink.get('pct_pm_data_last_24_hours'), '{:.0f}')}" )

st.divider()
st.caption("This page loads only the latest readings for speed. Add charts later if needed.")
