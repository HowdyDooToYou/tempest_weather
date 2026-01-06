import sqlite3
import pandas as pd
import streamlit as st

DB_PATH = "data/tempest.db"

st.set_page_config(
    page_title="Weather & Air Quality Dashboard",
    layout="wide"
)

# ------------------------
# Theming
# ------------------------
st.markdown(
    """
    <style>
    body { background-color: #0f1115; }
    .main { background-color: #0f1115; }
    .card {
        padding: 12px 14px;
        border-radius: 12px;
        background: #1a1d23;
        border: 1px solid #232834;
        color: #e7ecf3;
    }
    .card .title { font-size: 0.85rem; color: #9aa4b5; margin-bottom: 4px; }
    .card .value { font-size: 1.6rem; font-weight: 700; }
    .chip {
        display: inline-flex;
        align-items: center;
        padding: 4px 10px;
        border-radius: 999px;
        font-size: 0.8rem;
        font-weight: 600;
        border: 1px solid rgba(255,255,255,0.08);
    }
    </style>
    """,
    unsafe_allow_html=True,
)

st.title("Weather & Air Quality Dashboard")

# ------------------------
# Helpers
# ------------------------
@st.cache_data(ttl=60)
def load_df(query, params=None):
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query(query, conn, params=params or {})
    conn.close()
    return df


def epoch_to_dt(series):
    return pd.to_datetime(series, unit="s", utc=True).dt.tz_convert("America/New_York")


def c_to_f(c):
    return (c * 9 / 5) + 32


def hpa_to_inhg(hpa):
    return hpa * 0.0295299830714


def mps_to_mph(mps):
    return mps * 2.2369362921


def compute_heat_index(temp_f, humidity):
    # NOAA formula with low-temp fallback and standard adjustments
    t = pd.Series(temp_f, dtype="float64")
    r = pd.Series(humidity, dtype="float64")

    simple = 0.5 * (t + 61.0 + ((t - 68.0) * 1.2) + (r * 0.094))

    rothfusz = (
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

    hi = rothfusz.where((t >= 80) & (r >= 40), simple)

    adj1_mask = (r < 13) & (t >= 80) & (t <= 112)
    adj1 = ((13 - r) / 4) * ((17 - (t - 95).abs()) / 17)
    hi = hi - adj1.where(adj1_mask, 0)

    adj2_mask = (r > 85) & (t >= 80) & (t <= 87)
    adj2 = ((r - 85) / 10) * ((87 - t) / 5)
    hi = hi + adj2.where(adj2_mask, 0)

    return hi.fillna(t)


def compute_pm25_aqi(pm_value):
    # US EPA PM2.5 breakpoints (24h AQI)
    breakpoints = [
        (0.0, 12.0, 0, 50),
        (12.1, 35.4, 51, 100),
        (35.5, 55.4, 101, 150),
        (55.5, 150.4, 151, 200),
        (150.5, 250.4, 201, 300),
        (250.5, 350.4, 301, 400),
        (350.5, 500.4, 401, 500),
    ]
    pm = float(pm_value) if pm_value is not None else None
    if pm is None or pd.isna(pm):
        return None
    for c_low, c_high, a_low, a_high in breakpoints:
        if c_low <= pm <= c_high:
            return (a_high - a_low) / (c_high - c_low) * (pm - c_low) + a_low
    return 500.0


def aqi_category(aqi):
    if aqi is None or pd.isna(aqi):
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


def aqi_color(aqi):
    if aqi is None or pd.isna(aqi):
        return "#2d2f36"
    if aqi <= 50:
        return "#1e8f4b"
    if aqi <= 100:
        return "#c6a700"
    if aqi <= 150:
        return "#d35400"
    if aqi <= 200:
        return "#c0392b"
    if aqi <= 300:
        return "#8e44ad"
    return "#6e2c00"


def info_card(title, value, subtitle=None, color="#1a1d23"):
    st.markdown(
        f"""
        <div class="card" style="background:{color};">
            <div class="title">{title}</div>
            <div class="value">{value}</div>
            <div style="color:#b0bacb; font-size:0.85rem;">{subtitle or ''}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def latest_ts_str(ts_epoch):
    if pd.isna(ts_epoch):
        return "--"
    dt = epoch_to_dt(pd.Series([ts_epoch])).iloc[0]
    return dt.strftime("%Y-%m-%d %I:%M %p")


# ------------------------
# Sidebar controls
# ------------------------
st.sidebar.header("Controls")

if "hours" not in st.session_state:
    st.session_state.hours = 24

preset_cols = st.sidebar.columns(4)
presets = [6, 12, 24, 168]
preset_labels = ["6h", "12h", "24h", "7d"]
for col, hrs, label in zip(preset_cols, presets, preset_labels):
    if col.button(label):
        st.session_state.hours = hrs

st.session_state.hours = st.sidebar.slider(
    "Time window (hours)",
    min_value=1,
    max_value=168,
    value=int(st.session_state.hours),
)

since_epoch = int(
    (pd.Timestamp.utcnow() - pd.Timedelta(hours=st.session_state.hours)).timestamp()
)

# ------------------------
# Tempest (Outdoor)
# ------------------------
st.subheader("Outdoor Tempest Station")

tempest = load_df(
    """
    SELECT
        obs_epoch,
        air_temperature,
        relative_humidity,
        station_pressure,
        wind_avg
    FROM obs_st
    WHERE obs_epoch >= :since
    ORDER BY obs_epoch
    """,
    {"since": since_epoch},
)

if not tempest.empty:
    tempest["time"] = epoch_to_dt(tempest["obs_epoch"])
    tempest["air_temperature_f"] = c_to_f(tempest["air_temperature"])
    tempest["heat_index_f"] = compute_heat_index(
        tempest["air_temperature_f"],
        tempest["relative_humidity"]
    )
    tempest["pressure_inhg"] = hpa_to_inhg(tempest["station_pressure"])
    tempest["wind_speed_mph"] = mps_to_mph(tempest["wind_avg"])

    t_latest = tempest.iloc[-1]
    last_obs = latest_ts_str(t_latest.obs_epoch)

    st.caption(f"Last Tempest update: {last_obs} | Window: {st.session_state.hours}h")

    t_cards = st.columns(4)
    t_cards[0].metric("Temperature (F)", f"{t_latest.air_temperature_f:.1f}")
    t_cards[1].metric("Heat Index (F)", f"{t_latest.heat_index_f:.1f}")
    t_cards[2].metric("Humidity (%)", f"{t_latest.relative_humidity:.0f}")
    t_cards[3].metric("Pressure (inHg)", f"{t_latest.pressure_inhg:.2f}")

    st.area_chart(
        tempest.set_index("time")[
            ["air_temperature_f", "heat_index_f"]
        ],
        height=260,
    )

    st.line_chart(
        tempest.set_index("time")[
            ["wind_speed_mph"]
        ],
        height=200,
    )
else:
    st.info("No Tempest data in selected window.")

# ------------------------
# AirLink (Outdoor Air)
# ------------------------
st.subheader("Outdoor AirLink (Air Quality)")

airlink = load_df(
    """
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
        pm_2p5_last_1_hour,
        pm_2p5_last_24_hours,
        pm_2p5_nowcast,
        pm_1_nowcast,
        pm_10_nowcast
    FROM airlink_obs
    WHERE ts >= :since
    ORDER BY ts
    """,
    {"since": since_epoch},
)

if not airlink.empty:
    airlink["time"] = epoch_to_dt(airlink["ts"])
    airlink["aqi_pm25"] = airlink["pm_2p5"].apply(compute_pm25_aqi)
    airlink["aqi_pm25_last_1_hour"] = airlink["pm_2p5_last_1_hour"].apply(compute_pm25_aqi)
    airlink["aqi_pm25_last_24_hours"] = airlink["pm_2p5_last_24_hours"].apply(compute_pm25_aqi)
    airlink["aqi_pm25_nowcast"] = airlink["pm_2p5_nowcast"].apply(compute_pm25_aqi)

    latest = airlink.iloc[-1]
    last_obs = latest_ts_str(latest.ts)
    aqi_col = aqi_color(latest.aqi_pm25)

    st.caption(f"Last AirLink update: {last_obs} | Window: {st.session_state.hours}h")

    top_cols = st.columns(4)
    info_card("Outside Temp (F)", f"{latest.temp_f:.1f}" if pd.notna(latest.temp_f) else "--", "from AirLink", color="#1d2432")
    info_card("Heat Index (F)", f"{latest.heat_index_f:.1f}" if pd.notna(latest.heat_index_f) else "--", "AirLink", color="#1d2432")
    info_card("Humidity (%)", f"{latest.hum:.0f}" if pd.notna(latest.hum) else "--", "AirLink", color="#1d2432")
    with top_cols[3]:
        st.markdown("<div style='height:4px'></div>", unsafe_allow_html=True)
        st.markdown(
            f"<span class='chip' style='background:{aqi_col}; color:white;'>AQI {latest.aqi_pm25:.0f if pd.notna(latest.aqi_pm25) else '--'} | {aqi_category(latest.aqi_pm25)}</span>",
            unsafe_allow_html=True,
        )

    temp_bars = pd.DataFrame({
        "Temperature": [
            latest.temp_f,
            latest.heat_index_f,
            latest.dew_point_f,
            latest.wet_bulb_f,
        ]
    }, index=["Outside Temp", "Heat Index", "Dew Point", "Wet Bulb"])
    st.bar_chart(temp_bars, height=240)

    aqi_cols = st.columns(3)
    aqi_cols[0].metric(
        "Current AQI (PM2.5)",
        f"{latest.aqi_pm25:.0f}" if pd.notna(latest.aqi_pm25) else "--",
        aqi_category(latest.aqi_pm25)
    )
    aqi_cols[1].metric(
        "1 Hour AQI (PM2.5)",
        f"{latest.aqi_pm25_last_1_hour:.0f}" if pd.notna(latest.aqi_pm25_last_1_hour) else "--",
        aqi_category(latest.aqi_pm25_last_1_hour)
    )
    aqi_cols[2].metric(
        "NowCast AQI (PM2.5)",
        f"{latest.aqi_pm25_nowcast:.0f}" if pd.notna(latest.aqi_pm25_nowcast) else "--",
        aqi_category(latest.aqi_pm25_nowcast)
    )

    aqi_bar_df = pd.DataFrame({
        "Value": [
            latest.aqi_pm25,
            latest.aqi_pm25_last_1_hour,
            latest.aqi_pm25_nowcast,
        ]
    }, index=["Current AQI", "1 Hour AQI", "NowCast AQI"])
    st.bar_chart(aqi_bar_df, height=240)

    st.line_chart(
        airlink.set_index("time")["aqi_pm25"].dropna(),
        height=260,
    )

    pm_bar_df = pd.DataFrame({
        "ug/m3": [latest.pm_1, latest.pm_2p5, latest.pm_10]
    }, index=["PM1", "PM2.5", "PM10"])
    st.bar_chart(pm_bar_df, height=240)

    st.line_chart(
        airlink.set_index("time")["pm_1 pm_2p5 pm_10".split()].dropna(how="all"),
        height=260,
    )

    st.caption("AQI based on US EPA PM2.5 breakpoints; NowCast uses AirLink-provided nowcast field")
else:
    st.info("No AirLink data in selected window.")

# ------------------------
# Raw Event Explorer
# ------------------------
st.subheader("Raw Event Explorer")

raw = load_df(
    """
    SELECT
        received_at_epoch,
        message_type,
        device_id,
        payload_json
    FROM raw_events
    ORDER BY received_at_epoch DESC
    LIMIT 200
    """
)

if not raw.empty:
    raw["received_at"] = epoch_to_dt(raw["received_at_epoch"])
    st.dataframe(
        raw[[
            "received_at",
            "message_type",
            "device_id",
            "payload_json"
        ]],
        use_container_width=True
    )
else:
    st.info("No raw events available.")
