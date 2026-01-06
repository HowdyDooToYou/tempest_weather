import sqlite3
import pandas as pd
import altair as alt
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
    body { background: #0f1115; }
    .main { background: #0f1115; }
    .card {
        padding: 14px 16px;
        border-radius: 12px;
        background: #1a1d23;
        border: 1px solid #232834;
        color: #e7ecf3;
    }
    .card .title { font-size: 0.9rem; color: #9aa4b5; margin-bottom: 6px; }
    .card .value { font-size: 1.8rem; font-weight: 700; }
    .pill {
        display: inline-flex;
        align-items: center;
        padding: 6px 12px;
        border-radius: 999px;
        font-size: 0.85rem;
        font-weight: 600;
        border: 1px solid rgba(255,255,255,0.08);
        color: #fff;
    }
    .section-gap { margin-top: 18px; }
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


def clean_chart(data, height=240, title=None):
    chart = (
        alt.Chart(data)
        .mark_line()
        .encode(
            x=alt.X("time:T", title="Time"),
            y=alt.Y("value:Q", title=None),
            color=alt.Color("metric:N", legend=alt.Legend(title=None)),
            tooltip=["time:T", "metric:N", alt.Tooltip("value:Q", format=".2f")],
        )
        .properties(height=height, title=title)
        .configure_axis(
            labelColor="#cfd6e5",
            titleColor="#cfd6e5",
            gridColor="#1f252f"
        )
        .configure_legend(labelColor="#cfd6e5", titleColor="#cfd6e5")
        .configure_title(color="#cfd6e5")
    )
    return chart


def bar_chart(data, height=200, title=None, color="#61a5ff"):
    chart = (
        alt.Chart(data)
        .mark_bar(color=color)
        .encode(
            x=alt.X("label:N", title=None, sort=None),
            y=alt.Y("value:Q", title=None),
            tooltip=["label:N", alt.Tooltip("value:Q", format=".1f")],
        )
        .properties(height=height, title=title)
        .configure_axis(labelColor="#cfd6e5", titleColor="#cfd6e5", gridColor="#1f252f")
        .configure_title(color="#cfd6e5")
    )
    return chart


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
    st.caption(
        f"Last Tempest update: {latest_ts_str(t_latest.obs_epoch)} | Window: {st.session_state.hours}h"
    )

    t_cards = st.columns(4)
    t_cards[0].metric("Temperature (F)", f"{t_latest.air_temperature_f:.1f}")
    t_cards[1].metric("Heat Index (F)", f"{t_latest.heat_index_f:.1f}")
    t_cards[2].metric("Humidity (%)", f"{t_latest.relative_humidity:.0f}")
    t_cards[3].metric("Pressure (inHg)", f"{t_latest.pressure_inhg:.2f}")

    temp_long = tempest.melt(
        id_vars=["time"], value_vars=["air_temperature_f", "heat_index_f"], var_name="metric"
    )
    temp_long["metric"] = temp_long["metric"].map(
        {"air_temperature_f": "Air Temperature", "heat_index_f": "Heat Index"}
    )
    st.altair_chart(clean_chart(temp_long, height=260, title="Temperature vs Heat Index"), use_container_width=True)

    wind_long = tempest[["time", "wind_speed_mph"]].rename(columns={"wind_speed_mph": "value"})
    wind_long["metric"] = "Wind Speed (mph)"
    st.altair_chart(clean_chart(wind_long, height=200, title="Wind Speed"), use_container_width=True)
else:
    st.info("No Tempest data in selected window.")

# ------------------------
# AirLink (Outdoor Air)
# ------------------------
st.subheader("Outdoor AirLink (Air Quality)")

airlink = load_df(
    """
    SELECT
        did,
        ts,
        lsid,
        data_structure_type,
        last_report_time,
        temp_f,
        hum,
        dew_point_f,
        wet_bulb_f,
        heat_index_f,
        pm_1,
        pm_2p5,
        pm_10,
        pm_1_last,
        pm_2p5_last,
        pm_10_last,
        pm_1_last_1_hour,
        pm_2p5_last_1_hour,
        pm_10_last_1_hour,
        pm_1_last_3_hours,
        pm_2p5_last_3_hours,
        pm_10_last_3_hours,
        pm_1_last_24_hours,
        pm_2p5_last_24_hours,
        pm_10_last_24_hours,
        pm_1_nowcast,
        pm_2p5_nowcast,
        pm_10_nowcast,
        pct_pm_data_nowcast,
        pct_pm_data_last_1_hour,
        pct_pm_data_last_3_hours,
        pct_pm_data_last_24_hours
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
    st.caption(
        f"Last AirLink update: {latest_ts_str(latest.ts)} | Window: {st.session_state.hours}h"
    )

    # -------------------- dynamic layout controls
    section_options = [
        "Vitals",
        "AQI cards",
        "AQI history",
        "PM snapshot",
        "PM history",
        "PM windows",
        "Data coverage",
        "All metrics table",
    ]
    chosen_sections = st.sidebar.multiselect(
        "Sections to display (order applies)",
        options=section_options,
        default=section_options,
    )

    def show_section(name: str) -> bool:
        return name in chosen_sections

    if show_section("Vitals"):
        top_cols = st.columns([1, 1, 1, 1])
        with top_cols[0]:
            info_card("Outside Temp (F)", f"{latest.temp_f:.1f}" if pd.notna(latest.temp_f) else "--", "from AirLink")
        with top_cols[1]:
            info_card("Heat Index (F)", f"{latest.heat_index_f:.1f}" if pd.notna(latest.heat_index_f) else "--", "AirLink")
        with top_cols[2]:
            info_card("Humidity (%)", f"{latest.hum:.0f}" if pd.notna(latest.hum) else "--", "AirLink")
        with top_cols[3]:
            aqi_display = f"{latest.aqi_pm25:.0f}" if pd.notna(latest.aqi_pm25) else "--"
            st.markdown("<div class='section-gap'></div>", unsafe_allow_html=True)
            st.markdown(
                f"<span class='pill' style='background:{aqi_color(latest.aqi_pm25)};'>AQI {aqi_display} | {aqi_category(latest.aqi_pm25)}</span>",
                unsafe_allow_html=True,
            )

        temp_bars_df = pd.DataFrame({
            "label": ["Outside Temp", "Heat Index", "Dew Point", "Wet Bulb"],
            "value": [latest.temp_f, latest.heat_index_f, latest.dew_point_f, latest.wet_bulb_f],
        })
        st.altair_chart(bar_chart(temp_bars_df, height=200, title="Temperature Family (°F)"), use_container_width=True)

    if show_section("AQI cards"):
        aqi_cards = st.columns(3)
        aqi_cards[0].metric("Current AQI (PM2.5)", f"{latest.aqi_pm25:.0f}" if pd.notna(latest.aqi_pm25) else "--", aqi_category(latest.aqi_pm25))
        aqi_cards[1].metric("1 Hour AQI (PM2.5)", f"{latest.aqi_pm25_last_1_hour:.0f}" if pd.notna(latest.aqi_pm25_last_1_hour) else "--", aqi_category(latest.aqi_pm25_last_1_hour))
        aqi_cards[2].metric("NowCast AQI (PM2.5)", f"{latest.aqi_pm25_nowcast:.0f}" if pd.notna(latest.aqi_pm25_nowcast) else "--", aqi_category(latest.aqi_pm25_nowcast))

        aqi_bar_df = pd.DataFrame({
            "label": ["Current AQI", "1 Hour AQI", "NowCast AQI"],
            "value": [latest.aqi_pm25, latest.aqi_pm25_last_1_hour, latest.aqi_pm25_nowcast],
        })
        st.altair_chart(bar_chart(aqi_bar_df, height=180, title="AQI Comparison"), use_container_width=True)

    if show_section("AQI history"):
        aqi_long = airlink[["time", "aqi_pm25"]].rename(columns={"aqi_pm25": "value"})
        aqi_long["metric"] = "AQI (PM2.5)"
        st.altair_chart(clean_chart(aqi_long, height=240, title="AQI Over Time"), use_container_width=True)

    if show_section("PM snapshot"):
        pm_bars = pd.DataFrame({
            "label": ["PM1", "PM2.5", "PM10"],
            "value": [latest.pm_1, latest.pm_2p5, latest.pm_10],
        })
        st.altair_chart(bar_chart(pm_bars, height=180, title="Particulate Snapshot (µg/m³)"), use_container_width=True)

    if show_section("PM history"):
        pm_long = airlink.melt(
            id_vars=["time"],
            value_vars=["pm_1", "pm_2p5", "pm_10"],
            var_name="metric",
            value_name="value",
        )
        pm_long["metric"] = pm_long["metric"].map({"pm_1": "PM1", "pm_2p5": "PM2.5", "pm_10": "PM10"})
        st.altair_chart(clean_chart(pm_long, height=240, title="Particulate Over Time"), use_container_width=True)

    if show_section("PM windows"):
        window_bars = pd.DataFrame({
            "label": [
                "PM1 last", "PM2.5 last", "PM10 last",
                "PM1 1h", "PM2.5 1h", "PM10 1h",
                "PM1 3h", "PM2.5 3h", "PM10 3h",
                "PM1 24h", "PM2.5 24h", "PM10 24h",
                "PM1 NowCast", "PM2.5 NowCast", "PM10 NowCast",
            ],
            "value": [
                latest.pm_1_last, latest.pm_2p5_last, latest.pm_10_last,
                latest.pm_1_last_1_hour, latest.pm_2p5_last_1_hour, latest.pm_10_last_1_hour,
                latest.pm_1_last_3_hours, latest.pm_2p5_last_3_hours, latest.pm_10_last_3_hours,
                latest.pm_1_last_24_hours, latest.pm_2p5_last_24_hours, latest.pm_10_last_24_hours,
                latest.pm_1_nowcast, latest.pm_2p5_nowcast, latest.pm_10_nowcast,
            ],
        })
        st.altair_chart(bar_chart(window_bars, height=260, title="Particulate Averages / Windows (µg/m³)"), use_container_width=True)

    if show_section("Data coverage"):
        coverage_bars = pd.DataFrame({
            "label": [
                "NowCast data %",
                "1h data %",
                "3h data %",
                "24h data %",
            ],
            "value": [
                latest.pct_pm_data_nowcast,
                latest.pct_pm_data_last_1_hour,
                latest.pct_pm_data_last_3_hours,
                latest.pct_pm_data_last_24_hours,
            ],
        })
        st.altair_chart(bar_chart(coverage_bars, height=160, title="Data Coverage (%)"), use_container_width=True)

    if show_section("All metrics table"):
        display_cols = [
            "did", "ts", "lsid", "data_structure_type", "last_report_time",
            "temp_f", "hum", "dew_point_f", "wet_bulb_f", "heat_index_f",
            "pm_1", "pm_2p5", "pm_10",
            "pm_1_last", "pm_2p5_last", "pm_10_last",
            "pm_1_last_1_hour", "pm_2p5_last_1_hour", "pm_10_last_1_hour",
            "pm_1_last_3_hours", "pm_2p5_last_3_hours", "pm_10_last_3_hours",
            "pm_1_last_24_hours", "pm_2p5_last_24_hours", "pm_10_last_24_hours",
            "pm_1_nowcast", "pm_2p5_nowcast", "pm_10_nowcast",
            "pct_pm_data_nowcast", "pct_pm_data_last_1_hour", "pct_pm_data_last_3_hours", "pct_pm_data_last_24_hours",
            "aqi_pm25", "aqi_pm25_last_1_hour", "aqi_pm25_last_24_hours", "aqi_pm25_nowcast",
        ]
        st.dataframe(latest[display_cols].to_frame().T, use_container_width=True)
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
