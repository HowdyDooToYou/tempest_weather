import sqlite3
import json
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

DB_PATH = "data/tempest.db"
TEMPEST_STATION_ID = 475329
TEMPEST_HUB_ID = 475327

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
    .gauge-block {
        margin-top: 10px;
        padding: 12px 12px 10px 12px;
        border-radius: 12px;
        background: #161920;
        border: 1px solid #202636;
    }
    .gauge-header {
        display: flex;
        justify-content: space-between;
        align-items: center;
        color: #d8deed;
        font-size: 0.9rem;
        font-weight: 600;
    }
    .gauge-track {
        margin-top: 8px;
        width: 100%;
        height: 12px;
        border-radius: 999px;
        background: #0d1016;
        border: 1px solid #1f2430;
        overflow: hidden;
    }
    .gauge-fill {
        height: 100%;
        border-radius: 999px;
        background: linear-gradient(90deg, #59c5ff, #5f7bff);
        transition: width 0.8s ease, filter 0.3s ease;
    }
    .gauge-pulse .gauge-fill {
        animation: pulseBar 1.8s ease-in-out infinite;
    }
    .gauge-muted {
        color: #9aa4b5;
        font-size: 0.8rem;
        margin-top: 4px;
    }
    .gauge-block:hover .gauge-fill {
        filter: brightness(1.2);
    }
    @keyframes fadeInUp {
        from { opacity: 0; transform: translate3d(0, 12px, 0); }
        to { opacity: 1; transform: translate3d(0, 0, 0); }
    }
    [data-testid="stMetric"] {
        animation: fadeInUp 0.5s ease;
        animation-fill-mode: both;
    }
    .gauge-block {
        animation: fadeInUp 0.4s ease;
        animation-fill-mode: both;
    }
    @keyframes pulseBar {
        0%   { filter: brightness(1); }
        50%  { filter: brightness(1.35); }
        100% { filter: brightness(1); }
    }
    /* Tabs & mobile polish */
    [data-baseweb="tab-list"] {
        overflow-x: auto;
        scrollbar-width: thin;
    }
    @media (max-width: 820px) {
        .block-container { padding: 0.6rem 0.8rem; }
        [data-baseweb="tab-list"] { gap: 6px; }
    }
    /* Hero glow */
    .hero-glow {
        position: relative;
    }
    .hero-glow::after {
        content: "";
        position: absolute;
        inset: -8px -12px;
        background: radial-gradient(circle at 20% 20%, rgba(97,165,255,0.18), transparent 35%),
                    radial-gradient(circle at 80% 40%, rgba(75,208,194,0.16), transparent 30%);
        filter: blur(22px);
        z-index: -1;
    }
    .aurora {
        position: relative;
        height: 10px;
        border-radius: 999px;
        overflow: hidden;
        margin: 4px 0 14px 0;
        background: linear-gradient(90deg, rgba(97,165,255,0.28), rgba(75,208,194,0.28), rgba(97,165,255,0.28));
    }
    .aurora::before {
        content: "";
        position: absolute;
        inset: 0;
        background: linear-gradient(90deg, rgba(255,255,255,0.12), transparent 40%, rgba(255,255,255,0.12));
        animation: auroraSlide 8s linear infinite;
        mix-blend-mode: screen;
    }
    @keyframes auroraSlide {
        from { transform: translateX(-30%); }
        to { transform: translateX(30%); }
    }
    .ingest-shell {
        margin: 4px 0 16px 0;
        padding: 14px 16px 16px 16px;
        border-radius: 14px;
        border: 1px solid #1f2635;
        background:
            radial-gradient(circle at 10% 10%, rgba(97,165,255,0.08), transparent 38%),
            radial-gradient(circle at 80% 20%, rgba(75,208,194,0.08), transparent 32%),
            #0d1016;
        box-shadow: 0 14px 40px rgba(0,0,0,0.38);
    }
    .ingest-header-row {
        display: flex;
        justify-content: space-between;
        align-items: center;
        gap: 12px;
        flex-wrap: wrap;
    }
    .ingest-eyebrow {
        color: #8fb7ff;
        font-size: 0.78rem;
        text-transform: uppercase;
        letter-spacing: 0.8px;
        font-weight: 700;
    }
    .ingest-summary {
        color: #cfd6e5;
        font-size: 0.9rem;
    }
    .ingest-badge {
        padding: 6px 10px;
        border-radius: 10px;
        border: 1px solid rgba(97,165,255,0.3);
        background: rgba(97,165,255,0.14);
        color: #e9f1ff;
        font-weight: 700;
    }
    .ingest-grid {
        margin-top: 10px;
        display: flex;
        flex-direction: column;
        gap: 12px;
    }
    .ingest-row {
        display: grid;
        grid-template-columns: 1.3fr 3.2fr 1fr;
        gap: 12px;
        align-items: center;
    }
    .ingest-meta {
        display: flex;
        align-items: center;
        gap: 10px;
        color: #d8deed;
        font-weight: 700;
    }
    .ingest-dot {
        width: 14px;
        height: 14px;
        border-radius: 999px;
        box-shadow: 0 0 12px currentColor;
    }
    .ingest-name { font-size: 1rem; }
    .ingest-sub { font-size: 0.8rem; color: #9aa4b5; }
    .ingest-bar {
        position: relative;
        width: 100%;
        height: 14px;
        border-radius: 999px;
        background: #0b0f15;
        border: 1px solid #1f2635;
        overflow: hidden;
    }
    .ingest-fill {
        position: absolute;
        inset: 0;
        width: calc(var(--fill, 1) * 100%);
        background: linear-gradient(90deg, var(--fill-start), var(--fill-end));
        filter: drop-shadow(0 0 8px rgba(97,165,255,0.4));
        transition: width 0.5s ease;
    }
    .ingest-pulse {
        position: absolute;
        inset: 0;
        width: 30%;
        opacity: 0.9;
        background: radial-gradient(circle at 20% 50%, rgba(255,255,255,0.4), transparent 60%);
        mix-blend-mode: screen;
        animation: ingestFlow var(--pulse-speed, 2.2s) linear infinite;
    }
    @keyframes ingestFlow {
        from { transform: translateX(-20%); }
        to   { transform: translateX(calc(var(--fill, 1) * 100%)); }
    }
    .ingest-sink {
        position: absolute;
        right: 8px;
        top: 50%;
        transform: translateY(-50%);
        padding: 4px 8px;
        border-radius: 8px;
        background: #121824;
        border: 1px solid #1f2635;
        color: #9aa4b5;
        font-size: 0.75rem;
        letter-spacing: 0.6px;
        text-transform: uppercase;
    }
    .ingest-latency {
        text-align: right;
        color: #9aa4b5;
        font-size: 0.85rem;
    }
    @media (max-width: 960px) {
        .ingest-row { grid-template-columns: 1fr; }
        .ingest-latency { text-align: left; }
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


def sidebar_gauge(container, label, value, min_val, max_val, unit="", precision=1, color="#59c5ff", highlight=False):
    """Render a horizontal gauge in the sidebar container."""
    clean_value = None if value is None or pd.isna(value) else float(value)
    if clean_value is None or max_val == min_val:
        pct = 0
        display_value = "--"
    else:
        pct = (clean_value - min_val) / (max_val - min_val)
        pct = max(0.0, min(1.0, pct))
        display_value = f"{clean_value:.{precision}f}{unit}"

    pulse_class = "gauge-pulse" if highlight else ""
    container.markdown(
        f"""
        <div class="gauge-block {pulse_class}">
            <div class="gauge-header">
                <span>{label}</span>
                <span>{display_value}</span>
            </div>
            <div class="gauge-track">
                <div class="gauge-fill" style="width:{pct*100:.0f}%; background: linear-gradient(90deg, {color}, #4b83ff);"></div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_sidebar_gauges(container, tempest_latest=None, airlink_latest=None, highlights=None):
    """Show quick-read gauges using the freshest observations."""
    highlights = highlights or {}
    if tempest_latest is None and airlink_latest is None:
        return

    container.markdown("### Live Gauges")
    if tempest_latest is not None:
        sidebar_gauge(container, "Tempest Temp (F)", tempest_latest.air_temperature_f, -10, 110, precision=1, color="#59c5ff", highlight=highlights.get("temp"))
        sidebar_gauge(container, "Humidity (%)", tempest_latest.relative_humidity, 0, 100, precision=0, color="#4bd0c2", highlight=highlights.get("hum"))
        sidebar_gauge(container, "Pressure (inHg)", tempest_latest.pressure_inhg, 28, 32, precision=2, color="#9c7bff", highlight=highlights.get("pressure"))
        sidebar_gauge(container, "Wind Avg (mph)", tempest_latest.wind_speed_mph, 0, 40, precision=1, color="#61a5ff", highlight=highlights.get("wind"))
    if airlink_latest is not None:
        aqi_value = None if pd.isna(airlink_latest.aqi_pm25) else airlink_latest.aqi_pm25
        aqi_col = aqi_color(aqi_value)
        sidebar_gauge(container, "AQI PM2.5", aqi_value, 0, 300, precision=0, color=aqi_col, highlight=highlights.get("aqi"))
        container.markdown(
            f"<div class='gauge-muted'>AQI Category: {aqi_category(aqi_value)}</div>",
            unsafe_allow_html=True,
        )


def delta_over_window(series):
    if series is None or series.empty:
        return None
    start = series.iloc[0]
    end = series.iloc[-1]
    if pd.isna(start) or pd.isna(end):
        return None
    return float(end - start)


def format_latency(seconds):
    if seconds is None:
        return "--"
    if seconds < 60:
        return f"{seconds:.0f}s ago"
    if seconds < 3600:
        return f"{seconds/60:.1f}m ago"
    return f"{seconds/3600:.1f}h ago"


def ingestion_status(label, last_epoch, cadence_seconds, now_ts, max_lag_seconds=900, color="#59c5ff"):
    """Compatibility helper for older ingestion bar usage."""
    if last_epoch is None or pd.isna(last_epoch):
        return {
            "label": label,
            "fill_pct": 0.0,
            "status_label": "No Signal",
            "status_class": "offline",
            "latency_text": "--",
            "status_text": "Waiting on packets",
            "flow_speed": 3.2,
            "color": color,
            "latency_seconds": None,
        }

    latency_seconds = (now_ts - pd.to_datetime(last_epoch, unit="s", utc=True)).total_seconds()
    freshness = 1 - min(max(latency_seconds, 0) / max_lag_seconds, 1)
    fill_pct = max(0.08, freshness)
    status_label = "Ingesting" if latency_seconds <= cadence_seconds * 1.6 else "Delayed" if latency_seconds <= max_lag_seconds else "Stalled"
    status_class = "ok" if status_label == "Ingesting" else "delay" if status_label == "Delayed" else "offline"
    status_text = f"{format_latency(latency_seconds)} latency"
    flow_speed = 1.2 + (1 - fill_pct) * 2.4

    return {
        "label": label,
        "fill_pct": fill_pct,
        "status_label": status_label,
        "status_class": status_class,
        "latency_text": format_latency(latency_seconds),
        "status_text": status_text,
        "flow_speed": flow_speed,
        "color": color,
        "latency_seconds": latency_seconds,
    }


def minutes_since_epoch(epoch, now_ts):
    if epoch is None or pd.isna(epoch):
        return None
    return (now_ts - pd.to_datetime(epoch, unit="s", utc=True)).total_seconds() / 60


def latency_label(latency_minutes):
    if latency_minutes is None:
        return "no signal"
    if latency_minutes < 1:
        return "live (<1m)"
    if latency_minutes < 90:
        return f"{latency_minutes:.1f}m lag"
    return f"{latency_minutes/60:.1f}h lag"


def ingest_health(latency_minutes, fresh=2, stale=60):
    if latency_minutes is None:
        return 0.0
    if latency_minutes <= fresh:
        return 1.0
    if latency_minutes >= stale:
        return 0.05
    span = stale - fresh
    return max(0.05, 1 - ((latency_minutes - fresh) / span))


def flow_speed_from_load(events_per_hour, base=2.4):
    per_min = events_per_hour / 60 if events_per_hour is not None else 0
    return max(1.0, base - min(per_min, 12) * 0.18)


def recent_activity(table, epoch_col, cutoff_epoch, device_col=None, device_id=None, message_col=None, message_types=None):
    where = [f"{epoch_col} >= :cutoff"]
    params = {"cutoff": cutoff_epoch}
    if device_col and device_id is not None:
        where.append(f"{device_col} = :device_id")
        params["device_id"] = device_id
    if message_col and message_types:
        placeholders = ", ".join([f":mt{i}" for i in range(len(message_types))])
        where.append(f"{message_col} IN ({placeholders})")
        params.update({f"mt{i}": m for i, m in enumerate(message_types)})
    where_clause = " AND ".join(where)

    df = load_df(
        f"""
        SELECT
            COUNT(*) AS cnt,
            MAX({epoch_col}) AS last_epoch
        FROM {table}
        WHERE {where_clause}
        """,
        params,
    )
    if not df.empty:
        count_raw = df.iloc[0]["cnt"]
        last_epoch_raw = df.iloc[0]["last_epoch"]
    else:
        count_raw = 0
        last_epoch_raw = None

    count = 0 if count_raw is None or pd.isna(count_raw) else int(count_raw)
    last_epoch = None if last_epoch_raw is None or pd.isna(last_epoch_raw) else last_epoch_raw

    if (last_epoch is None) or pd.isna(last_epoch):
        fallback_where = ""
        fallback_params = {}
        if device_col and device_id is not None:
            fallback_where = f"WHERE {device_col} = :device_id"
            fallback_params["device_id"] = device_id
        fallback_where_clauses = []
        if fallback_where:
            fallback_where_clauses.append(fallback_where.strip())
        if message_col and message_types:
            placeholders = ", ".join([f":mt_fb{i}" for i in range(len(message_types))])
            fallback_where_clauses.append(f"{message_col} IN ({placeholders})")
            fallback_params.update({f"mt_fb{i}": m for i, m in enumerate(message_types)})
        final_where = ""
        if fallback_where_clauses:
            final_where = "WHERE " + " AND ".join([clause.replace("WHERE ", "") for clause in fallback_where_clauses])

        fallback_df = load_df(
            f"SELECT MAX({epoch_col}) AS last_epoch FROM {table} {final_where}",
            fallback_params,
        )
        last_epoch = fallback_df.iloc[0]["last_epoch"] if not fallback_df.empty else None

    return {"count": count, "last_epoch": last_epoch}


def render_ingest_banner(sources, total_recent, avg_latency_text=None):
    if not sources:
        return

    badge_text = f"{total_recent} evt/hr"
    if avg_latency_text:
        badge_text = f"{badge_text} • {avg_latency_text} avg lag"

    rows_html = "".join(
        f"""<div class="ingest-row">
    <div class="ingest-meta">
        <span class="ingest-dot" style="background:{src['colors'][0]};"></span>
        <div>
            <div class="ingest-name">{src['name']}</div>
            <div class="ingest-sub">{src['latency_text']} • {src['load_text']}</div>
        </div>
    </div>
    <div class="ingest-bar">
        <div class="ingest-fill" style="--fill:{src['fill']:.3f}; --fill-start:{src['colors'][0]}; --fill-end:{src['colors'][1]};"></div>
        <div class="ingest-pulse" style="--fill:{src['fill']:.3f}; --pulse-speed:{src['pulse_speed']:.2f}s; background: radial-gradient(circle at 20% 50%, {src['colors'][1]} 0%, transparent 65%);"></div>
        <div class="ingest-sink">Data</div>
    </div>
    <div class="ingest-latency">Last packet {src['last_seen']}</div>
</div>"""
        for src in sources
    )

    rows_html = "".join(
        f"""<div class="ingest-row">
    <div class="ingest-meta">
        <span class="ingest-dot" style="background:{src['colors'][0]};"></span>
        <div>
            <div class="ingest-name">{src['name']}</div>
            <div class="ingest-sub">{src['latency_text']} • {src['load_text']}</div>
        </div>
    </div>
    <div class="ingest-bar">
        <div class="ingest-fill" style="--fill:{src['fill']:.3f}; --fill-start:{src['colors'][0]}; --fill-end:{src['colors'][1]};"></div>
        <div class="ingest-pulse" style="--fill:{src['fill']:.3f}; --pulse-speed:{src['pulse_speed']:.2f}s; background: radial-gradient(circle at 20% 50%, {src['colors'][1]} 0%, transparent 65%);"></div>
        <div class="ingest-sink">Data</div>
    </div>
    <div class="ingest-latency">Last packet {src['last_seen']}</div>
</div>"""
        for src in sources
    )

    st.markdown(
        f"""
<div class="ingest-shell hero-glow">
  <div class="ingest-header-row">
    <div>
      <div class="ingest-eyebrow">Live ingest</div>
      <div class="ingest-summary">Progress pulses pull in from AirLink, Tempest Station, and Hub to mimic the pipeline latency.</div>
    </div>
    <div class="ingest-badge">{badge_text}</div>
  </div>
  <div class="ingest-grid">
    {rows_html}
  </div>
</div>
        """,
        unsafe_allow_html=True,
    )


def daily_extremes(df, time_col, value_cols):
    if df is None or df.empty:
        return {}
    temp_df = df.set_index(pd.DatetimeIndex(df[time_col]))
    extremes = {}
    for col in value_cols:
        if col not in temp_df:
            continue
        daily = temp_df[col].resample("1D").agg(["min", "max"])
        if daily.empty:
            continue
        extremes[col] = {
            "min": daily["min"].min(),
            "max": daily["max"].max(),
        }
    return extremes


def aqi_zone_share(aqi_series):
    if aqi_series is None or aqi_series.empty:
        return pd.DataFrame(columns=["category", "share"])
    cats = aqi_series.apply(aqi_category)
    counts = cats.value_counts(normalize=True).rename_axis("category").reset_index(name="share")
    return counts


def story_lines(tempest_df, airlink_df, window_desc: str):
    lines = []
    window_label = window_desc or "this window"
    if tempest_df is not None and not tempest_df.empty:
        latest = tempest_df.iloc[-1]
        delta_temp = delta_over_window(tempest_df["air_temperature_f"])
        delta_hum = delta_over_window(tempest_df["relative_humidity"])
        if delta_temp is not None:
            lines.append(f"Temp changed {delta_temp:+.1f}F over {window_label} (now {latest.air_temperature_f:.1f}F).")
        if delta_hum is not None:
            lines.append(f"Humidity shifted {delta_hum:+.0f}% over {window_label} (now {latest.relative_humidity:.0f}%).")
    if airlink_df is not None and not airlink_df.empty:
        latest = airlink_df.iloc[-1]
        delta_aqi = delta_over_window(airlink_df["aqi_pm25"]) if "aqi_pm25" in airlink_df else None
        if delta_aqi is not None:
            lines.append(f"AQI moved {delta_aqi:+.0f} points over {window_label}; current {latest.aqi_pm25:.0f} ({aqi_category(latest.aqi_pm25)}).")
        zone_share = aqi_zone_share(airlink_df["aqi_pm25"])
        if not zone_share.empty:
            top_zone = zone_share.sort_values("share", ascending=False).iloc[0]
            lines.append(f"{top_zone['share']*100:.0f}% of readings over {window_label} were {top_zone['category']}.")
    return lines


def render_sprite_component(wind_speed, lightning_count, ingest_rate, wind_dir_deg=None):
    """Embed the sprite-sheet canvas using live data."""
    js_path = Path("static/sprite_player.js")
    sheet_path = Path("images/sprite-full.png")
    import base64

    sprite_js = ""
    sheet_b64 = ""
    sheet_data_uri = ""
    sheet_ok = False
    status_boot = "Loading sprites..."
    missing = []

    if js_path.exists():
        sprite_js = js_path.read_text()
    else:
        missing.append("sprite_player.js missing")
        sprite_js = "window.SpriteSheetPlayer={mount:()=>null};"

    if sheet_path.exists():
        sheet_b64 = base64.b64encode(sheet_path.read_bytes()).decode("ascii")
        sheet_data_uri = "data:image/png;base64," + sheet_b64
        sheet_ok = True
    else:
        missing.append("sprite-full.png missing")

    # Character strips: small horizontal sequences bundled into base64 data URIs.
    def strip_uri(path):
        return "data:image/png;base64," + base64.b64encode(path.read_bytes()).decode("ascii")

    spearman_root = Path("images/Skeleton_Spearman")
    archer_root = Path("images/Skeleton_Archer")
    warrior_root = Path("images/Skeleton_Warrior")

    characters_data = {}

    spearman_strips = {
        "idle": ("Idle.png", 7),
        "walk": ("Walk.png", 7),
        "run": ("Run.png", 6),
        "runAttack": ("Run+attack.png", 5),
        "attack": ("Attack_1.png", 4),
    }
    spearman_payload = {"strips": {}, "effect": "spear"}
    for key, (fname, frames) in spearman_strips.items():
        strip_path = spearman_root / fname
        if strip_path.exists():
            spearman_payload["strips"][key] = {"uri": strip_uri(strip_path), "frames": frames}
    characters_data["spearman"] = spearman_payload

    archer_strips = {
        "idle": ("Idle.png", 7),
        "walk": ("Walk.png", 8),
        "run": ("Walk.png", 8),
        "runAttack": ("Shot_2.png", 15),
        "attack": ("Shot_1.png", 15),
    }
    archer_payload = {"strips": {}, "effect": "arrow"}
    for key, (fname, frames) in archer_strips.items():
        strip_path = archer_root / fname
        if strip_path.exists():
            archer_payload["strips"][key] = {"uri": strip_uri(strip_path), "frames": frames}
    arrow_path = archer_root / "Arrow.png"
    if arrow_path.exists():
        archer_payload["projectileUri"] = strip_uri(arrow_path)
    characters_data["archer"] = archer_payload

    warrior_strips = {
        "idle": ("Idle.png", 7),
        "walk": ("Walk.png", 7),
        "run": ("Run.png", 8),
        "runAttack": ("Run+attack.png", 7),
        "attack": ("Attack_1.png", 5),
    }
    warrior_payload = {"strips": {}, "effect": "slash"}
    for key, (fname, frames) in warrior_strips.items():
        strip_path = warrior_root / fname
        if strip_path.exists():
            warrior_payload["strips"][key] = {"uri": strip_uri(strip_path), "frames": frames}
    characters_data["warrior"] = warrior_payload

    payload = {
        "windMph": float(wind_speed or 0),
        "windDirDeg": float(wind_dir_deg) if wind_dir_deg is not None else None,
        "lightningCount": int(lightning_count or 0),
        "ingestRate": float(ingest_rate or 0),
        "lightningNear": bool(lightning_count),
    }

    if missing:
        status_boot = " | ".join(missing)

    html = f"""
    <style>
      .wind-arena {{
        position: relative;
        border: 1px solid #2b3550;
        border-radius: 14px;
        overflow: hidden;
        background: radial-gradient(circle at 15% 10%, rgba(97,165,255,0.12), transparent 34%), #0b0f16;
        box-shadow: 0 18px 46px rgba(0,0,0,0.55);
        padding: 10px 10px 12px 10px;
        min-height: 260px;
      }}
      .wind-arena__bar {{
        display: flex;
        justify-content: space-between;
        align-items: center;
        font-size: 0.92rem;
        color: #cfd6e5;
        margin-bottom: 6px;
        letter-spacing: 0.6px;
      }}
      .wind-arena__status {{
        font-size: 0.8rem;
        color: #8fb7ff;
        text-transform: uppercase;
        letter-spacing: 0.9px;
      }}
      .wind-arena__canvas {{
        display: block;
        width: 100%;
        height: 230px;
        border-radius: 10px;
        background: #0b0f16;
        border: 1px solid rgba(97,165,255,0.15);
      }}
      .wind-arena__overlay {{
        position: absolute;
        inset: 10px;
        border-radius: 10px;
        background: linear-gradient(135deg, rgba(97,165,255,0.08), rgba(75,208,194,0.08));
        color: #dce6ff;
        display: flex;
        align-items: center;
        justify-content: space-between;
        padding: 12px;
        font-size: 0.88rem;
        pointer-events: none;
        transition: opacity 0.4s ease;
      }}
    </style>
    <div class="wind-arena" id="windArena">
      <div class="wind-arena__bar">
        <span>Wind Arena - Spearman</span>
        <span class="wind-arena__status" id="windArenaStatus">{status_boot}</span>
      </div>
      <canvas id="spriteCanvas" width="400" height="230" class="wind-arena__canvas"></canvas>
      <div class="wind-arena__overlay" id="windArenaOverlay">
        <span>{status_boot}</span>
        <span style="font-size:0.8rem;color:#8fb7ff;">wind speed {float(wind_speed or 0):.1f} mph</span>
      </div>
    </div>
    <script>{sprite_js}</script>
    <script>
      (async function() {{
        const payload = {json.dumps(payload)};
        const charactersData = {json.dumps(characters_data)};
        const sheetOk = {str(sheet_ok).lower()};
        const img = new Image();
        const statusEl = document.getElementById("windArenaStatus");
        const overlayEl = document.getElementById("windArenaOverlay");
        function setStatus(msg, ok=true) {{
          if (statusEl) {{
            statusEl.textContent = msg;
            statusEl.style.color = ok ? "#8fb7ff" : "#ff8080";
          }}
          if (overlayEl) {{
            overlayEl.style.opacity = ok ? 0 : 1;
            const label = overlayEl.querySelector("span");
            if (label) label.textContent = msg;
          }}
        }}
        let triedBlob = false;
        let triedDataUri = false;
        let triedRelative = false;

        const tryMount = (useImg) => {{
          try {{
            const el = document.getElementById("spriteCanvas");
            if (window.SpriteSheetPlayer && el) {{
              window.__spritePlayer = window.SpriteSheetPlayer.mount(el, useImg ? img : null, payload, charactersData);
              setStatus(useImg ? "Live" : "Live (characters)");
            }} else {{
              setStatus("Sprite player missing", false);
            }}
          }} catch (err) {{
            console.error("Sprite mount error", err);
            setStatus("Sprite error", false);
          }}
        }};

        const tryBlob = () => {{
          try {{
            const b64 = "{sheet_b64}";
            const bin = atob(b64);
            const len = bin.length;
            const bytes = new Uint8Array(len);
            for (let i = 0; i < len; i++) bytes[i] = bin.charCodeAt(i);
            const blob = new Blob([bytes], {{ type: "image/png" }});
            const url = URL.createObjectURL(blob);
            triedBlob = true;
            img.src = url;
          }} catch (e) {{
            console.error("sheet decode error", e);
            triedBlob = true;
          }}
        }};

        const tryDataUri = () => {{
          triedDataUri = true;
          img.src = "{sheet_data_uri}";
        }};

        const tryRelative = () => {{
          triedRelative = true;
          img.src = "images/sprite-full.png";
        }};

        img.onload = function() {{
          tryMount(true);
        }};

        img.onerror = function() {{
          if (!triedDataUri && sheetOk) {{
            setStatus("Retry sheet (data URI)...", false);
            tryDataUri();
            return;
          }}
          if (!triedRelative && sheetOk) {{
            setStatus("Retry sheet (relative)...", false);
            tryRelative();
            return;
          }}
          tryMount(false);
        }};

        if (sheetOk) {{
          if (!triedBlob) {{
            tryBlob();
          }} else if (!triedDataUri) {{
            tryDataUri();
          }} else {{
            tryRelative();
          }}
        }} else {{
          tryMount(false);
        }}
      }})();
    </script>
    """
    components.html(html, height=320)


# ------------------------
# Sidebar controls
# ------------------------
st.sidebar.header("Controls")

if "hours" not in st.session_state:
    st.session_state.hours = 24
if "filter_mode" not in st.session_state:
    st.session_state.filter_mode = "Window (hours)"
if "date_range" not in st.session_state:
    today = pd.Timestamp.utcnow().date()
    st.session_state.date_range = (today - pd.Timedelta(days=1), today)

filter_mode = st.sidebar.radio(
    "Range mode",
    ["Window (hours)", "Custom dates", "All time"],
    index=["Window (hours)", "Custom dates", "All time"].index(st.session_state.filter_mode)
)
st.session_state.filter_mode = filter_mode

since_epoch = 0
until_epoch = None
window_desc = "this window"

if filter_mode == "Window (hours)":
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
    window_desc = f"the last {st.session_state.hours}h"

elif filter_mode == "Custom dates":
    date_range = st.sidebar.date_input(
        "Date range (inclusive)",
        value=st.session_state.date_range,
    )
    if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
        start_date, end_date = date_range
    else:
        start_date = end_date = pd.Timestamp.utcnow().date()
    if end_date < start_date:
        start_date, end_date = end_date, start_date
    st.session_state.date_range = (start_date, end_date)
    start_ts = pd.Timestamp(start_date)
    end_ts = pd.Timestamp(end_date) + pd.Timedelta(days=1)
    since_epoch = int(start_ts.timestamp())
    until_epoch = int(end_ts.timestamp())
    window_desc = f"{start_date} to {end_date}"
else:
    # All time
    since_epoch = 0
    until_epoch = None
    window_desc = "all time"

gauge_container = st.sidebar.container()

# ------------------------
# Data load and transforms
# ------------------------
now_ts = pd.Timestamp.utcnow()
recent_cutoff_epoch = int((pd.Timestamp.utcnow() - pd.Timedelta(hours=1)).timestamp())
hub_recent_cutoff_epoch = int((pd.Timestamp.utcnow() - pd.Timedelta(hours=24)).timestamp())
hub_recent_cutoff_epoch = int((pd.Timestamp.utcnow() - pd.Timedelta(hours=24)).timestamp())

tempest_until_clause = "AND obs_epoch <= :until" if until_epoch is not None else ""
airlink_until_clause = "AND ts <= :until" if until_epoch is not None else ""

tempest = load_df(
    f"""
    SELECT
        obs_epoch,
        air_temperature,
        relative_humidity,
        station_pressure,
        wind_avg,
        wind_dir
    FROM obs_st
    WHERE obs_epoch >= :since
    {tempest_until_clause}
    ORDER BY obs_epoch
    """,
    {"since": since_epoch, **({"until": until_epoch} if until_epoch is not None else {})},
)

airlink = load_df(
    f"""
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
    {airlink_until_clause}
    ORDER BY ts
    """,
    {"since": since_epoch, **({"until": until_epoch} if until_epoch is not None else {})},
)

# Tempest transforms
tempest_latest = None
tempest_temp_delta = None
tempest_hum_delta = None
tempest_pressure_delta = None
tempest_wind_delta = None
tempest_extremes = {}

if not tempest.empty:
    tempest["time"] = epoch_to_dt(tempest["obs_epoch"])
    tempest["air_temperature_f"] = c_to_f(tempest["air_temperature"])
    tempest["heat_index_f"] = compute_heat_index(
        tempest["air_temperature_f"],
        tempest["relative_humidity"]
    )
    tempest["pressure_inhg"] = hpa_to_inhg(tempest["station_pressure"])
    tempest["wind_speed_mph"] = mps_to_mph(tempest["wind_avg"])
    if "wind_dir" in tempest:
        tempest["wind_dir_deg"] = tempest["wind_dir"].astype(float)

    tempest_latest = tempest.iloc[-1]
    tempest_temp_delta = delta_over_window(tempest["air_temperature_f"])
    tempest_hum_delta = delta_over_window(tempest["relative_humidity"])
    tempest_pressure_delta = delta_over_window(tempest["pressure_inhg"])
    tempest_wind_delta = delta_over_window(tempest["wind_speed_mph"])
    tempest_extremes = daily_extremes(
        tempest,
        "time",
        ["air_temperature_f", "relative_humidity", "pressure_inhg", "wind_speed_mph"],
    )
    lightning_strikes_window = int(tempest["lightning_strike_count"].sum()) if "lightning_strike_count" in tempest else 0
    lightning_avg_dist_km = None
    lightning_avg_dist_mi = None
    if "lightning_avg_dist" in tempest:
        nonzero_dist = tempest.loc[tempest["lightning_avg_dist"] > 0, "lightning_avg_dist"]
        if not nonzero_dist.empty:
            lightning_avg_dist_km = float(nonzero_dist.iloc[-1])
            lightning_avg_dist_mi = lightning_avg_dist_km * 0.621371
else:
    lightning_strikes_window = 0
    lightning_avg_dist_km = None
    lightning_avg_dist_mi = None
lightning_active = lightning_strikes_window > 0

# AirLink transforms
airlink_latest = None
aqi_share_df = pd.DataFrame()

if not airlink.empty:
    airlink["time"] = epoch_to_dt(airlink["ts"])
    airlink["aqi_pm25"] = airlink["pm_2p5"].apply(compute_pm25_aqi)
    airlink["aqi_pm25_last_1_hour"] = airlink["pm_2p5_last_1_hour"].apply(compute_pm25_aqi)
    airlink["aqi_pm25_last_24_hours"] = airlink["pm_2p5_last_24_hours"].apply(compute_pm25_aqi)
    airlink["aqi_pm25_nowcast"] = airlink["pm_2p5_nowcast"].apply(compute_pm25_aqi)

    airlink_latest = airlink.iloc[-1]
    aqi_share_df = aqi_zone_share(airlink["aqi_pm25"])

# gauge highlights when window change is notable
highlight_map = {
    "temp": tempest_temp_delta is not None and abs(tempest_temp_delta) >= 3,
    "hum": tempest_hum_delta is not None and abs(tempest_hum_delta) >= 8,
    "pressure": tempest_pressure_delta is not None and abs(tempest_pressure_delta) >= 0.08,
    "wind": tempest_wind_delta is not None and abs(tempest_wind_delta) >= 4,
    "aqi": (not airlink.empty) and (abs(delta_over_window(airlink["aqi_pm25"])) >= 15 if "aqi_pm25" in airlink else False),
}

# Sidebar gauges using freshest data
render_sidebar_gauges(
    gauge_container,
    tempest_latest=tempest_latest,
    airlink_latest=airlink_latest,
    highlights=highlight_map,
)

ingest_sources = []
airlink_activity = recent_activity("airlink_obs", "ts", recent_cutoff_epoch)
station_activity = recent_activity("obs_st", "obs_epoch", recent_cutoff_epoch, "device_id", TEMPEST_STATION_ID)
hub_activity = recent_activity(
    "raw_events",
    "received_at_epoch",
    hub_recent_cutoff_epoch,
    message_col="message_type",
    message_types=["connection_opened", "ack"],
)

for label, activity, colors in [
    ("AirLink", airlink_activity, ("#4bd0c2", "#7be7d9")),
    ("Tempest Station", station_activity, ("#59c5ff", "#8cc5ff")),
    ("Tempest Hub", hub_activity, ("#9c7bff", "#d8c6ff")),
]:
    latency_minutes = minutes_since_epoch(activity["last_epoch"], now_ts)
    # Hub pings are sparse; treat them as heartbeat instead of continuous flow.
    if label == "Tempest Hub":
        health = ingest_health(latency_minutes, fresh=60, stale=24 * 60)
    else:
        health = ingest_health(latency_minutes)
    fill = max(0.08, min(1.0, health))
    event_rate = activity["count"] or 0
    load_text = f"{event_rate} evt/hr" if event_rate > 0 else ("standby" if label == "Tempest Hub" else "0 evt/hr")
    latency_text = latency_label(latency_minutes) if event_rate > 0 else ("Standby" if label == "Tempest Hub" else latency_label(latency_minutes))
    ingest_sources.append({
        "name": label,
        "latency_text": latency_text,
        "latency_minutes": latency_minutes,
        "load_text": load_text,
        "last_seen": latest_ts_str(activity["last_epoch"]),
        "fill": fill,
        "pulse_speed": flow_speed_from_load(event_rate),
        "colors": colors,
        "recent_count": event_rate,
    })

latency_values = [s["latency_minutes"] for s in ingest_sources if s["latency_minutes"] is not None]
avg_latency_minutes = sum(latency_values) / len(latency_values) if latency_values else None
avg_latency_text = latency_label(avg_latency_minutes) if avg_latency_minutes is not None else None
total_recent = sum(s["recent_count"] for s in ingest_sources if s["recent_count"] is not None)

render_ingest_banner(ingest_sources, total_recent, avg_latency_text=avg_latency_text)

# ------------------------
# Tabs layout: Overview, Air, Wind, Data Quality, Raw
# ------------------------
tabs = st.tabs(["Overview", "Air Quality", "Wind", "Data Quality", "Raw"])

# Overview tab
with tabs[0]:
    st.subheader("Overview")
    story = story_lines(tempest, airlink, window_desc)
    top_cols = st.columns(4)
    if tempest_latest is not None:
        top_cols[0].metric("Outside Temp (F)", f"{tempest_latest.air_temperature_f:.1f}", None if tempest_temp_delta is None else f"{tempest_temp_delta:+.1f} vs start")
        top_cols[1].metric("Humidity (%)", f"{tempest_latest.relative_humidity:.0f}", None if tempest_hum_delta is None else f"{tempest_hum_delta:+.0f} vs start")
        top_cols[2].metric("Pressure (inHg)", f"{tempest_latest.pressure_inhg:.2f}", None if tempest_pressure_delta is None else f"{tempest_pressure_delta:+.2f} vs start")
    if airlink_latest is not None:
        aqi_delta = delta_over_window(airlink["aqi_pm25"]) if not airlink.empty else None
        top_cols[3].metric(
            "AQI (PM2.5)",
            f"{airlink_latest.aqi_pm25:.0f}" if pd.notna(airlink_latest.aqi_pm25) else "--",
            None if aqi_delta is None else f"{aqi_delta:+.0f} vs start",
        )

    if story:
        st.markdown("#### Window Story")
        for line in story:
            st.markdown(f"- {line}")
    if not tempest.empty:
        t_hi = tempest["air_temperature_f"].max()
        t_lo = tempest["air_temperature_f"].min()
        st.markdown(f"**Window highs/lows:** {t_hi:.1f}F / {t_lo:.1f}F")

    overview_charts = st.columns(2)
    if not tempest.empty:
        temp_long = tempest.melt(
            id_vars=["time"], value_vars=["air_temperature_f", "heat_index_f"], var_name="metric"
        )
        temp_long["metric"] = temp_long["metric"].map(
            {"air_temperature_f": "Air Temperature", "heat_index_f": "Heat Index"}
        )
        overview_charts[0].altair_chart(clean_chart(temp_long, height=240, title="Temperature vs Heat Index"), use_container_width=True)
    if not airlink.empty:
        aqi_long = airlink[["time", "aqi_pm25"]].rename(columns={"aqi_pm25": "value"})
        aqi_long["metric"] = "AQI (PM2.5)"
        overview_charts[1].altair_chart(clean_chart(aqi_long, height=240, title="AQI Over Time"), use_container_width=True)

# Air Quality tab
with tabs[1]:
    st.subheader("Air Quality")
    if airlink.empty:
        st.info("No AirLink data in selected window.")
    else:
        aqi_cols = st.columns(3)
        aqi_cols[0].metric("Current AQI (PM2.5)", f"{airlink_latest.aqi_pm25:.0f}" if pd.notna(airlink_latest.aqi_pm25) else "--", aqi_category(airlink_latest.aqi_pm25))
        aqi_cols[1].metric("1 Hour AQI (PM2.5)", f"{airlink_latest.aqi_pm25_last_1_hour:.0f}" if pd.notna(airlink_latest.aqi_pm25_last_1_hour) else "--", aqi_category(airlink_latest.aqi_pm25_last_1_hour))
        aqi_cols[2].metric("NowCast AQI (PM2.5)", f"{airlink_latest.aqi_pm25_nowcast:.0f}" if pd.notna(airlink_latest.aqi_pm25_nowcast) else "--", aqi_category(airlink_latest.aqi_pm25_nowcast))

        aqi_delta_air = delta_over_window(airlink["aqi_pm25"]) if not airlink.empty else None
        if aqi_delta_air is not None and pd.notna(airlink_latest.aqi_pm25):
            if airlink_latest.aqi_pm25 > 100 and aqi_delta_air >= 0:
                cta = "AQI elevated; close windows & run purifier."
            elif airlink_latest.aqi_pm25 > 100 and aqi_delta_air < 0:
                cta = "AQI elevated but improving; ventilate cautiously."
            elif airlink_latest.aqi_pm25 <= 50:
                cta = "Air is clean; ventilation OK."
            else:
                cta = "Moderate air; light ventilation if needed."
            st.markdown(f"**Action hint:** {cta}")

        charts = st.columns(2)
        aqi_long = airlink[["time", "aqi_pm25"]].rename(columns={"aqi_pm25": "value"})
        aqi_long["metric"] = "AQI (PM2.5)"
        charts[0].altair_chart(clean_chart(aqi_long, height=260, title="AQI Over Time"), use_container_width=True)

        pm_bars = pd.DataFrame({
            "label": ["PM1", "PM2.5", "PM10"],
            "value": [airlink_latest.pm_1, airlink_latest.pm_2p5, airlink_latest.pm_10],
        })
        charts[1].altair_chart(bar_chart(pm_bars, height=200, title="Particulate Snapshot (ug/m3)"), use_container_width=True)

        pm_long = airlink.melt(
            id_vars=["time"],
            value_vars=["pm_1", "pm_2p5", "pm_10"],
            var_name="metric",
            value_name="value",
        )
        pm_long["metric"] = pm_long["metric"].map({"pm_1": "PM1", "pm_2p5": "PM2.5", "pm_10": "PM10"})
        st.altair_chart(clean_chart(pm_long, height=240, title="Particulate Over Time"), use_container_width=True)

        if not aqi_share_df.empty:
            zone_plot = aqi_share_df.copy()
            zone_plot["percent"] = zone_plot["share"] * 100
            zone_plot = zone_plot.rename(columns={"category": "label", "percent": "value"})
            st.markdown("#### Time in AQI Zones")
            st.altair_chart(
                bar_chart(zone_plot[["label", "value"]], height=160, title="Share of observations by AQI category (%)"),
                use_container_width=True,
            )

# Wind tab
with tabs[2]:
    st.subheader("Wind")
    if tempest.empty:
        st.info("No Tempest data in selected window.")
    else:
        wind_speed_now = float(tempest_latest.wind_speed_mph) if pd.notna(tempest_latest.wind_speed_mph) else 0.0
        wind_dir_deg = float(tempest_latest.wind_dir_deg) if "wind_dir_deg" in tempest_latest and pd.notna(tempest_latest.wind_dir_deg) else None

        compass_dir = wind_dir_deg if wind_dir_deg is not None else 0.0
        compass_arrow = compass_dir
        # simple cardinal text
        def deg_to_cardinal(deg):
            if deg is None:
                return "--"
            dirs = ["N","NE","E","SE","S","SW","W","NW"]
            ix = int((deg + 22.5) // 45) % 8
            return dirs[ix]
        cardinal = deg_to_cardinal(wind_dir_deg)

        # Scale center orb with wind speed (cap size)
        orb_scale = min(1.8, 1.0 + (wind_speed_now / 20.0))
        orb_size = 34 * orb_scale

        sprite_js = Path("static/sprite_player.js").read_text() if Path("static/sprite_player.js").exists() else "window.SpriteSheetPlayer={mount:()=>null};"
        import base64
        def strip_uri(path):
            return "data:image/png;base64," + base64.b64encode(path.read_bytes()).decode("ascii")

        spearman_root = Path("images/Skeleton_Spearman")
        archer_root = Path("images/Skeleton_Archer")
        warrior_root = Path("images/Skeleton_Warrior")

        characters_data = {}

        spearman_strips = {
            "idle": ("Idle.png", 7),
            "walk": ("Walk.png", 7),
            "run": ("Run.png", 6),
            "runAttack": ("Run+attack.png", 5),
            "attack": ("Attack_1.png", 4),
        }
        spearman_payload = {"strips": {}, "effect": "spear"}
        for key, (fname, frames) in spearman_strips.items():
            strip_path = spearman_root / fname
            if strip_path.exists():
                spearman_payload["strips"][key] = {"uri": strip_uri(strip_path), "frames": frames}
        characters_data["spearman"] = spearman_payload

        archer_strips = {
            "idle": ("Idle.png", 7),
            "walk": ("Walk.png", 8),
            "run": ("Walk.png", 8),
            "runAttack": ("Shot_2.png", 15),
            "attack": ("Shot_1.png", 15),
        }
        archer_payload = {"strips": {}, "effect": "arrow"}
        for key, (fname, frames) in archer_strips.items():
            strip_path = archer_root / fname
            if strip_path.exists():
                archer_payload["strips"][key] = {"uri": strip_uri(strip_path), "frames": frames}
        arrow_path = archer_root / "Arrow.png"
        if arrow_path.exists():
            archer_payload["projectileUri"] = strip_uri(arrow_path)
        characters_data["archer"] = archer_payload

        warrior_strips = {
            "idle": ("Idle.png", 7),
            "walk": ("Walk.png", 7),
            "run": ("Run.png", 8),
            "runAttack": ("Run+attack.png", 7),
            "attack": ("Attack_1.png", 5),
        }
        warrior_payload = {"strips": {}, "effect": "slash"}
        for key, (fname, frames) in warrior_strips.items():
            strip_path = warrior_root / fname
            if strip_path.exists():
                warrior_payload["strips"][key] = {"uri": strip_uri(strip_path), "frames": frames}
        characters_data["warrior"] = warrior_payload

        recent_cutoff_epoch = int((pd.Timestamp.utcnow() - pd.Timedelta(hours=2)).timestamp())
        if "lightning_strike_count" in tempest:
            lightning_recent = int(tempest.loc[tempest["obs_epoch"] >= recent_cutoff_epoch, "lightning_strike_count"].sum())
        else:
            lightning_recent = 0
        lightning_near = bool(lightning_recent > 0 and lightning_avg_dist_mi is not None and lightning_avg_dist_mi <= 20)

        sprite_payload = {
            "windMph": float(wind_speed_now or 0),
            "windDirDeg": float(wind_dir_deg) if wind_dir_deg is not None else None,
            "lightningCount": int(lightning_strikes_window or 0),
            "lightningNear": lightning_near,
            "ingestRate": float(total_recent or 0),
        }

        compass_html = f"""
        <style>
        .compass-wrap {{
            position: relative;
            width: 320px;
            height: 320px;
            margin: 0 0 10px 0;
            border-radius: 18px;
            background: radial-gradient(circle at 50% 45%, rgba(97,165,255,0.12), rgba(15,17,23,0.9) 60%);
            border: 1px solid #1f2736;
            box-shadow: 0 12px 26px rgba(0,0,0,0.35);
        }}
        .compass-face {{
            position: absolute;
            inset: 20px;
            border-radius: 50%;
            background: radial-gradient(circle, #0e141d, #0a0e15);
            border: 1px solid #273040;
            box-shadow: inset 0 0 18px rgba(0,0,0,0.4);
        }}
        .compass-ticks {{
            position: absolute;
            inset: 20px;
        }}
        .compass-ticks::before {{
            content: "";
            position: absolute;
            inset: 0;
            border-radius: 50%;
            border: 1px dashed rgba(255,255,255,0.08);
        }}
        .compass-arrow {{
            position: absolute;
            top: 50%;
            left: 50%;
            width: 6px;
            height: 120px;
            transform-origin: 50% 90%;
            transform: translate(-50%, -90%) rotate({compass_arrow}deg);
        }}
        .compass-arrow::before {{
            content: "";
            position: absolute;
            top: 0;
            left: 50%;
            transform: translateX(-50%);
            width: 0;
            height: 0;
            border-left: 10px solid transparent;
            border-right: 10px solid transparent;
            border-bottom: 26px solid #ff6b6b;
            filter: drop-shadow(0 0 10px rgba(255,107,107,0.4));
        }}
        .compass-arrow::after {{
            content: "";
            position: absolute;
            bottom: 0;
            left: 50%;
            transform: translate(-50%, 12px);
            width: 10px;
            height: 52px;
            background: linear-gradient(180deg, #4bd0c2, #1e8f4b);
            border-radius: 10px;
            filter: drop-shadow(0 0 8px rgba(75,208,194,0.35));
        }}
        .compass-center {{
            position: absolute;
            top: 50%;
            left: 50%;
            transform: translate(-50%, -50%);
            width: {orb_size:.1f}px;
            height: {orb_size:.1f}px;
            border-radius: 50%;
            background: radial-gradient(circle, #6ac8ff, #2c3745);
            box-shadow: 0 0 12px rgba(97,165,255,0.6);
        }}
        .compass-labels {{
            position: absolute;
            top: 8px;
            left: 0;
            right: 0;
            display: flex;
            justify-content: space-between;
            padding: 0 12px;
            color: #9aa4b5;
            font-weight: 700;
        }}
        .compass-labels .center {{
            position: absolute;
            top: 50%;
            left: 50%;
            transform: translate(-50%, -50%);
            font-size: 0.85rem;
            color: #d8deed;
        }}
        .compass-arena {{
            position: absolute;
            inset: 20px;
            border-radius: 50%;
            overflow: hidden;
            pointer-events: auto;
            cursor: pointer;
        }}
        .compass-arena canvas {{
            width: 100%;
            height: 100%;
            display: block;
        }}
        </style>
        <div class="compass-wrap">
            <div class="compass-labels">
                <span>N</span><span>E</span>
            </div>
            <div class="compass-face"></div>
            <div class="compass-ticks"></div>
            <div class="compass-arena">
                <canvas id="compassSprite" width="280" height="280"></canvas>
            </div>
            <div class="compass-arrow"></div>
            <div class="compass-center"></div>
            <div class="compass-labels" style="top:auto; bottom:8px;">
                <span>W</span><span>S</span>
            </div>
            <div class="compass-labels center">{cardinal} {compass_dir:.0f} deg</div>
        </div>
        <script>{sprite_js}</script>
        <script>
          (function() {{
            const payload = {json.dumps(sprite_payload)};
            const charactersData = {json.dumps(characters_data)};
            const el = document.getElementById("compassSprite");
            if (window.SpriteSheetPlayer && el) {{
              window.__compassPlayer = window.SpriteSheetPlayer.mount(el, null, payload, charactersData);
              el.addEventListener("click", () => {{
                if (window.__compassPlayer && window.__compassPlayer.toggleCharacter) {{
                  window.__compassPlayer.toggleCharacter();
                }}
              }});
            }}
          }})();
        </script>
        """
        comp_col, stats_col = st.columns([1.05, 1])
        with comp_col:
            components.html(compass_html, height=340)
        with stats_col:
            stat_row_1 = st.columns(2)
            stat_row_1[0].metric("Current Wind (mph)", f"{tempest_latest.wind_speed_mph:.1f}")
            stat_row_1[1].metric("Max Wind (window)", f"{tempest['wind_speed_mph'].max():.1f}")
            stat_row_2 = st.columns(2)
            stat_row_2[0].metric("Avg Wind (window)", f"{tempest['wind_speed_mph'].mean():.1f}")
            stat_row_2[1].metric("Lightning strikes (window)", lightning_strikes_window)

        wind_long = tempest[["time", "wind_speed_mph"]].rename(columns={"wind_speed_mph": "value"})
        wind_long["metric"] = "Wind Speed (mph)"
        st.altair_chart(clean_chart(wind_long, height=240, title="Wind Speed"), use_container_width=True)
# Data Quality tab
with tabs[3]:
    st.subheader("Data Quality")
    dq_cols = st.columns(2)
    if tempest_latest is not None:
        tempest_age_minutes = (now_ts - pd.to_datetime(tempest_latest.obs_epoch, unit="s", utc=True)).total_seconds() / 60
        dq_cols[0].metric("Tempest recency (min)", f"{tempest_age_minutes:.1f}")
    else:
        dq_cols[0].metric("Tempest recency (min)", "--")

    if airlink_latest is not None:
        airlink_age_minutes = (now_ts - pd.to_datetime(airlink_latest.ts, unit="s", utc=True)).total_seconds() / 60
        dq_cols[1].metric("AirLink recency (min)", f"{airlink_age_minutes:.1f}")
    else:
        dq_cols[1].metric("AirLink recency (min)", "--")

    if airlink_latest is not None:
        coverage_bars = pd.DataFrame({
            "label": [
                "NowCast data %",
                "1h data %",
                "3h data %",
                "24h data %",
            ],
            "value": [
                airlink_latest.pct_pm_data_nowcast,
                airlink_latest.pct_pm_data_last_1_hour,
                airlink_latest.pct_pm_data_last_3_hours,
                airlink_latest.pct_pm_data_last_24_hours,
            ],
        })
        st.altair_chart(bar_chart(coverage_bars, height=160, title="AirLink data coverage (%)"), use_container_width=True)

    if tempest_extremes:
        st.markdown("#### Window highs/lows (Tempest)")
        records = []
        for metric, vals in tempest_extremes.items():
            records.append({"Metric": metric.replace("_", " ").title(), "Min": vals["min"], "Max": vals["max"]})
        st.dataframe(pd.DataFrame(records))

    # Recency sparkline & freshness badge
    recency_points = []
    if not tempest.empty:
        recency_points.append(
            pd.DataFrame({
                "time": tempest["time"],
                "value": (now_ts - pd.to_datetime(tempest["obs_epoch"], unit="s", utc=True)).dt.total_seconds() / 60,
                "metric": "Tempest recency (min)",
            })
        )
    if not airlink.empty:
        recency_points.append(
            pd.DataFrame({
                "time": airlink["time"],
                "value": (now_ts - pd.to_datetime(airlink["ts"], unit="s", utc=True)).dt.total_seconds() / 60,
                "metric": "AirLink recency (min)",
            })
        )
    if recency_points:
        recency_long = pd.concat(recency_points)
        st.altair_chart(clean_chart(recency_long, height=180, title="Ingest recency over window (minutes)"), use_container_width=True)

    freshness = "red"
    if tempest_latest is not None and airlink_latest is not None:
        worst_age = max(tempest_age_minutes, airlink_age_minutes)
        if worst_age <= 10:
            freshness = "green"
        elif worst_age <= 30:
            freshness = "yellow"
    elif tempest_latest is not None:
        freshness = "green" if tempest_age_minutes <= 10 else "yellow"
    elif airlink_latest is not None:
        freshness = "green" if airlink_age_minutes <= 10 else "yellow"

    st.markdown(f"**Freshness badge:** :{freshness}_circle: based on latest ingest recency")

# Raw tab
with tabs[4]:
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
