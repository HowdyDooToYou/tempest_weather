import sqlite3
import json
import html
import subprocess
import time
import re
from datetime import datetime
from contextlib import closing
import requests
from pathlib import Path
import os
from urllib.parse import urlencode

import altair as alt
import pandas as pd
import streamlit as st
import streamlit.components.v1 as components
from streamlit_autorefresh import st_autorefresh

from src.ui.apply_styles import apply_styles
from src.ui.components.cards import metric_card
from src.ui.shell import render_left_rail, render_header_strip, render_main_layout
from src.pages import home as page_home
from src.pages import trends as page_trends
from src.pages import compare as page_compare
from src.pages import data as page_data
from src.alerting import (
    build_freeze_alert_message,
    delete_alert_config,
    determine_freeze_alerts,
    load_alert_config,
    load_alert_state,
    resolve_alert_recipients,
    save_alert_config,
    save_alert_state,
    send_email,
    send_verizon_sms,
)
from src.config_store import (
    connect as config_connect,
    get_bool,
    get_float,
    set_bool,
    set_float,
)
from src.forecast import parse_tempest_forecast

DB_PATH = os.getenv("TEMPEST_DB_PATH", "data/tempest.db")
TEMPEST_STATION_ID = 475329
TEMPEST_HUB_ID = 475327

PING_TARGETS = {
    "AirLink": "192.168.1.19",
    "Tempest Hub": "192.168.1.26",
}
COLLECTOR_LABELS = {
    "airlink_collector": "AirLink Collector",
    "tempest_collector": "Tempest Collector",
}
COLLECTOR_STALE_SECONDS = {
    "airlink_collector": 180,
    "tempest_collector": 300,
}
COLLECTOR_ERROR_GRACE_SECONDS = 600
WATCHDOG_STALE_SECONDS = int(os.getenv("WATCHDOG_STALE_SECONDS", "600"))
WATCHDOG_LOG_PATH = Path("logs/collector_watchdog.log")
COLLECTOR_COLORS = {
    "airlink_collector": ("#4bd0c2", "#7be7d9"),
    "tempest_collector": ("#59c5ff", "#8cc5ff"),
}
WATCHDOG_COLORS = ("#f4b860", "#ffd59a")

CHART_SCHEME = "tableau10"
CHART_LABEL_COLOR = "#cfd6e5"
CHART_TITLE_COLOR = "#cfd6e5"
CHART_GRID_COLOR = "#1f252f"
CHART_TEXT_COLOR = "#9fb2cc"
THEME_MODE = "dark"
THEME_COLORS = {
    "accent": "#7be7d9",
    "accent2": "#61a5ff",
    "accent3": "#f2a85b",
    "status_ok": "#7be7d9",
    "status_warn": "#f2a85b",
    "status_bad": "#ff7b7b",
    "status_idle": "#9aa4b5",
    "text_primary": "#f4f7ff",
    "text_secondary": "#9aa4b5",
    "text_muted": "#8aa4c8",
    "border": "#232834",
    "surface_3": "#0d1016",
}
GAUGE_COLORS = {
    "temp": "#61a5ff",
    "air_temp": "#4bd0c2",
    "feels": "#7be7d9",
    "hum": "#4bd0c2",
    "pressure": "#9c7bff",
    "wind": "#61a5ff",
    "gust": "#f2a85b",
}
LOCAL_TZ = os.getenv("LOCAL_TZ", "America/New_York")
CONTROL_REFRESH_SECONDS = int(os.getenv("CONTROL_REFRESH_SECONDS", os.getenv("AUTO_REFRESH_SECONDS", "120")))
FORECAST_REFRESH_MINUTES = int(os.getenv("FORECAST_REFRESH_MINUTES", "30"))
FORECAST_UNITS = "imperial"
FREEZE_WARNING_F = float(os.getenv("FREEZE_WARNING_F", "32"))
DEEP_FREEZE_F = float(os.getenv("DEEP_FREEZE_F", "18"))
FREEZE_RESET_F = float(os.getenv("FREEZE_RESET_F", "34"))
ALERTS_WORKER_ENABLED = os.getenv("ALERTS_WORKER_ENABLED", "false").lower() in ("1", "true", "yes", "on")

def resolve_table(candidates):
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        for name in candidates:
            cur.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (name,),
            )
            if cur.fetchone():
                return name
    except Exception:
        pass
    finally:
        try:
            conn.close()
        except Exception:
            pass
    return None

AIRLINK_TABLE = resolve_table(["airlink_current_obs", "airlink_obs"])
AIRLINK_RAW_TABLE = resolve_table(["airlink_raw_all", "airlink_raw"])
RAW_EVENTS_TABLE = resolve_table(["raw_events"])
HEARTBEAT_TABLE = resolve_table(["collector_heartbeat"])

st.set_page_config(
    page_title="Tempest Air & Weather",
    layout="wide"
)

apply_styles()

# ------------------------
# Local UI state (tabs/scroll/refresh)
# ------------------------
state_script = """
<script>
(function() {
  const storage = window.parent.localStorage || window.localStorage;
  const SCROLL_KEY = "tempest:last_scroll";
  window.parent.addEventListener("beforeunload", () => {
    storage.setItem(SCROLL_KEY, String(window.parent.scrollY || 0));
  });
  window.parent.addEventListener("load", () => {
    const y = parseInt(storage.getItem(SCROLL_KEY) || "0", 10);
    if (y) {
      window.parent.setTimeout(() => window.parent.scrollTo(0, y), 200);
    }
  });
})();
</script>
"""
components.html(
    state_script,
    height=0,
)

components.html(
    """
    <script>
    (function() {
      const doc = window.parent && window.parent.document;
      if (!doc) return;
      function applyThemeClass() {
        const bg = window.parent.getComputedStyle(doc.body).backgroundColor || "";
        const nums = bg.match(/\\d+/g);
        let isLight = false;
        if (nums && nums.length >= 3) {
          const r = parseInt(nums[0], 10);
          const g = parseInt(nums[1], 10);
          const b = parseInt(nums[2], 10);
          const luminance = 0.2126 * r + 0.7152 * g + 0.0722 * b;
          isLight = luminance > 200;
        }
        doc.body.classList.toggle("theme-light", isLight);
      }
      applyThemeClass();
      window.parent.setInterval(applyThemeClass, 1500);
    })();
    </script>
    """,
    height=0,
)

# Navigation/page state
if "page" not in st.session_state:
    st.session_state.page = "home"
valid_pages = ["home", "trends", "compare", "data"]
try:
    query_page = st.query_params.get("page")
except Exception:
    query_page = None
if isinstance(query_page, list):
    query_page = query_page[0] if query_page else None
if query_page in valid_pages:
    st.session_state.page = query_page

# ------------------------
# Theming
# ------------------------
st.markdown(
    """
    <style>
    html { font-size: 110%; }
    :root {
        --color-scheme: dark;
        --bg: #0f1115;
        --surface: #161920;
        --surface-2: #1a1d23;
        --surface-3: #0d1016;
        --surface-4: #101722;
        --border: #232834;
        --border-muted: rgba(35,40,52,0.55);
        --text-primary: #f4f7ff;
        --text-secondary: #9aa4b5;
        --text-muted: #8aa4c8;
        --chart-text: var(--text-secondary);
        --chart-title: var(--text-primary);
        --chart-grid: var(--border-muted);
        --accent: #7be7d9;
        --accent-2: #61a5ff;
        --accent-3: #f2a85b;
        --accent-soft: rgba(123,231,217,0.18);
        --accent-border: rgba(123,231,217,0.5);
        --accent-2-soft: rgba(97,165,255,0.18);
        --accent-2-border: rgba(97,165,255,0.35);
        --accent-2-glow: rgba(97,165,255,0.4);
        --accent-3-soft: rgba(242,168,91,0.12);
        --accent-3-border: rgba(242,168,91,0.35);
        --accent-3-glow: rgba(242,168,91,0.6);
        --status-ok: #7be7d9;
        --status-warn: #f2a85b;
        --status-bad: #ff7b7b;
        --status-idle: #9aa4b5;
        --status-ok-border: rgba(123,231,217,0.4);
        --status-warn-border: rgba(242,168,91,0.45);
        --status-bad-border: rgba(255,123,123,0.45);
        --status-bad-border-strong: rgba(255,123,123,0.55);
        --status-idle-border: rgba(154,164,181,0.35);
        --status-warn-soft: rgba(242,168,91,0.08);
        --status-bad-soft: rgba(255,123,123,0.08);
        --status-bad-strong: rgba(255,123,123,0.14);
    }
    body {
        background: var(--bg);
        color: var(--text-primary);
        color-scheme: var(--color-scheme);
    }
    .main { background: var(--bg); }
    .card {
        padding: 14px 16px;
        border-radius: 12px;
        background: var(--surface-2);
        border: 1px solid var(--border);
        color: var(--text-primary);
    }
    .card .title { font-size: 0.9rem; color: var(--text-secondary); margin-bottom: 6px; }
    .card .value { font-size: 1.8rem; font-weight: 700; }
    .pill {
        display: inline-flex;
        align-items: center;
        padding: 6px 12px;
        border-radius: 999px;
        font-size: 0.85rem;
        font-weight: 600;
        border: 1px solid var(--border-muted);
        color: var(--text-primary);
    }
    .chart-header {
        display: flex;
        align-items: center;
        gap: 8px;
        font-weight: 600;
        color: var(--text-primary);
        margin: 6px 0 4px;
    }
    .info-icon {
        display: inline-flex;
        align-items: center;
        justify-content: center;
        width: 18px;
        height: 18px;
        border-radius: 50%;
        border: 1px solid var(--accent-border);
        color: var(--text-muted);
        font-size: 0.72rem;
        cursor: help;
    }
    .metric-grid {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
        gap: 14px;
        margin-bottom: 12px;
    }
    .metric-card {
        padding: 12px 14px;
        border-radius: 14px;
        border: 1px solid var(--border-muted);
        background: linear-gradient(160deg, var(--surface-2), var(--surface-3));
        box-shadow: 0 18px 36px rgba(0,0,0,0.35);
    }
    .metric-card .label {
        font-size: 0.78rem;
        text-transform: uppercase;
        letter-spacing: 0.12em;
        color: var(--text-muted);
        margin-bottom: 6px;
    }
    .metric-card .value {
        font-size: 1.7rem;
        font-weight: 700;
        color: var(--text-primary);
    }
    .metric-card .sub {
        font-size: 0.78rem;
        color: var(--text-secondary);
        margin-top: 4px;
    }
    .metric-expanders {
        display: block;
        margin-bottom: 14px;
    }
    .metric-expanders [data-testid="stExpander"] {
        border: 1px solid var(--border-muted);
        border-radius: 14px;
        background: linear-gradient(160deg, var(--surface-2), var(--surface-3));
        margin-bottom: 10px;
    }
    .metric-expanders [data-testid="stExpander"] summary {
        padding: 12px 14px;
        font-weight: 600;
        color: var(--text-primary);
    }
    .metric-expanders [data-testid="stExpander"] summary:hover {
        background: var(--accent-soft);
    }
    div[data-testid="stMarkdown"]:has(#hour-presets) + div [data-testid="stRadio"] > div {
        display: flex;
        flex-wrap: wrap;
        gap: 8px;
    }
    div[data-testid="stMarkdown"]:has(#hour-presets) + div [data-testid="stRadio"] label {
        display: inline-flex;
        align-items: center;
        gap: 6px;
        padding: 4px 10px;
        border-radius: 999px;
        border: 1px solid var(--accent-2-border);
        background: linear-gradient(135deg, var(--accent-2-soft), var(--surface-3));
        color: var(--text-primary);
        font-weight: 600;
        font-size: 0.75rem;
        text-transform: uppercase;
        letter-spacing: 0.5px;
        box-shadow: 0 8px 18px var(--accent-2-soft);
    }
    div[data-testid="stMarkdown"]:has(#hour-presets) + div [data-testid="stRadio"] label:hover {
        border-color: var(--accent-border);
        color: var(--text-primary);
    }
    div[data-testid="stMarkdown"]:has(#hour-presets) + div [data-testid="stRadio"] label:has(input:checked) {
        border-color: var(--accent);
        background: linear-gradient(135deg, var(--accent-soft), var(--surface-3));
        box-shadow: 0 10px 22px var(--accent-soft);
        color: var(--text-primary);
    }
    div[data-testid="stMarkdown"]:has(#hour-presets) + div [data-testid="stRadio"] label::before {
        content: "";
        width: 8px;
        height: 8px;
        border-radius: 999px;
        background: var(--accent-2);
        box-shadow: 0 0 10px var(--accent-2-glow);
    }
    div[data-testid="stMarkdown"]:has(#hour-presets) + div [data-testid="stRadio"] label:has(input:checked)::before {
        background: var(--accent);
        box-shadow: 0 0 12px var(--accent-border);
    }
    div[data-testid="stMarkdown"]:has(#hour-presets) + div [data-testid="stRadio"] label div:first-child {
        display: none;
    }
    .section-gap { margin-top: 18px; }
    .gauge-block {
        margin-top: 10px;
        padding: 12px 12px 10px 12px;
        border-radius: 12px;
        background: var(--surface);
        border: 1px solid var(--border);
    }
    .wind-flow {
        position: relative;
        width: 92px;
        height: 92px;
        border-radius: 50%;
        border: 1px solid var(--border);
        background: radial-gradient(circle at 50% 50%, var(--accent-2-soft), transparent 60%);
        display: flex;
        align-items: center;
        justify-content: center;
        overflow: hidden;
    }
    .wind-flow .needle {
        position: absolute;
        width: 8px;
        height: 38px;
        background: linear-gradient(180deg, var(--accent), var(--accent-2));
        border-radius: 999px;
        transform-origin: 50% 100%;
        transform: translate(-50%, -50%) rotate(var(--wind-angle, 0deg));
        left: 50%;
        top: 50%;
        box-shadow: 0 0 12px var(--accent-2-glow);
    }
    .wind-flow .needle::after {
        content: "";
        position: absolute;
        top: -6px;
        left: 50%;
        transform: translateX(-50%);
        width: 0;
        height: 0;
        border-left: 7px solid transparent;
        border-right: 7px solid transparent;
        border-bottom: 10px solid var(--accent);
    }
    .wind-flow .tail {
        position: absolute;
        width: 10px;
        height: 10px;
        border-radius: 999px;
        background: var(--accent);
        box-shadow: 0 0 14px var(--accent-border);
        left: 50%;
        top: 50%;
        transform: translate(-50%, -50%) rotate(var(--wind-angle, 0deg)) translateY(-32px);
        animation: tailPulse 1.6s ease-in-out infinite;
    }
    @keyframes tailPulse {
        0% { opacity: 0.35; transform: translate(-50%, -50%) rotate(var(--wind-angle, 0deg)) translateY(-30px) scale(0.9); }
        50% { opacity: 1; transform: translate(-50%, -50%) rotate(var(--wind-angle, 0deg)) translateY(-36px) scale(1.1); }
        100% { opacity: 0.35; transform: translate(-50%, -50%) rotate(var(--wind-angle, 0deg)) translateY(-30px) scale(0.9); }
    }
    .gauge-header {
        display: flex;
        justify-content: space-between;
        align-items: center;
        color: var(--text-primary);
        font-size: 0.9rem;
        font-weight: 600;
    }
    .gauge-category {
        margin-left: 8px;
        font-size: 0.75rem;
        font-weight: 600;
        color: var(--text-secondary);
    }
    .gauge-track {
        margin-top: 8px;
        width: 100%;
        height: 12px;
        border-radius: 999px;
        background: var(--surface-3);
        border: 1px solid var(--border);
        overflow: hidden;
    }
    .gauge-fill {
        height: 100%;
        border-radius: 999px;
        background: linear-gradient(90deg, var(--accent-2), var(--accent-3));
        transition: width 0.8s ease, filter 0.3s ease;
    }
    .gauge-pulse .gauge-fill {
        animation: pulseBar 1.8s ease-in-out infinite;
    }
    .gauge-muted {
        color: var(--text-secondary);
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
    @media (max-width: 700px) {
        html { font-size: 100%; }
        .ingest-shell { padding: 12px; }
        .ingest-row { grid-template-columns: 1fr; }
        .ingest-latency { text-align: left; }
        .compass-wrap { width: 280px; height: 280px; }
    }
    /* Hero glow */
    .hero-glow {
        position: relative;
    }
    .hero-glow::after {
        content: "";
        position: absolute;
        inset: -8px -12px;
        background: radial-gradient(circle at 20% 20%, var(--accent-2-soft), transparent 35%),
                    radial-gradient(circle at 80% 40%, var(--accent-soft), transparent 30%);
        filter: blur(22px);
        z-index: -1;
    }
    .aurora {
        position: relative;
        height: 10px;
        border-radius: 999px;
        overflow: hidden;
        margin: 4px 0 14px 0;
        background: linear-gradient(90deg, var(--accent-2-soft), var(--accent-soft), var(--accent-2-soft));
    }
    .aurora::before {
        content: "";
        position: absolute;
        inset: 0;
        background: linear-gradient(90deg, var(--border-muted), transparent 40%, var(--border-muted));
        animation: auroraSlide 8s linear infinite;
        mix-blend-mode: screen;
    }
    .dash-title {
        font-size: 2.4rem;
        font-weight: 800;
        letter-spacing: -0.02em;
        color: var(--text-primary);
    }
    .overview-title {
        font-size: 1.5rem;
    }
    .gauge-clock .clock-time {
        font-size: 1.05rem;
        font-weight: 700;
        font-variant-numeric: tabular-nums;
    }
    .gauge-clock .clock-date {
        margin-top: 4px;
        font-variant-numeric: tabular-nums;
    }
    .gauge-clock .gauge-track {
        height: 8px;
        margin-top: 8px;
    }
    .gauge-clock .gauge-fill {
        background: linear-gradient(90deg, var(--accent), var(--accent-2));
    }
    .overview-header {
        display: flex;
        align-items: center;
        justify-content: space-between;
        gap: 12px;
        margin-bottom: 4px;
    }
    .wind-flag {
        display: inline-flex;
        align-items: center;
        gap: 6px;
        padding: 4px 10px;
        border-radius: 999px;
        border: 1px solid var(--accent-2-border);
        background: linear-gradient(135deg, var(--accent-2-soft), var(--surface-3));
        color: var(--text-primary);
        font-weight: 600;
        font-size: 0.82rem;
        box-shadow: 0 10px 22px var(--accent-2-soft);
    }
    .wind-flag .arrow {
        display: inline-flex;
        align-items: center;
        justify-content: center;
        width: 20px;
        height: 20px;
        border-radius: 50%;
        border: 1px solid var(--border-muted);
        background: radial-gradient(circle at 30% 30%, var(--accent-2-soft), var(--surface-3));
        box-shadow: 0 0 12px var(--accent-2-glow);
        color: var(--text-primary);
        font-size: 0.7rem;
        transform: rotate(0deg);
    }
    .wind-flag .wind-dir {
        font-weight: 700;
        letter-spacing: 0.3px;
    }
    .wind-flag .wind-speed {
        padding: 2px 6px;
        border-radius: 999px;
        border: 1px solid var(--border-muted);
        background: var(--surface-3);
        color: var(--text-primary);
        font-size: 0.7rem;
        font-weight: 700;
        letter-spacing: 0.4px;
    }
    .aqi-badge {
        display: inline-flex;
        align-items: center;
        gap: 6px;
        padding: 4px 10px;
        border-radius: 999px;
        border: 1px solid var(--aqi-border, rgba(255,255,255,0.2));
        background: linear-gradient(135deg, var(--aqi-tint, rgba(16,24,36,0.6)), var(--surface-3));
        color: var(--text-primary);
        font-weight: 600;
        font-size: 0.82rem;
        box-shadow: 0 10px 22px rgba(0,0,0,0.18);
    }
    .aqi-badge .aqi-dot {
        width: 10px;
        height: 10px;
        border-radius: 999px;
        background: var(--aqi-color, #2d2f36);
        box-shadow: 0 0 10px var(--aqi-color, #2d2f36);
    }
    .aqi-badge .aqi-label {
        font-size: 0.62rem;
        text-transform: uppercase;
        letter-spacing: 0.7px;
        color: var(--text-secondary);
    }
    .aqi-badge .aqi-value {
        font-weight: 700;
        color: var(--text-primary);
    }
    .aqi-badge .aqi-status {
        font-size: 0.7rem;
        color: var(--text-secondary);
    }
    .overview-actions {
        display: flex;
        align-items: center;
        justify-content: flex-end;
        gap: 8px;
        margin: 4px 0 10px 0;
    }
    .overview-badges {
        display: flex;
        align-items: center;
        gap: 8px;
    }
    .header-badges {
        display: flex;
        flex-wrap: wrap;
        justify-content: flex-start;
        gap: 8px;
        margin-top: 6px;
    }
    .header-badges .wind-flag,
    .header-badges .sun-badge,
    .header-badges .aqi-badge {
        font-size: 0.75rem;
        padding: 4px 8px;
    }
    .header-badges .wind-flag .wind-speed {
        font-size: 0.66rem;
    }
    .header-badges .aqi-badge .aqi-status {
        font-size: 0.66rem;
    }
    .alert-banner {
        margin-top: 10px;
        padding: 8px 12px;
        border-radius: 12px;
        border: 1px solid rgba(97,165,255,0.45);
        background: linear-gradient(135deg, rgba(97,165,255,0.2), var(--surface-3));
        display: flex;
        flex-wrap: wrap;
        align-items: center;
        gap: 10px;
        box-shadow: 0 12px 24px rgba(0,0,0,0.2);
    }
    .alert-banner .alert-title {
        font-weight: 700;
        letter-spacing: 0.3px;
    }
    .alert-banner .alert-meta {
        font-size: 0.8rem;
        color: var(--text-secondary);
    }
    .alert-banner.freeze {
        border-color: rgba(97,165,255,0.55);
        background: linear-gradient(135deg, rgba(97,165,255,0.25), var(--surface-3));
    }
    .alert-banner.deep-freeze {
        border-color: rgba(89,197,255,0.6);
        background: linear-gradient(135deg, rgba(89,197,255,0.28), var(--surface-3));
    }
    .sun-badge {
        display: inline-flex;
        align-items: center;
        gap: 8px;
        padding: 4px 10px;
        border-radius: 999px;
        border: 1px solid var(--accent-3-border);
        background: var(--accent-3-soft);
        color: var(--text-primary);
        font-weight: 600;
        font-size: 0.82rem;
    }
    .sun-badge .sun-icon {
        width: 20px;
        height: 20px;
        border-radius: 50%;
        background: radial-gradient(circle at 30% 30%, var(--accent-3-glow), var(--accent-3) 60%);
        box-shadow: 0 0 10px var(--accent-3-glow);
        position: relative;
    }
    .sun-badge .sun-icon::after {
        content: "";
        position: absolute;
        inset: -4px;
        border-radius: 50%;
        border: 1px dashed var(--accent-3-border);
        animation: sunPulse 6s linear infinite;
    }
    @keyframes sunPulse {
        from { transform: rotate(0deg); }
        to { transform: rotate(360deg); }
    }
    .sun-badge .moon-icon {
        width: 20px;
        height: 20px;
        border-radius: 50%;
        background: radial-gradient(circle at 35% 35%, var(--accent-2-soft), var(--accent-2) 65%);
        box-shadow: 0 0 10px var(--accent-2-glow);
        position: relative;
    }
    .sun-badge .moon-icon::after {
        content: "";
        position: absolute;
        inset: -4px;
        border-radius: 50%;
        border: 1px dashed var(--accent-2-border);
        animation: sunPulse 8s linear infinite;
    }
    .sunrise-day {
        animation: sunFade 6s ease-in-out infinite;
    }
    .sunrise-night {
        animation: moonFade 6s ease-in-out infinite;
    }
    @keyframes sunFade {
        0% { opacity: 0.4; }
        50% { opacity: 1; }
        100% { opacity: 0.4; }
    }
    @keyframes moonFade {
        0% { opacity: 0.5; }
        50% { opacity: 1; }
        100% { opacity: 0.5; }
    }
    @keyframes auroraSlide {
        from { transform: translateX(-30%); }
        to { transform: translateX(30%); }
    }
    body,
    .stApp,
    [data-testid="stAppViewContainer"] {
        background: var(--bg);
        color: var(--text-primary);
    }
    body .stMarkdown,
    body .stCaption,
    body .stText,
    body .stSubheader,
    body h1,
    body h2,
    body h3,
    body h4,
    .stApp .stMarkdown,
    .stApp .stCaption,
    .stApp .stText,
    .stApp .stSubheader,
    .stApp h1,
    .stApp h2,
    .stApp h3,
    .stApp h4 {
        color: var(--text-primary);
    }
    [data-baseweb="tab"] { color: var(--text-secondary); }
    [data-baseweb="tab"][aria-selected="true"] { color: var(--text-primary); }
    .ingest-shell {
        margin: 4px 0 16px 0;
        padding: 14px 16px 16px 16px;
        border-radius: 14px;
        border: 1px solid var(--border);
        background:
            radial-gradient(circle at 10% 10%, var(--accent-2-soft), transparent 38%),
            radial-gradient(circle at 80% 20%, var(--accent-soft), transparent 32%),
            var(--surface-3);
        box-shadow: 0 14px 40px rgba(0,0,0,0.38);
    }
    .ingest-status-row {
        margin-top: 8px;
        display: flex;
        flex-wrap: wrap;
        gap: 10px;
        align-items: center;
    }
    .ingest-chip {
        display: inline-flex;
        align-items: center;
        gap: 8px;
        padding: 6px 10px;
        border-radius: 10px;
        background: var(--surface-4);
        border: 1px solid var(--border);
        color: var(--text-primary);
        font-weight: 600;
        font-size: 0.85rem;
        min-height: 34px;
        max-width: 100%;
    }
    .ingest-dot {
        width: 10px;
        height: 10px;
        border-radius: 999px;
        box-shadow: 0 0 10px currentColor;
    }
    .ingest-chip.ok { border-color: var(--status-ok-border); }
    .ingest-chip.warn { border-color: var(--status-warn-border); }
    .ingest-chip.offline { border-color: var(--status-bad-border); }
    .ingest-chip.standby { border-color: var(--status-idle-border); }
    .ingest-body {
        display: flex;
        flex-direction: column;
        gap: 2px;
        min-width: 0;
    }
    .ingest-title {
        font-weight: 700;
    }
    .ingest-meta {
        font-size: 0.72rem;
        color: var(--text-secondary);
        font-weight: 500;
    }
    .ingest-title,
    .ingest-meta {
        overflow: hidden;
        text-overflow: ellipsis;
        white-space: nowrap;
        max-width: 100%;
    }
    .ingest-pill {
        padding: 2px 6px;
        border-radius: 999px;
        font-size: 0.72rem;
        letter-spacing: 0.6px;
        text-transform: uppercase;
        border: 1px solid var(--border-muted);
        color: var(--text-secondary);
        margin-left: auto;
    }
    .ingest-pill.ok { color: var(--status-ok); border-color: var(--status-ok-border); }
    .ingest-pill.warn { color: var(--status-warn); border-color: var(--status-warn-border); }
    .ingest-pill.offline { color: var(--status-bad); border-color: var(--status-bad-border); }
    .ingest-pill.standby { color: var(--status-idle); border-color: var(--status-idle-border); }
    .ingest-help {
        margin-top: 6px;
        color: var(--text-secondary);
        font-size: 0.78rem;
    }
    .ingest-divider {
        margin: 10px 0;
        height: 1px;
        background: var(--border);
        opacity: 0.7;
    }
    .ingest-snapshot {
        margin-top: 6px;
        color: var(--text-secondary);
        font-size: 0.78rem;
    }
    .ping-toast {
        margin: 6px 0 2px 0;
        font-size: 0.82rem;
        color: var(--text-secondary);
        opacity: 0;
        animation: pingFade 6s ease-in-out forwards;
    }
    @keyframes pingFade {
        0% { opacity: 0; transform: translateY(-2px); }
        12% { opacity: 1; transform: translateY(0); }
        70% { opacity: 1; }
        100% { opacity: 0; transform: translateY(-2px); }
    }
    .ping-btn {
        font-size: 0.7rem;
        padding: 0.15rem 0.45rem;
    }
    .ingest-details {
        margin-top: 8px;
        display: flex;
        flex-direction: column;
        gap: 10px;
    }
    .ingest-detail-row {
        display: grid;
        grid-template-columns: 1.2fr 1.4fr 1fr;
        gap: 10px;
        align-items: center;
        padding: 8px 10px;
        border-radius: 10px;
        border: 1px solid var(--border);
        background: var(--surface-3);
    }
    .ingest-detail-row.warn {
        border-color: var(--status-warn-border);
        box-shadow: 0 0 0 1px var(--status-warn-soft) inset;
    }
    .ingest-detail-row.offline {
        border-color: var(--status-bad-border);
        box-shadow: 0 0 0 1px var(--status-bad-soft) inset;
    }
    .ingest-detail-row.alert {
        border-color: var(--status-bad-border-strong);
        box-shadow: 0 0 0 1px var(--status-bad-strong) inset;
    }
    .ingest-detail-row .meta {
        display: flex;
        align-items: center;
        gap: 10px;
        flex-wrap: wrap;
        font-weight: 700;
        color: var(--text-primary);
    }
    .ingest-detail-row .detail {
        color: var(--text-secondary);
        font-size: 0.85rem;
    }
    .ingest-detail-row .last {
        text-align: right;
        color: var(--text-secondary);
        font-size: 0.82rem;
    }
    @media (max-width: 960px) {
        .ingest-detail-row {
            grid-template-columns: 1fr;
        }
        .ingest-detail-row .last {
            text-align: left;
        }
    }
    .ingest-header-row {
        display: flex;
        justify-content: space-between;
        align-items: center;
        gap: 12px;
        flex-wrap: wrap;
    }
    .ingest-eyebrow {
        color: var(--accent-2);
        font-size: 0.78rem;
        text-transform: uppercase;
        letter-spacing: 0.8px;
        font-weight: 700;
    }
    .ingest-summary {
        color: var(--text-secondary);
        font-size: 0.9rem;
    }
    .ingest-events {
        display: flex;
        align-items: center;
        flex-wrap: wrap;
        gap: 6px;
        margin-top: 6px;
        color: var(--text-primary);
        font-weight: 600;
    }
    .ingest-events span {
        flex: 0 0 auto;
        color: var(--text-secondary);
        font-weight: 700;
    }
    .ingest-info {
        display: inline-flex;
        align-items: center;
        padding: 0 4px;
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
        color: var(--text-primary);
        font-weight: 700;
    }
    .ingest-dot {
        width: 14px;
        height: 14px;
        border-radius: 999px;
        box-shadow: 0 0 12px currentColor;
    }
    .ingest-name { font-size: 1rem; }
    .ingest-sub { font-size: 0.8rem; color: var(--text-secondary); }
    .ingest-bar {
        position: relative;
        width: 100%;
        height: 14px;
        border-radius: 999px;
        background: var(--surface-3);
        border: 1px solid var(--border);
        overflow: hidden;
    }
    .ingest-fill {
        position: absolute;
        inset: 0;
        width: calc(var(--fill, 1) * 100%);
        background: linear-gradient(90deg, var(--fill-start), var(--fill-end));
        filter: drop-shadow(0 0 8px var(--accent-2-glow));
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
        background: var(--surface-4);
        border: 1px solid var(--border);
        color: var(--text-secondary);
        font-size: 0.75rem;
        letter-spacing: 0.6px;
        text-transform: uppercase;
    }
    .ingest-latency {
        text-align: right;
        color: var(--text-secondary);
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
    return pd.to_datetime(series, unit="s", utc=True).dt.tz_convert(LOCAL_TZ)


def fmt_time(dt_value):
    if dt_value is None:
        return "--"
    try:
        return dt_value.strftime("%I:%M %p").lstrip("0")
    except Exception:
        return "--"


def html_escape(value):
    if value is None:
        return "--"
    return html.escape(str(value))


def render_alert_overrides_sync():
    components.html(
        """
        <script>
        (function() {
          const parent = window.parent;
          if (!parent || !parent.document) return;
          const storage = parent.localStorage || window.localStorage;
          const emailKey = "tempest:alert_email_to";
          const smsKey = "tempest:alert_sms_to";
          const emailLabel = "Alert recipient email";
          const smsLabel = "Verizon SMS number";

          function findInput(labelText) {
            const labels = Array.from(parent.document.querySelectorAll("label"));
            const label = labels.find((el) => el.textContent.trim().startsWith(labelText));
            if (!label) return null;
            const wrapper = label.closest('div[data-testid="stTextInput"]');
            if (!wrapper) return null;
            return wrapper.querySelector("input");
          }

          function setInputValue(input, value) {
            const setter = Object.getOwnPropertyDescriptor(parent.HTMLInputElement.prototype, "value").set;
            setter.call(input, value);
            input.dispatchEvent(new parent.Event("input", { bubbles: true }));
            input.dispatchEvent(new parent.Event("change", { bubbles: true }));
          }

          function attachListener(input, key) {
            if (input.dataset.tempestStorage === "1") return;
            input.dataset.tempestStorage = "1";
            input.addEventListener("input", () => {
              const val = input.value || "";
              if (val) {
                storage.setItem(key, val);
              } else {
                storage.removeItem(key);
              }
            });
          }

          function init() {
            const emailInput = findInput(emailLabel);
            const smsInput = findInput(smsLabel);
            if (!emailInput || !smsInput) return false;
            const storedEmail = storage.getItem(emailKey) || "";
            const storedSms = storage.getItem(smsKey) || "";
            if (!emailInput.value && storedEmail) setInputValue(emailInput, storedEmail);
            if (!smsInput.value && storedSms) setInputValue(smsInput, storedSms);
            attachListener(emailInput, emailKey);
            attachListener(smsInput, smsKey);
            return true;
          }

          if (parent.__tempestAlertStorageTimer) {
            parent.clearInterval(parent.__tempestAlertStorageTimer);
          }
          let tries = 0;
          parent.__tempestAlertStorageTimer = parent.setInterval(() => {
            tries += 1;
            if (init() || tries > 12) {
              parent.clearInterval(parent.__tempestAlertStorageTimer);
              parent.__tempestAlertStorageTimer = null;
            }
          }, 400);
        })();
        </script>
        """,
        height=0,
    )


def alert_overrides_from_session():
    return {
        "alert_email_to": st.session_state.get("alert_email_to", "").strip(),
        "alert_sms_to": st.session_state.get("alert_sms_to", "").strip(),
        "smtp_username": st.session_state.get("smtp_username", "").strip(),
        "smtp_password": st.session_state.get("smtp_password", ""),
        "smtp_from": st.session_state.get("smtp_from", "").strip(),
    }


def send_test_alerts(temp_f, when_local):
    overrides = alert_overrides_from_session()
    email_to, sms_to = resolve_alert_recipients(DB_PATH, overrides=overrides)
    title = "Freeze Warning (Test)"
    message_body = build_freeze_alert_message(title, temp_f, when_local)
    subject = f"{title} - Tempest {temp_f:.1f} F"
    email_sent, email_error = send_email(
        subject,
        message_body,
        to_address=email_to,
        overrides=overrides,
        return_error=True,
    )
    sms_sent, sms_error = send_verizon_sms(
        message_body,
        sms_number=sms_to,
        overrides=overrides,
        return_error=True,
    )
    return email_sent, sms_sent, email_error, sms_error


def build_freeze_banner(temp_f, when_local):
    if temp_f is None or pd.isna(temp_f):
        return ""
    temp_f = float(temp_f)
    time_text = fmt_time(when_local)
    if temp_f <= DEEP_FREEZE_F:
        title = "Deep Freeze Advisory"
        level_class = "deep-freeze"
    elif temp_f <= FREEZE_WARNING_F:
        title = "Freeze Warning"
        level_class = "freeze"
    else:
        return ""
    detail = f"Tempest {temp_f:.1f} F at {time_text}"
    return (
        f"<div class=\"alert-banner {level_class}\">"
        f"<span class=\"alert-title\">{html_escape(title)}</span>"
        f"<span class=\"alert-meta\">{html_escape(detail)}</span>"
        "</div>"
    )


def collector_row_class(status, error_recent=False):
    if error_recent:
        return "ingest-detail-row alert"
    if status in ("warn", "offline"):
        return f"ingest-detail-row {status}"
    return "ingest-detail-row"


def fmt_duration(seconds):
    if seconds is None:
        return "--"
    minutes = max(0, int(round(seconds / 60)))
    hours, mins = divmod(minutes, 60)
    return f"{hours}h {mins:02d}m"


@st.cache_data(ttl=3600)
def fetch_sun_times(lat, lon, date_str):
    try:
        resp = requests.get(
            "https://api.sunrise-sunset.org/json",
            params={"lat": lat, "lng": lon, "date": date_str, "formatted": 0},
            timeout=6,
        )
        resp.raise_for_status()
        payload = resp.json()
        if not isinstance(payload, dict):
            return None
        return payload.get("results")
    except Exception:
        return None


@st.cache_data(ttl=3600)
def fetch_station_location(token, station_id):
    try:
        resp = requests.get(
            "https://swd.weatherflow.com/swd/rest/stations",
            params={"token": token},
            timeout=6,
        )
        resp.raise_for_status()
        payload = resp.json()
        stations = payload.get("stations", []) if isinstance(payload, dict) else []
        for station in stations:
            if station.get("station_id") == station_id:
                lat = station.get("latitude") or station.get("station_latitude")
                lon = station.get("longitude") or station.get("station_longitude") or station.get("lng")
                return {
                    "name": station.get("name") or "Tempest Station",
                    "lat": lat,
                    "lon": lon,
                }
        if stations:
            station = stations[0]
            lat = station.get("latitude") or station.get("station_latitude")
            lon = station.get("longitude") or station.get("station_longitude") or station.get("lng")
            return {
                "name": station.get("name") or "Tempest Station",
                "lat": lat,
                "lon": lon,
            }
    except Exception:
        return None
    return None


def fmt_bytes(size_bytes):
    if size_bytes is None:
        return "--"
    size = float(size_bytes)
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size < 1024 or unit == "TB":
            return f"{size:.1f} {unit}" if unit != "B" else f"{size:.0f} {unit}"
        size /= 1024


@st.cache_data(ttl=300)
def get_storage_stats():
    stats = {
        "db_size": 0,
        "assets_size": 0,
        "total_rows": 0,
        "measurements": 0,
    }
    try:
        db_path = Path(DB_PATH)
        if db_path.exists():
            stats["db_size"] = db_path.stat().st_size
    except Exception:
        pass

    total_assets = 0
    for root in [Path("images"), Path("static")]:
        try:
            if root.is_file():
                total_assets += root.stat().st_size
            elif root.exists():
                for file in root.rglob("*"):
                    if file.is_file():
                        total_assets += file.stat().st_size
        except Exception:
            continue
    stats["assets_size"] = total_assets

    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(1) FROM obs_st")
        stats["total_rows"] += int(cur.fetchone()[0])
        if AIRLINK_TABLE:
            cur.execute(f"SELECT COUNT(1) FROM {AIRLINK_TABLE}")
            stats["total_rows"] += int(cur.fetchone()[0])
        def measurement_cols(table, exclude):
            cur.execute(f"PRAGMA table_info({table})")
            cols = [row[1] for row in cur.fetchall()]
            return [c for c in cols if c not in exclude]
        obs_exclude = {"obs_epoch", "device_id", "obs_raw_json"}
        air_exclude = {"did", "ts", "lsid", "data_structure_type", "last_report_time"}
        measurements = set(measurement_cols("obs_st", obs_exclude))
        if AIRLINK_TABLE:
            measurements |= set(measurement_cols(AIRLINK_TABLE, air_exclude))
        stats["measurements"] = len(measurements)
        conn.close()
    except Exception:
        pass

    return stats


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


def compute_wind_chill(temp_f, wind_mph):
    """Wind chill per NOAA; returns the input temp when outside formula bounds."""
    if temp_f is None or wind_mph is None:
        return None
    if pd.isna(temp_f) or pd.isna(wind_mph):
        return None
    if temp_f > 50 or wind_mph < 3:
        return temp_f
    w_pow = wind_mph ** 0.16
    return 35.74 + (0.6215 * temp_f) - 35.75 * w_pow + 0.4275 * temp_f * w_pow


def ensure_daily_briefs_table(conn: sqlite3.Connection):
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS daily_briefs (
            date TEXT PRIMARY KEY,
            generated_at TEXT,
            tz TEXT,
            headline TEXT,
            bullets_json TEXT,
            tomorrow_text TEXT,
            model TEXT,
            version TEXT
        )
        """
    )
    conn.commit()


def load_daily_briefs(conn: sqlite3.Connection):
    ensure_daily_briefs_table(conn)
    rows = conn.execute(
        "SELECT date, generated_at, headline, bullets_json, tomorrow_text FROM daily_briefs ORDER BY date DESC LIMIT 2"
    ).fetchall()
    briefs = []
    for row in rows:
        try:
            bullets = json.loads(row[3]) if row[3] else []
        except Exception:
            bullets = []
        briefs.append(
            {
                "date": row[0],
                "generated_at": row[1],
                "headline": row[2],
                "bullets": bullets,
                "tomorrow": row[4],
            }
        )
    return briefs


@st.cache_data(ttl=FORECAST_REFRESH_MINUTES * 60)
def fetch_tempest_forecast(token=None, station_id=None, lat=None, lon=None, api_key=None):
    """
    Fetch Tempest better_forecast.
    Accepts either token (preferred) or api_key. If override lat/lon provided, try them first; fallback to station_id.
    """
    if not token and not api_key:
        return None, "TEMPEST_API_TOKEN or TEMPEST_API_KEY not set"
    base_url = "https://swd.weatherflow.com/swd/rest/better_forecast"
    units = {
        "units_temp": "f",
        "units_wind": "mph",
        "units_pressure": "inhg",
        "units_precip": "in",
        "units_distance": "mi",
    }
    params = {
        "station_id": station_id,
        **units,
    }
    if token:
        params["token"] = token
    if api_key and not token:
        params["api_key"] = api_key
    if lat is not None and lon is not None:
        params["lat"] = lat
        params["lon"] = lon
    try:
        resp = requests.get(base_url, params=params, timeout=10, headers={"accept": "application/json"})
        resp.raise_for_status()
        payload = resp.json()
        status = payload.get("status") or {}
        if status.get("status_code") not in (0, None):
            if "lat" in params:
                params.pop("lat", None)
                params.pop("lon", None)
                resp = requests.get(base_url, params=params, timeout=10, headers={"accept": "application/json"})
                resp.raise_for_status()
                payload = resp.json()
                status = payload.get("status") or {}
                if status.get("status_code") not in (0, None):
                    return None, status.get("status_message") or "API returned error"
            else:
                return None, status.get("status_message") or "API returned error"
        return payload, status.get("status_message") or "OK"
    except Exception:
        return None, "Request failed"


@st.cache_data(ttl=FORECAST_REFRESH_MINUTES * 60)
def fetch_openmeteo_forecast(lat, lon, tz_name):
    """Fetch forecast from Open-Meteo (no key required)."""
    if lat is None or lon is None:
        return None, None, "lat/lon missing"
    hourly_fields = [
        "temperature_2m",
        "apparent_temperature",
        "precipitation_probability",
        "precipitation",
        "pressure_msl",
        "relativehumidity_2m",
        "windspeed_10m",
        "windgusts_10m",
    ]
    daily_fields = [
        "temperature_2m_max",
        "temperature_2m_min",
        "precipitation_probability_max",
        "sunrise",
        "sunset",
    ]
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": ",".join(hourly_fields),
        "daily": ",".join(daily_fields),
        "timezone": tz_name or "auto",
        "forecast_days": 8,
    }
    params.update(
        {
            "windspeed_unit": "mph",
            "temperature_unit": "fahrenheit",
            "precipitation_unit": "inch",
            "timeformat": "iso8601",
        }
    )
    try:
        resp = requests.get("https://api.open-meteo.com/v1/forecast", params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
    except Exception:
        return None, None, "Open-Meteo request failed"

    hourly_raw = data.get("hourly")
    daily_raw = data.get("daily")
    hourly_df = pd.DataFrame(hourly_raw) if isinstance(hourly_raw, dict) else None
    daily_df = pd.DataFrame(daily_raw) if isinstance(daily_raw, dict) else None

    if hourly_df is not None and not hourly_df.empty:
        hourly_df["time"] = pd.to_datetime(hourly_df["time"], utc=True)
        try:
            hourly_df["time"] = hourly_df["time"].dt.tz_convert(tz_name)
        except Exception:
            hourly_df["time"] = hourly_df["time"].dt.tz_convert("UTC")
        hourly_df.rename(
            columns={
                "temperature_2m": "air_temperature",
                "apparent_temperature": "feels_like",
                "precipitation_probability": "precip_probability",
                "pressure_msl": "pressure",
                "relativehumidity_2m": "relative_humidity",
                "windspeed_10m": "wind_avg",
                "windgusts_10m": "wind_gust",
            },
            inplace=True,
        )
        # Open-Meteo pressure is hPa; convert to inHg for the UI.
        hourly_df["pressure"] = hourly_df["pressure"].apply(hpa_to_inhg)
        hourly_df["precip_probability"] = hourly_df["precip_probability"].fillna(0)
    else:
        hourly_df = None

    if daily_df is not None and not daily_df.empty:
        daily_df["day_start_local"] = pd.to_datetime(daily_df["time"], utc=True)
        daily_df["sunrise"] = pd.to_datetime(daily_df["sunrise"], utc=True)
        daily_df["sunset"] = pd.to_datetime(daily_df["sunset"], utc=True)
        try:
            daily_df["day_start_local"] = daily_df["day_start_local"].dt.tz_convert(tz_name)
            daily_df["sunrise"] = daily_df["sunrise"].dt.tz_convert(tz_name)
            daily_df["sunset"] = daily_df["sunset"].dt.tz_convert(tz_name)
        except Exception:
            daily_df["day_start_local"] = daily_df["day_start_local"].dt.tz_convert("UTC")
            daily_df["sunrise"] = daily_df["sunrise"].dt.tz_convert("UTC")
            daily_df["sunset"] = daily_df["sunset"].dt.tz_convert("UTC")
        daily_df.rename(
            columns={
                "temperature_2m_max": "air_temp_high",
                "temperature_2m_min": "air_temp_low",
                "precipitation_probability_max": "precip_probability",
            },
            inplace=True,
        )
    else:
        daily_df = None

    return hourly_df, daily_df, "OK"


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
    # EPA guidance: truncate/round PM2.5 to 0.1 for AQI calculations.
    pm = round(pm, 1)
    for c_low, c_high, a_low, a_high in breakpoints:
        if c_low <= pm <= c_high:
            return (a_high - a_low) / (c_high - c_low) * (pm - c_low) + a_low
    return 500.0


def backfill_aqi_columns():
    """Ensure AQI columns exist and are populated using corrected rounding."""
    if not AIRLINK_TABLE:
        return 0
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({AIRLINK_TABLE})")
    cols = {row[1] for row in cur.fetchall()}
    desired = ["aqi_pm25", "aqi_pm25_last_1_hour", "aqi_pm25_nowcast"]
    missing = [c for c in desired if c not in cols]
    for col in missing:
        cur.execute(f"ALTER TABLE {AIRLINK_TABLE} ADD COLUMN {col} REAL")
    conn.commit()

    where_clause = " OR ".join([f"{c} IS NULL" for c in desired])
    query = f"""
SELECT rowid, pm_2p5, pm_2p5_last_1_hour, pm_2p5_nowcast
FROM {AIRLINK_TABLE}
WHERE {where_clause}
"""
    df = pd.read_sql_query(query, conn)
    if not df.empty:
        df["aqi_pm25"] = df["pm_2p5"].apply(compute_pm25_aqi)
        df["aqi_pm25_last_1_hour"] = df["pm_2p5_last_1_hour"].apply(compute_pm25_aqi)
        df["aqi_pm25_nowcast"] = df["pm_2p5_nowcast"].apply(compute_pm25_aqi)
        rows = list(
            zip(
                df["aqi_pm25"],
                df["aqi_pm25_last_1_hour"],
                df["aqi_pm25_nowcast"],
                df["rowid"],
            )
        )
        cur.executemany(
            f"UPDATE {AIRLINK_TABLE} SET aqi_pm25=?, aqi_pm25_last_1_hour=?, aqi_pm25_nowcast=? WHERE rowid=?",
            rows,
        )
        conn.commit()
    conn.close()
    return len(df)


if "aqi_backfill_done" not in st.session_state:
    try:
        backfill_aqi_columns()
    except Exception:
        pass
    st.session_state.aqi_backfill_done = True


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


def aqi_badge_label(aqi):
    category = aqi_category(aqi)
    return {
        "Unhealthy for Sensitive Groups": "Sensitive",
        "Very Unhealthy": "Very",
        "Hazardous": "Hazard",
    }.get(category, category)


def aqi_color(aqi):
    if aqi is None or pd.isna(aqi):
        return "#2d2f36"
    if aqi <= 50:
        return "#00e400"
    if aqi <= 100:
        return "#ffff00"
    if aqi <= 150:
        return "#ff7e00"
    if aqi <= 200:
        return "#ff0000"
    if aqi <= 300:
        return "#8f3f97"
    return "#7e0023"


def latest_ts_str(ts_epoch):
    if pd.isna(ts_epoch):
        return "--"
    dt = epoch_to_dt(pd.Series([ts_epoch])).iloc[0]
    return dt.strftime("%Y-%m-%d %I:%M %p")


def short_text(text, max_len=90):
    if not text or pd.isna(text):
        return ""
    text = str(text)
    if len(text) <= max_len:
        return text
    return f"{text[:max_len - 3]}..."


def normalize_error_message(text):
    if not text or pd.isna(text):
        return ""
    msg = str(text).strip()
    if msg.startswith("OperationalError(") and msg.endswith(")"):
        msg = msg[len("OperationalError("):-1].strip("'\"")
    if "database is locked" in msg.lower():
        msg = "DB busy (locked)"
    return short_text(msg, 80)


def read_watchdog_status():
    if not WATCHDOG_LOG_PATH.exists():
        return None
    try:
        with WATCHDOG_LOG_PATH.open("rb") as handle:
            handle.seek(0, os.SEEK_END)
            size = handle.tell()
            handle.seek(max(0, size - 4096))
            chunk = handle.read().decode("utf-8", errors="ignore")
        lines = [line for line in chunk.splitlines() if "|" in line]
        if not lines:
            return None
        last_line = lines[-1]
        ts_part, rest = last_line.split("|", 1)
        ts = datetime.strptime(ts_part.strip(), "%Y-%m-%d %H:%M:%S")
        age_seconds = max(0, int((datetime.now() - ts).total_seconds()))
        age_text = format_latency(age_seconds)
        detail = rest.strip()
        status = "ok"
        pill_text = "OK"
        if "WARN:" in detail:
            status = "warn"
            pill_text = "WARN"
        if age_seconds > WATCHDOG_STALE_SECONDS:
            status = "warn"
            pill_text = "STALE"
        return {
            "name": "Watchdog",
            "status": status,
            "latency_text": f"Last run: {age_text}",
            "error_text": short_text(detail, 120),
            "colors": WATCHDOG_COLORS,
            "snapshot_text": f"Watchdog: {pill_text} ({age_text})",
            "error_recent": status == "warn",
            "pill_text": pill_text,
            "meta_text": f"Last run {age_text}",
            "meta_title": f"Stale after {WATCHDOG_STALE_SECONDS // 60}m",
        }
    except Exception:
        return None


def clean_chart(data, height=240, title=None):
    """Shared line chart without hover overlays for better performance."""
    y_scale = None
    if isinstance(data, pd.DataFrame) and not data.empty and "value" in data:
        metrics = set(str(m).lower() for m in data["metric"].unique()) if "metric" in data else set()
        is_pressure_only = len(metrics) == 1 and any("pressure" in m for m in metrics)
        if is_pressure_only:
            values = pd.to_numeric(data["value"], errors="coerce").dropna()
            if not values.empty:
                v_min, v_max = values.min(), values.max()
                span = max(v_max - v_min, 0.01)
                pad = max(0.02, span * 0.25)
                y_scale = alt.Scale(domain=[v_min - pad, v_max + pad])

    chart = (
        alt.Chart(data)
        .mark_line(interpolate="monotone", strokeWidth=2)
        .encode(
            x=alt.X("time:T", title="Time"),
            y=alt.Y("value:Q", title=None, scale=y_scale) if y_scale else alt.Y("value:Q", title=None),
            color=alt.Color("metric:N", legend=alt.Legend(title=None), scale=alt.Scale(scheme=CHART_SCHEME)),
        )
        .properties(height=height)
    )
    if title:
        chart = chart.properties(title=title)

    need_aqi_bands = (
        isinstance(data, pd.DataFrame)
        and not data.empty
        and "metric" in data
        and any("AQI" in str(m) for m in data["metric"].unique())
    )
    if need_aqi_bands:
        band_df = pd.DataFrame(
            [
                {"y0": 0, "y1": 50, "label": "Good"},
                {"y0": 51, "y1": 100, "label": "Moderate"},
                {"y0": 101, "y1": 150, "label": "Unhealthy (SG)"},
                {"y0": 151, "y1": 200, "label": "Unhealthy"},
                {"y0": 201, "y1": 300, "label": "Very Unhealthy"},
                {"y0": 301, "y1": 500, "label": "Hazardous"},
            ]
        )
        band_colors = ["#67e777", "#ffd75e", "#ffb347", "#ff7b7b", "#c065ff", "#803400"]
        bands = (
            alt.Chart(band_df)
            .mark_rect(opacity=0.08)
            .encode(
                y="y0:Q",
                y2="y1:Q",
                color=alt.Color(
                    "label:N",
                    legend=alt.Legend(title="AQI Zones"),
                    scale=alt.Scale(domain=band_df["label"].tolist(), range=band_colors),
                ),
            )
        )
        chart = alt.layer(bands, chart).resolve_scale(color="independent")

    return (
        chart
        .configure_axis(labelColor=CHART_LABEL_COLOR, titleColor=CHART_LABEL_COLOR, gridColor=CHART_GRID_COLOR)
        .configure_legend(labelColor=CHART_LABEL_COLOR, titleColor=CHART_LABEL_COLOR)
        .configure_title(color=CHART_TITLE_COLOR)
    )


def forecast_hourly_chart(hourly_df: pd.DataFrame | None):
    if hourly_df is None or hourly_df.empty:
        return None
    hours = hourly_df.copy().head(24)
    temp_cols = []
    if "air_temperature" in hours:
        temp_cols.append(("air_temperature", "Air Temp"))
    if "feels_like" in hours:
        temp_cols.append(("feels_like", "Feels Like"))
    if not temp_cols:
        return None
    temp_df = hours[["time"] + [c[0] for c in temp_cols]].melt(
        id_vars=["time"],
        var_name="metric_raw",
        value_name="value",
    )
    label_map = {raw: label for raw, label in temp_cols}
    temp_df["metric"] = temp_df["metric_raw"].map(label_map)
    temp_df.drop(columns=["metric_raw"], inplace=True)
    color_domain = [label for _, label in temp_cols]
    color_range = [THEME_COLORS["accent"], THEME_COLORS["accent3"]][: len(color_domain)]

    lines = (
        alt.Chart(temp_df)
        .mark_line(interpolate="monotone", strokeWidth=2.2)
        .encode(
            x=alt.X("time:T", title="Time"),
            y=alt.Y("value:Q", title="Temp (F)"),
            color=alt.Color("metric:N", legend=alt.Legend(title=None), scale=alt.Scale(domain=color_domain, range=color_range)),
        )
    )

    precip = None
    if "precip_probability" in hours:
        precip = (
            alt.Chart(hours)
            .mark_bar(opacity=0.25, color=THEME_COLORS["accent2"])
            .encode(
                x=alt.X("time:T", title=""),
                y=alt.Y("precip_probability:Q", title="Precip %", scale=alt.Scale(domain=[0, 100])),
            )
        )

    chart = lines
    if precip is not None:
        chart = alt.layer(precip, lines).resolve_scale(y="independent")

    return (
        chart.properties(height=260)
        .configure_axis(labelColor=CHART_LABEL_COLOR, titleColor=CHART_LABEL_COLOR, gridColor=CHART_GRID_COLOR)
        .configure_legend(labelColor=CHART_LABEL_COLOR, titleColor=CHART_LABEL_COLOR)
    )


def render_daily_outlook(daily_df: pd.DataFrame | None, max_days: int = 5):
    if daily_df is None or daily_df.empty:
        return
    days = daily_df.head(max_days)
    cols = st.columns(len(days))
    for col, (_, row) in zip(cols, days.iterrows()):
        date_val = row.get("day_start_local")
        date_label = date_val.strftime("%a %b %d") if isinstance(date_val, (pd.Timestamp, datetime)) else "Day"
        cond = row.get("conditions", "--")
        high = row.get("air_temp_high")
        low = row.get("air_temp_low")
        precip_prob = row.get("precip_probability")
        sunrise = row.get("sunrise")
        sunset = row.get("sunset")
        precip_text = f"{precip_prob:.0f}%" if precip_prob is not None and not pd.isna(precip_prob) else "--"
        high_text = "--" if high is None or pd.isna(high) else f"{high:.0f}F"
        low_text = "--" if low is None or pd.isna(low) else f"{low:.0f}F"
        sunrise_text = fmt_time(sunrise) if sunrise is not None else "--"
        sunset_text = fmt_time(sunset) if sunset is not None else "--"
        col.markdown(
            f"""
            <div class="daily-card">
              <div class="daily-date">{date_label}</div>
              <div class="daily-cond">{html_escape(cond)}</div>
              <div class="daily-temps"><span class="hi">High {high_text}</span> - <span class="lo">Low {low_text}</span></div>
              <div class="daily-precip">Precip {precip_text}</div>
              <div class="daily-sun">Sunrise {sunrise_text} - Sunset {sunset_text}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )


def bar_chart(data, height=200, title=None, color=None):
    if color is None:
        color = THEME_COLORS["accent2"]
    chart = (
        alt.Chart(data)
        .mark_bar(color=color)
        .encode(
            x=alt.X("label:N", title=None, sort=None),
            y=alt.Y("value:Q", title=None),
        )
        .properties(height=height)
        .configure_axis(labelColor=CHART_LABEL_COLOR, titleColor=CHART_LABEL_COLOR, gridColor=CHART_GRID_COLOR)
        .configure_title(color=CHART_TITLE_COLOR)
    )
    if title:
        chart = chart.properties(title=title)
    return chart


def sidebar_gauge(container, label, value, min_val, max_val, unit="", precision=1, color=None, highlight=False, meta_text=None):
    """Render a horizontal gauge in the sidebar container."""
    if color is None:
        color = THEME_COLORS["accent2"]
    clean_value = None if value is None or pd.isna(value) else float(value)
    if clean_value is None or max_val == min_val:
        pct = 0
        display_value = "--"
    else:
        pct = (clean_value - min_val) / (max_val - min_val)
        pct = max(0.0, min(1.0, pct))
        display_value = f"{clean_value:.{precision}f}{unit}"

    pulse_class = "gauge-pulse" if highlight else ""
    meta_html = f"<span class='gauge-category'>{meta_text}</span>" if meta_text else ""
    container.markdown(
        f"""
        <div class="gauge-block {pulse_class}">
            <div class="gauge-header">
                <span>{label}{meta_html}</span>
                <span>{display_value}</span>
            </div>
            <div class="gauge-track">
                <div class="gauge-fill" style="width:{pct*100:.0f}%; background: {color};"></div>
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
    airlink_temp = airlink_latest.get("temp_f") if airlink_latest is not None else None
    if tempest_latest is not None:
        sidebar_gauge(container, "Tempest Temp (F)", tempest_latest.air_temperature_f, -10, 110, precision=1, color=GAUGE_COLORS["temp"], highlight=highlights.get("temp"))
        if airlink_temp is not None and not pd.isna(airlink_temp):
            sidebar_gauge(container, "AirLink Temp (F)", airlink_temp, -10, 110, precision=1, color=GAUGE_COLORS["air_temp"])
        heat_index = tempest_latest.get("heat_index_f")
        if heat_index is not None and not pd.isna(heat_index):
            sidebar_gauge(container, "Feels Like (F)", heat_index, -10, 110, precision=1, color=GAUGE_COLORS["feels"], highlight=highlights.get("temp"))
        wind_chill = compute_wind_chill(tempest_latest.air_temperature_f, tempest_latest.wind_speed_mph)
        if wind_chill is not None:
            sidebar_gauge(container, "Wind Chill (F)", wind_chill, -40, 80, precision=1, color=GAUGE_COLORS["wind"], highlight=highlights.get("wind"))
        sidebar_gauge(container, "Humidity (%)", tempest_latest.relative_humidity, 0, 100, precision=0, color=GAUGE_COLORS["hum"], highlight=highlights.get("hum"))
        sidebar_gauge(container, "Pressure (inHg)", tempest_latest.pressure_inhg, 28, 32, precision=2, color=GAUGE_COLORS["pressure"], highlight=highlights.get("pressure"))
        sidebar_gauge(container, "Wind Avg (mph)", tempest_latest.wind_speed_mph, 0, 40, precision=1, color=GAUGE_COLORS["wind"], highlight=highlights.get("wind"))
        gust_value = tempest_latest.get("wind_gust_mph")
        if gust_value is not None and not pd.isna(gust_value):
            sidebar_gauge(container, "Wind Gust (mph)", gust_value, 0, 60, precision=1, color=GAUGE_COLORS["gust"], highlight=highlights.get("wind"))
    if airlink_latest is not None:
        aqi_value = None if pd.isna(airlink_latest.aqi_pm25) else airlink_latest.aqi_pm25
        aqi_col = aqi_color(aqi_value)
        aqi_category_text = aqi_category(aqi_value)
        sidebar_gauge(
            container,
            "AQI PM2.5",
            aqi_value,
            0,
            300,
            precision=0,
            color=aqi_col,
            highlight=highlights.get("aqi"),
            meta_text=aqi_category_text if aqi_category_text != "--" else None,
        )


def render_sidebar_clock(container):
    """Render a live clock styled like the live gauges."""
    try:
        now_local = pd.Timestamp.now(tz="UTC").tz_convert(LOCAL_TZ)
        time_text = now_local.strftime("%I:%M:%S %p").lstrip("0")
        date_text = now_local.strftime("%a, %b %d")
        fill_pct = int((now_local.second / 60) * 100)
    except Exception:
        now_local = datetime.now()
        time_text = now_local.strftime("%I:%M:%S %p").lstrip("0")
        date_text = now_local.strftime("%a, %b %d")
        fill_pct = 0
    container.markdown(
        f"""
        <div class="gauge-block gauge-clock">
            <div class="gauge-header">
                <span>Local Time</span>
                <span class="clock-time" data-clock-time>{time_text}</span>
            </div>
            <div class="gauge-muted clock-date" data-clock-date>{date_text}</div>
            <div class="gauge-track">
                <div class="gauge-fill" data-clock-fill style="width:{fill_pct}%;"></div>
            </div>
        </div>
        """,
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


def ingestion_status(label, last_epoch, cadence_seconds, now_ts, max_lag_seconds=900, color=None):
    """Compatibility helper for older ingestion bar usage."""
    if color is None:
        color = THEME_COLORS["accent2"]
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


def seconds_since_epoch(epoch, now_ts):
    if epoch is None or pd.isna(epoch):
        return None
    return (now_ts - pd.to_datetime(epoch, unit="s", utc=True)).total_seconds()


def build_collector_statuses(now_ts):
    if not HEARTBEAT_TABLE:
        return []
    hb_df = load_df(
        f"""
        SELECT name, last_ok_epoch, last_error_epoch, last_ok_message, last_error
        FROM {HEARTBEAT_TABLE}
        """
    )
    if hb_df.empty:
        return []

    status_label_map = {
        "ok": "Live",
        "warn": "Delayed",
        "offline": "Offline",
        "standby": "Standby",
    }
    statuses = []
    for _, row in hb_df.iterrows():
        name_key = row["name"]
        label = COLLECTOR_LABELS.get(name_key, name_key)
        stale_seconds = COLLECTOR_STALE_SECONDS.get(name_key, 300)
        colors = COLLECTOR_COLORS.get(
            name_key,
            (THEME_COLORS["text_secondary"], THEME_COLORS["text_muted"]),
        )

        ok_age = seconds_since_epoch(row["last_ok_epoch"], now_ts)
        if ok_age is None:
            status = "offline"
            ok_text = "Last ok: --"
        else:
            if ok_age <= stale_seconds:
                status = "ok"
            elif ok_age <= stale_seconds * 3:
                status = "warn"
            else:
                status = "offline"
            ok_text = f"Last ok: {format_latency(ok_age)}"

        ok_msg = row["last_ok_message"]
        if ok_msg and not pd.isna(ok_msg):
            ok_text = f"{ok_text} - {short_text(ok_msg, 48)}"

        err_age = seconds_since_epoch(row["last_error_epoch"], now_ts)
        err_text = "Last error: --"
        error_recent = False
        if err_age is not None:
            ok_after_error = (
                row["last_ok_epoch"] is not None
                and row["last_error_epoch"] is not None
                and row["last_ok_epoch"] > row["last_error_epoch"]
            )
            error_recent = err_age <= COLLECTOR_ERROR_GRACE_SECONDS
            err_text = f"Last error: {format_latency(err_age)}"
            err_msg = normalize_error_message(row["last_error"])
            if err_msg and (error_recent or not ok_after_error):
                err_text = f"{err_text} - {err_msg}"
            elif ok_after_error:
                err_text = f"{err_text} (resolved)"

        ok_age_text = format_latency(ok_age) if ok_age is not None else "--"
        snapshot_text = f"{label}: {status_label_map.get(status, 'Live')} ({ok_age_text})"
        statuses.append(
            {
                "name": label,
                "status": status,
                "latency_text": ok_text,
                "error_text": err_text,
                "error_recent": error_recent,
                "colors": colors,
                "snapshot_text": snapshot_text,
            }
        )

    return statuses

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


def render_ingest_banner(
    sources,
    total_recent,
    avg_latency_text=None,
    collector_statuses=None,
    target=None,
    include_diagnostics=False,
    title="Station health",
):
    if not sources:
        return

    target = target or st

    if "ping_results" not in st.session_state:
        st.session_state.ping_results = {}
    if "app_started_at" not in st.session_state:
        st.session_state.app_started_at = time.time()

    by_name = {s["name"]: s for s in sources}
    station_recent = by_name.get("Tempest Station", {}).get("recent_count", 0) or 0

    def source_status(src):
        latency = src["latency_minutes"]
        if latency is None:
            return "offline"
        if src["name"] == "Tempest Hub":
            if station_recent > 0 and src["recent_count"] <= 0:
                return "standby"
            return "ok" if latency <= 60 else "warn"
        if src["recent_count"] <= 0:
            return "offline"
        return "ok" if latency <= 10 else "warn"

    status_labels = {
        "ok": "Live",
        "warn": "Delayed",
        "offline": "Offline",
        "standby": "Standby",
    }

    def indicator_chip(name, status, colors, meta=None, pill_text=None, meta_title=None):
        pill = pill_text or status_labels.get(status, "Live")
        meta_attr = f" title=\"{html_escape(meta_title)}\"" if meta and meta_title else ""
        meta_html = f"<div class=\"ingest-meta\"{meta_attr}>{html_escape(meta)}</div>" if meta else ""
        return f"""<div class="ingest-chip {status}">
  <span class="ingest-dot" style="background:{colors[0]};"></span>
  <div class="ingest-body">
    <div class="ingest-title">{html_escape(name)}</div>
    {meta_html}
  </div>
  <span class="ingest-pill {status}">{html_escape(pill)}</span>
</div>"""

    collector_statuses = collector_statuses or []
    watchdog_status = read_watchdog_status()
    if watchdog_status:
        collector_statuses = collector_statuses + [watchdog_status]

    statuses = [
        {
            "name": s["name"],
            "status": source_status(s),
            "colors": s["colors"],
            "latency_text": s["latency_text"],
            "load_text": s["load_text"],
            "last_seen": s["last_seen"],
        }
        for s in sources
    ]

    def needs_attention(item):
        if item["name"] == "Tempest Hub":
            return item["status"] == "offline"
        return item["status"] in ("warn", "offline")

    overall = "warn" if any(needs_attention(s) for s in statuses) else "ok"
    header_title = "Signals steady" if overall == "ok" else "Signals monitoring"
    badge_text = f"{total_recent} events/hr" if total_recent else "No recent events"
    if avg_latency_text:
        badge_text = f"{badge_text} - avg data age {avg_latency_text}"
    events_html = (
        f"<div class=\"ingest-help ingest-events\">{html_escape(badge_text)} "
        "<span class=\"ingest-info\" title='Avg data age reflects how long ago each source last reported.'>(i)</span></div>"
    )

    summary_html = "".join(
        indicator_chip(s["name"], s["status"], s["colors"])
        for s in statuses
    )
    collector_section_html = ""
    if collector_statuses:
        collector_summary_html = "".join(
            indicator_chip(
                s["name"],
                s["status"],
                s["colors"],
                meta=s.get("meta_text"),
                meta_title=s.get("meta_title"),
                pill_text=s.get("pill_text"),
            )
            for s in collector_statuses
        )
        snapshot_items = [
            s["snapshot_text"]
            for s in collector_statuses
            if s.get("snapshot_text")
        ]
        snapshot_html = "<br>".join(html_escape(item) for item in snapshot_items)
        snapshot_line = (
            f"<div class=\"ingest-snapshot\">{snapshot_html}</div>"
            if snapshot_items
            else ""
        )
        collector_section_html = "\n".join(
            [
                '<div class="ingest-divider"></div>',
                '<div class="ingest-eyebrow">Collector health</div>',
                snapshot_line,
                '<div class="ingest-status-row">',
                collector_summary_html,
                "</div>",
            ]
        )

    if title:
        target.markdown(f"<div class='section-title'>{title}</div>", unsafe_allow_html=True)

    hub_uptime_text = fmt_duration(
        (time.time() - hub_activity["last_epoch"]) if hub_activity.get("last_epoch") else None
    )
    target.markdown(
        f"""
<div class="ingest-shell hero-glow">
  <div class="ingest-header-row">
    <div>
      <div class="ingest-eyebrow">Signals</div>
      <div class="ingest-summary">{header_title}</div>
      {events_html}
      <div class="ingest-help">Hub uptime: {hub_uptime_text}</div>
      <div class="ingest-status-row">
        {summary_html}
      </div>
      {collector_section_html}
    </div>
  </div>
</div>
        """,
        unsafe_allow_html=True,
    )

    if include_diagnostics:
        target.markdown("<div class='metric-expanders'>", unsafe_allow_html=True)

        def ping_device(host):
            if not host:
                return False, "Ping target not configured"
            try:
                result = subprocess.run(
                    ["ping", "-n", "1", "-w", "1000", host],
                    capture_output=True,
                    text=True,
                    timeout=3,
                )
                ok = result.returncode == 0
                return ok, "Reachable" if ok else "No response"
            except Exception:
                return False, "Ping failed"

        with target.expander("Diagnostics", expanded=False):
            st.caption("Ping checks local network reachability for each source.")
            has_targets = False
            for src in statuses:
                ping_target = PING_TARGETS.get(src["name"]) or (
                    PING_TARGETS.get("Tempest Hub") if src["name"] == "Tempest Station" else None
                )
                if ping_target:
                    has_targets = True
                disabled = not ping_target
                if st.button(f"Ping {src['name']}", key=f"ping_{src['name']}", disabled=disabled):
                    ok, msg = ping_device(ping_target)
                    st.session_state.ping_results[src["name"]] = (ok, msg, time.time())
                result = st.session_state.ping_results.get(src["name"])
                if result and time.time() - result[2] < 6:
                    ok, msg, _ = result
                    status_label = "OK" if ok else "WARN"
                    st.markdown(
                        f"<div class='ping-toast'>{status_label}: {src['name']} - {msg}</div>",
                        unsafe_allow_html=True,
                    )
            if not has_targets:
                st.caption("Set ping targets in PING_TARGETS to enable diagnostics.")
        target.markdown("</div>", unsafe_allow_html=True)


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


def series_from_df(df, time_col, value_col):
    if df is None or df.empty or time_col not in df or value_col not in df:
        return []
    series = []
    for _, row in df[[time_col, value_col]].dropna().iterrows():
        ts = row[time_col]
        if pd.isna(ts):
            continue
        series.append({"t": ts.isoformat(), "v": float(row[value_col])})
    return series


def build_overview_payload(tempest_df, airlink_df):
    payload = {}
    if tempest_df is not None and not tempest_df.empty:
        payload["temp"] = series_from_df(tempest_df, "time", "air_temperature_f")
        payload["heat"] = series_from_df(tempest_df, "time", "heat_index_f")
        payload["humidity"] = series_from_df(tempest_df, "time", "relative_humidity")
        payload["pressure"] = series_from_df(tempest_df, "time", "pressure_inhg")
        payload["wind"] = series_from_df(tempest_df, "time", "wind_speed_mph")
        payload["gust"] = series_from_df(tempest_df, "time", "wind_gust_mph")
        payload["rain"] = series_from_df(tempest_df, "time", "rain_mm")
    if airlink_df is not None and not airlink_df.empty:
        payload["aqi"] = series_from_df(airlink_df, "time", "aqi_pm25")
    return payload


def build_comparison_payload(tempest_df, airlink_df):
    payload = {"temp_today": [], "temp_yesterday": [], "aqi_wind": []}
    if tempest_df is not None and not tempest_df.empty:
        df = tempest_df.copy()
        df["date"] = df["time"].dt.date
        latest_date = df["date"].max()
        yesterday_date = latest_date - pd.Timedelta(days=1)
        today_df = df[df["date"] == latest_date]
        yesterday_df = df[df["date"] == yesterday_date]
        payload["temp_today"] = series_from_df(today_df, "time", "air_temperature_f")
        payload["temp_yesterday"] = series_from_df(yesterday_df, "time", "air_temperature_f")
        if airlink_df is not None and not airlink_df.empty:
            merged = pd.merge_asof(
                airlink_df.sort_values("time"),
                df.sort_values("time"),
                on="time",
                direction="nearest",
            )
            if "aqi_pm25" in merged and "wind_speed_mph" in merged:
                for _, row in merged[["aqi_pm25", "wind_speed_mph"]].dropna().iterrows():
                    payload["aqi_wind"].append(
                        {"x": float(row["wind_speed_mph"]), "y": float(row["aqi_pm25"])}
                    )
    return payload


def build_raw_table(tempest_df, airlink_df, limit=120):
    if tempest_df is None or tempest_df.empty:
        return []
    df = tempest_df.copy()
    if airlink_df is not None and not airlink_df.empty:
        df = pd.merge_asof(
            df.sort_values("time"),
            airlink_df.sort_values("time")[["time", "aqi_pm25"]],
            on="time",
            direction="nearest",
        )
    cols = [
        "time",
        "air_temperature_f",
        "heat_index_f",
        "relative_humidity",
        "pressure_inhg",
        "wind_speed_mph",
        "wind_gust_mph",
        "rain_mm",
        "aqi_pm25",
    ]
    for col in cols:
        if col not in df:
            df[col] = pd.NA
    rows = []
    for _, row in df[cols].tail(limit).iterrows():
        rows.append(
            {
                "time": row["time"].strftime("%Y-%m-%d %H:%M"),
                "temp": None if pd.isna(row["air_temperature_f"]) else f"{row['air_temperature_f']:.1f}",
                "feels": None if pd.isna(row["heat_index_f"]) else f"{row['heat_index_f']:.1f}",
                "hum": None if pd.isna(row["relative_humidity"]) else f"{row['relative_humidity']:.0f}",
                "press": None if pd.isna(row["pressure_inhg"]) else f"{row['pressure_inhg']:.2f}",
                "wind": None if pd.isna(row["wind_speed_mph"]) else f"{row['wind_speed_mph']:.1f}",
                "gust": None if pd.isna(row["wind_gust_mph"]) else f"{row['wind_gust_mph']:.1f}",
                "rain": None if pd.isna(row["rain_mm"]) else f"{row['rain_mm']:.2f}",
                "aqi": None if pd.isna(row.get("aqi_pm25", None)) else f"{row['aqi_pm25']:.0f}",
            }
        )
    return rows


def fmt_value(value, fmt_str="{:.1f}", fallback="--"):
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return fallback
    try:
        return fmt_str.format(value)
    except Exception:
        return fallback


def compass_dir(deg):
    if deg is None or (isinstance(deg, float) and pd.isna(deg)):
        return "--"
    directions = ["N", "NE", "E", "SE", "S", "SW", "W", "NW"]
    idx = int((float(deg) + 22.5) // 45) % 8
    return directions[idx]


def hex_to_rgba(color, alpha):
    color = color.lstrip("#")
    if len(color) != 6:
        return f"rgba(123,231,217,{alpha})"
    r = int(color[0:2], 16)
    g = int(color[2:4], 16)
    b = int(color[4:6], 16)
    return f"rgba({r},{g},{b},{alpha})"


def render_grid_dashboard(tab_id, tiles_html, data_payload, height=900):
    grid_id = f"{tab_id}-grid"
    data_json = json.dumps(data_payload)
    return f"""
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/gridstack@11.1.2/dist/gridstack.min.css" />
    <style>
      :root {{
        color-scheme: var(--color-scheme, dark);
      }}
      .dash-shell {{
        padding: 10px 6px 20px 6px;
        border-radius: 18px;
        background: radial-gradient(circle at 20% 10%, var(--accent-2-soft, rgba(97,165,255,0.16)), transparent 45%), var(--surface-3, #0c111a);
        border: 1px solid var(--border, #1c2434);
        box-shadow: inset 0 0 0 1px var(--border-muted, rgba(255,255,255,0.02));
      }}
      .grid-stack-item-content {{
        background: linear-gradient(160deg, var(--surface-2, rgba(22,30,44,0.92)), var(--surface-3, rgba(12,16,24,0.95)));
        border-radius: 16px;
        border: 1px solid var(--border-muted, rgba(110,140,190,0.18));
        box-shadow: 0 20px 40px rgba(0,0,0,0.35);
        color: var(--text-primary, #e8edf7);
        padding: 14px;
        overflow: hidden;
      }}
      .tile-title {{
        font-size: 0.78rem;
        text-transform: uppercase;
        letter-spacing: 0.12em;
        color: var(--text-muted, #8aa4c8);
        margin-bottom: 6px;
      }}
      .tile-value {{
        font-size: 1.9rem;
        font-weight: 700;
        color: var(--text-primary, #f4f7ff);
      }}
      .tile-sub {{
        font-size: 0.82rem;
        color: var(--text-secondary, #9fb2cc);
      }}
      .tile-meta {{
        display: flex;
        gap: 10px;
        flex-wrap: wrap;
        margin-top: 6px;
        font-size: 0.78rem;
        color: var(--text-secondary, #9fb2cc);
      }}
      .tile-pill {{
        padding: 2px 8px;
        border-radius: 999px;
        background: var(--accent-soft, rgba(123,231,217,0.12));
        border: 1px solid var(--accent-border, rgba(123,231,217,0.3));
        font-size: 0.72rem;
        color: var(--text-primary, #bfeee6);
      }}
      .tile-canvas {{
        width: 100%;
        height: 100%;
        display: block;
      }}
      .chart-wrap {{
        height: 100%;
      }}
      .dial-label {{
        font-size: 0.84rem;
        color: var(--text-secondary, #9fb2cc);
      }}
      .raw-table {{
        width: 100%;
        border-collapse: collapse;
        font-size: 0.78rem;
      }}
      .raw-table th, .raw-table td {{
        padding: 6px 8px;
        border-bottom: 1px solid var(--border-muted, rgba(255,255,255,0.06));
        text-align: left;
      }}
      .raw-table th {{
        color: var(--text-muted, #8aa4c8);
        text-transform: uppercase;
        letter-spacing: 0.08em;
        font-size: 0.7rem;
      }}
      .raw-scroll {{
        max-height: 460px;
        overflow: auto;
        border: 1px solid var(--border-muted, rgba(255,255,255,0.05));
        border-radius: 12px;
      }}
      .raw-scroll::-webkit-scrollbar,
      .grid-stack::-webkit-scrollbar,
      .grid-stack-item-content::-webkit-scrollbar {{
        display: none;
      }}
      .raw-scroll,
      .grid-stack,
      .grid-stack-item-content {{
        scrollbar-width: none;
      }}
      .grid-stack > .grid-stack-item > .grid-stack-item-content {{
        cursor: move;
      }}
    </style>
    <div class="dash-shell">
      <div class="grid-stack" id="{grid_id}">
        {tiles_html}
      </div>
    </div>
    <script src="https://cdn.jsdelivr.net/npm/gridstack@11.1.2/dist/gridstack-all.js"></script>
    <script>
      (function() {{
        const payload = {data_json};
        const grid = GridStack.init({{
          margin: 12,
          cellHeight: 64,
          float: true,
          resizable: {{ handles: 'e, se, s, sw, w' }},
        }}, document.getElementById("{grid_id}"));
        const rootStyles = getComputedStyle(document.documentElement);
        const cssVar = (name, fallback) => rootStyles.getPropertyValue(name).trim() || fallback;
        const chartText = cssVar("--chart-text", "#9fb2cc");
        const chartTitle = cssVar("--chart-title", "#f4f7ff");
        const chartGrid = cssVar("--chart-grid", "rgba(255,255,255,0.12)");
        const accent = cssVar("--accent-2", "#61a5ff");
        const accentAlt = cssVar("--accent", "#7be7d9");
        const accentWarm = cssVar("--accent-3", "#f2a85b");
        const accentSoft = cssVar("--accent-soft", "rgba(123,231,217,0.25)");
        const accent2Soft = cssVar("--accent-2-soft", "rgba(97,165,255,0.12)");

        function toSeries(series) {{
          return (series || []).map((d) => d.v);
        }}

        function formatTimeShort(value) {{
          if (!value) return "";
          const d = new Date(value);
          if (Number.isNaN(d.getTime())) return "";
          return d.toLocaleTimeString([], {{ hour: "2-digit", minute: "2-digit" }});
        }}

        function setupCanvas(canvas, minW, minH) {{
          const rect = canvas.getBoundingClientRect();
          const dpr = window.devicePixelRatio || 1;
          const width = Math.max(minW, rect.width);
          const height = Math.max(minH, rect.height);
          canvas.width = Math.round(width * dpr);
          canvas.height = Math.round(height * dpr);
          canvas.style.width = width + "px";
          canvas.style.height = height + "px";
          const ctx = canvas.getContext("2d");
          ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
          return {{ ctx, width, height }};
        }}

        function drawLine(canvasId, seriesList, colors) {{
          const canvas = document.getElementById(canvasId);
          if (!canvas) return;
          const {{ ctx, width, height }} = setupCanvas(canvas, 200, 140);
          ctx.clearRect(0, 0, width, height);
          const seriesData = seriesList.map(toSeries).filter((s) => s.length);
          if (!seriesData.length) return;
          const maxLen = Math.max(...seriesData.map((s) => s.length));
          const values = seriesData.flat();
          const minV = Math.min(...values);
          const maxV = Math.max(...values);
          const pad = 28;
          const w = width - pad * 2;
          const h = height - pad * 2;
          ctx.strokeStyle = chartGrid;
          ctx.lineWidth = 1;
          ctx.beginPath();
          ctx.moveTo(pad, pad);
          ctx.lineTo(pad, pad + h);
          ctx.lineTo(pad + w, pad + h);
          ctx.stroke();
          seriesData.forEach((series, idx) => {{
            const color = colors[idx] || accent;
            ctx.strokeStyle = color;
            ctx.lineWidth = 2;
            ctx.beginPath();
            series.forEach((val, i) => {{
              const x = pad + (w * (i / Math.max(1, maxLen - 1)));
              const y = pad + h - (h * (val - minV) / Math.max(1e-6, maxV - minV));
              if (i === 0) ctx.moveTo(x, y);
              else ctx.lineTo(x, y);
            }});
            ctx.stroke();
          }});
          ctx.fillStyle = chartText;
          ctx.font = "12px sans-serif";
          ctx.textAlign = "left";
          ctx.fillText(minV.toFixed(1), 6, pad + h);
          ctx.fillText(maxV.toFixed(1), 6, pad + 10);
          const baseSeries = seriesList[0] || [];
          if (baseSeries.length) {{
            const start = formatTimeShort(baseSeries[0].t);
            const end = formatTimeShort(baseSeries[baseSeries.length - 1].t);
            ctx.textAlign = "left";
            ctx.fillText(start, pad, height - 6);
            ctx.textAlign = "right";
            ctx.fillText(end, pad + w, height - 6);
          }}
        }}

        function drawDial(canvasId, value, minV, maxV, accentColor) {{
          const canvas = document.getElementById(canvasId);
          if (!canvas) return;
          const {{ ctx, width, height }} = setupCanvas(canvas, 200, 180);
          const cx = width / 2;
          const cy = height * 0.62;
          const radius = Math.min(width, height) * 0.42;
          ctx.clearRect(0, 0, width, height);
          ctx.strokeStyle = chartGrid;
          ctx.lineWidth = 10;
          ctx.beginPath();
          ctx.arc(cx, cy, radius, Math.PI, 0);
          ctx.stroke();
          const ratio = Math.max(0, Math.min(1, (value - minV) / (maxV - minV)));
          ctx.strokeStyle = accentColor || accent;
          ctx.beginPath();
          ctx.arc(cx, cy, radius, Math.PI, Math.PI + ratio * Math.PI);
          ctx.stroke();
          ctx.fillStyle = chartTitle;
          ctx.font = "700 28px sans-serif";
          ctx.textAlign = "center";
          ctx.fillText(value.toFixed(1), cx, cy);
          ctx.font = "12px sans-serif";
          ctx.fillStyle = chartText;
          ctx.fillText(minV.toFixed(0), cx - radius + 10, cy + 18);
          ctx.fillText(maxV.toFixed(0), cx + radius - 10, cy + 18);
        }}

        function drawScatter(canvasId, points, color) {{
          const canvas = document.getElementById(canvasId);
          if (!canvas) return;
          const {{ ctx, width, height }} = setupCanvas(canvas, 200, 160);
          ctx.clearRect(0, 0, width, height);
          if (!points || !points.length) return;
          const xs = points.map((p) => p.x);
          const ys = points.map((p) => p.y);
          const minX = Math.min(...xs);
          const maxX = Math.max(...xs);
          const minY = Math.min(...ys);
          const maxY = Math.max(...ys);
          const pad = 28;
          const w = width - pad * 2;
          const h = height - pad * 2;
          ctx.fillStyle = color || accentAlt;
          points.forEach((p) => {{
            const x = pad + w * (p.x - minX) / Math.max(1e-6, maxX - minX);
            const y = pad + h - h * (p.y - minY) / Math.max(1e-6, maxY - minY);
            ctx.beginPath();
            ctx.arc(x, y, 3, 0, Math.PI * 2);
            ctx.fill();
          }});
          ctx.fillStyle = chartText;
          ctx.font = "12px sans-serif";
          ctx.textAlign = "left";
          ctx.fillText(minY.toFixed(0), 6, pad + h);
          ctx.fillText(maxY.toFixed(0), 6, pad + 10);
          ctx.textAlign = "left";
          ctx.fillText(minX.toFixed(0), pad, height - 6);
          ctx.textAlign = "right";
          ctx.fillText(maxX.toFixed(0), pad + w, height - 6);
        }}

        function drawComfort(canvasId, tempValue, humValue) {{
          const canvas = document.getElementById(canvasId);
          if (!canvas) return;
          const {{ ctx, width, height }} = setupCanvas(canvas, 200, 140);
          ctx.clearRect(0, 0, width, height);
          const pad = 18;
          const w = width - pad * 2;
          const h = height - pad * 2;
          ctx.fillStyle = accent2Soft;
          ctx.fillRect(pad, pad, w, h);
          ctx.fillStyle = accentSoft;
          const comfortX = pad + w * 0.35;
          const comfortW = w * 0.3;
          const comfortY = pad + h * 0.3;
          const comfortH = h * 0.4;
          ctx.fillRect(comfortX, comfortY, comfortW, comfortH);
          if (tempValue === null || humValue === null) return;
          const tNorm = Math.max(0, Math.min(1, (tempValue - 30) / 70));
          const hNorm = Math.max(0, Math.min(1, humValue / 100));
          const x = pad + w * tNorm;
          const y = pad + h - h * hNorm;
          ctx.fillStyle = chartTitle;
          ctx.beginPath();
          ctx.arc(x, y, 5, 0, Math.PI * 2);
          ctx.fill();
          ctx.fillStyle = chartText;
          ctx.font = "12px sans-serif";
          ctx.fillText("Temp", pad, height - 6);
          ctx.save();
          ctx.translate(8, pad + h);
          ctx.rotate(-Math.PI / 2);
          ctx.fillText("Humidity", 0, 0);
          ctx.restore();
        }}

        function renderAll() {{
          drawLine("overviewTempChart", [payload.temp, payload.heat], [accent, accentAlt]);
          drawLine("overviewAqiChart", [payload.aqi], [accentWarm]);
          drawLine("trendTempChart", [payload.temp, payload.heat], [accent, accentAlt]);
          drawLine("trendAqiChart", [payload.aqi], [accentWarm]);
          drawLine("trendWindChart", [payload.wind, payload.gust], [accent, accentWarm]);
          drawLine("compareTempChart", [payload.temp_today, payload.temp_yesterday], [accent, accentWarm]);
          drawScatter("compareScatterChart", payload.aqi_wind, accentAlt);
          drawComfort("comfortChart", payload.current_temp, payload.current_humidity);
          if (payload.current_wind !== null) {{
            drawDial("windDial", payload.current_wind, 0, 40, accent);
          }}
          if (payload.current_pressure !== null) {{
            drawDial("pressureDial", payload.current_pressure, 28, 31, accentWarm);
          }}
        }}
        renderAll();

        grid.on("change", function(e, items) {{
          try {{
            localStorage.setItem("{grid_id}-layout", JSON.stringify(items));
          }} catch (err) {{}}
        }});
        try {{
          const saved = localStorage.getItem("{grid_id}-layout");
          if (saved) {{
            const items = JSON.parse(saved);
            grid.load(items);
          }}
        }} catch (err) {{}}
        const resizeObserver = new ResizeObserver(() => {{
          renderAll();
        }});
        document.querySelectorAll("#{grid_id} canvas").forEach((node) => {{
          resizeObserver.observe(node);
        }});
      }})();
    </script>
    """


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


# ------------------------
# Sidebar controls (periodic rerun to keep controls fresh)
# ------------------------
if CONTROL_REFRESH_SECONDS > 0:
    st_autorefresh(
        interval=CONTROL_REFRESH_SECONDS * 1000,
        key="controls_autorefresh",
    )

if "timeframe" not in st.session_state:
    st.session_state.timeframe = "24h"
if "custom_range" not in st.session_state:
    today = pd.Timestamp.utcnow().date()
    st.session_state.custom_range = (today - pd.Timedelta(days=1), today)

def render_filters_ui():
    st.markdown("<div class='section-title'>Date Filters</div>", unsafe_allow_html=True)
    timeframe = st.radio(
        "Date Filters",
        ["Today", "24h", "7d", "Custom"],
        horizontal=True,
        label_visibility="collapsed",
        index=["Today", "24h", "7d", "Custom"].index(st.session_state.timeframe),
    )
    st.session_state.timeframe = timeframe
    if timeframe == "Custom":
        date_range = st.date_input(
            "Date range (inclusive)",
            value=st.session_state.custom_range,
        )
        if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
            st.session_state.custom_range = (date_range[0], date_range[1])
    st.markdown("<div class='section-title'>Overlays</div>", unsafe_allow_html=True)
    st.session_state.show_feels = st.checkbox("Feels like", value=st.session_state.get("show_feels", True))
    st.session_state.show_aqi = st.checkbox("AQI", value=st.session_state.get("show_aqi", True))

render_left_rail(st.session_state.page, render_filters_ui)

def render_icon_rail(active_page: str):
    params = {}
    try:
        params = {key: value for key, value in st.query_params.items() if value}
    except Exception:
        params = {}

    def build_link(page_key: str) -> str:
        updated = dict(params)
        updated["page"] = page_key
        query = urlencode(updated, doseq=True)
        return f"?{query}" if query else "?page=" + page_key

    icons = [
        ("home", "H", "Home"),
        ("trends", "Tr", "Trends"),
        ("compare", "C", "Compare"),
        ("data", "D", "Data"),
    ]
    buttons = []
    for key, label, title in icons:
        active_class = "active" if key == active_page else ""
        href = build_link(key)
        buttons.append(
            f"<a class=\"icon-btn {active_class}\" href=\"{href}\" title=\"{title}\">{label}</a>"
        )
    st.markdown(
        "<div class='floating-rail'>" + "".join(buttons) + "</div>",
        unsafe_allow_html=True,
    )

render_icon_rail(st.session_state.page)

def compute_time_window():
    now = pd.Timestamp.utcnow()
    timeframe = st.session_state.timeframe
    if timeframe == "Today":
        start = now.tz_localize("UTC").tz_convert(LOCAL_TZ).normalize().tz_convert("UTC")
        return int(start.timestamp()), None, "today"
    if timeframe == "24h":
        return int((now - pd.Timedelta(hours=24)).timestamp()), None, "last 24h"
    if timeframe == "7d":
        return int((now - pd.Timedelta(days=7)).timestamp()), None, "last 7d"
    start_date, end_date = st.session_state.custom_range
    if end_date < start_date:
        start_date, end_date = end_date, start_date
    start_ts = pd.Timestamp(start_date)
    end_ts = pd.Timestamp(end_date) + pd.Timedelta(days=1)
    return int(start_ts.timestamp()), int(end_ts.timestamp()), f"{start_date} to {end_date}"

since_epoch, until_epoch, window_desc = compute_time_window()

filters_visible = True
palette_options = {
    "Aurora": {
        "scheme": "viridis",
        "mode": "dark",
        "bg": "#0f1115",
        "surface": "#161920",
        "surface_2": "#1a1d23",
        "surface_3": "#0d1016",
        "surface_4": "#101722",
        "border": "#232834",
        "text_primary": "#f4f7ff",
        "text_secondary": "#9aa4b5",
        "text_muted": "#8aa4c8",
        "accent": "#7be7d9",
        "accent2": "#61a5ff",
        "accent3": "#f2a85b",
        "status_ok": "#7be7d9",
        "status_warn": "#f2a85b",
        "status_bad": "#ff7b7b",
        "status_idle": "#9aa4b5",
    },
    "Solstice": {
        "scheme": "plasma",
        "mode": "dark",
        "bg": "#121015",
        "surface": "#1b1616",
        "surface_2": "#1f1a1a",
        "surface_3": "#111013",
        "surface_4": "#171318",
        "border": "#2a2122",
        "text_primary": "#fdf7f0",
        "text_secondary": "#cbbfb4",
        "text_muted": "#b4a39a",
        "accent": "#ffcc66",
        "accent2": "#ff8a5c",
        "accent3": "#6f79ff",
        "status_ok": "#ffcc66",
        "status_warn": "#ff8a5c",
        "status_bad": "#ff6b6b",
        "status_idle": "#cbbfb4",
    },
    "Monsoon": {
        "scheme": "magma",
        "mode": "dark",
        "bg": "#0e1117",
        "surface": "#171a22",
        "surface_2": "#1b1f2a",
        "surface_3": "#0c0f15",
        "surface_4": "#121721",
        "border": "#253046",
        "text_primary": "#f5f7ff",
        "text_secondary": "#a1acc4",
        "text_muted": "#8b95b0",
        "accent": "#5eead4",
        "accent2": "#38bdf8",
        "accent3": "#f472b6",
        "status_ok": "#5eead4",
        "status_warn": "#f472b6",
        "status_bad": "#ff6b6b",
        "status_idle": "#a1acc4",
    },
    "Ember": {
        "scheme": "inferno",
        "mode": "dark",
        "bg": "#140e0b",
        "surface": "#1d1512",
        "surface_2": "#221a16",
        "surface_3": "#100c0a",
        "surface_4": "#191210",
        "border": "#2f2420",
        "text_primary": "#fff4e8",
        "text_secondary": "#ccb7a8",
        "text_muted": "#b49c8f",
        "accent": "#f97316",
        "accent2": "#f43f5e",
        "accent3": "#facc15",
        "status_ok": "#f97316",
        "status_warn": "#facc15",
        "status_bad": "#ff6b6b",
        "status_idle": "#ccb7a8",
    },
    "Glacier": {
        "scheme": "cividis",
        "mode": "light",
        "bg": "#f4f7fb",
        "surface": "#ffffff",
        "surface_2": "#f7f9fc",
        "surface_3": "#e7edf5",
        "surface_4": "#eef3f9",
        "border": "#d5dde8",
        "text_primary": "#101828",
        "text_secondary": "#475467",
        "text_muted": "#5b677a",
        "accent": "#8fe3ff",
        "accent2": "#4f7ecb",
        "accent3": "#ffd39c",
        "status_ok": "#4f7ecb",
        "status_warn": "#ffd39c",
        "status_bad": "#d64545",
        "status_idle": "#475467",
    },
    "Harbor Fog": {
        "scheme": "tableau10",
        "mode": "light",
        "bg": "#f2f4f7",
        "surface": "#ffffff",
        "surface_2": "#f5f7fa",
        "surface_3": "#e6ebf2",
        "surface_4": "#eef2f7",
        "border": "#d0d7e2",
        "text_primary": "#1f2937",
        "text_secondary": "#4b5563",
        "text_muted": "#64748b",
        "accent": "#5fb3b3",
        "accent2": "#3b6ea5",
        "accent3": "#d8b26e",
        "status_ok": "#3b6ea5",
        "status_warn": "#d8b26e",
        "status_bad": "#d64545",
        "status_idle": "#4b5563",
    },
    "Canyon": {
        "scheme": "set2",
        "mode": "dark",
        "bg": "#140f0d",
        "surface": "#1f1714",
        "surface_2": "#241b17",
        "surface_3": "#100c0a",
        "surface_4": "#191210",
        "border": "#30241e",
        "text_primary": "#fff1e6",
        "text_secondary": "#c6b1a3",
        "text_muted": "#b19a8b",
        "accent": "#e76f51",
        "accent2": "#f4a261",
        "accent3": "#2a9d8f",
        "status_ok": "#e76f51",
        "status_warn": "#f4a261",
        "status_bad": "#ff6b6b",
        "status_idle": "#c6b1a3",
    },
    "Grove": {
        "scheme": "set3",
        "mode": "dark",
        "bg": "#0f1410",
        "surface": "#161f18",
        "surface_2": "#1b241d",
        "surface_3": "#0c0f0d",
        "surface_4": "#121916",
        "border": "#253026",
        "text_primary": "#f2fff5",
        "text_secondary": "#a8b8aa",
        "text_muted": "#8fa08f",
        "accent": "#8bc34a",
        "accent2": "#2f855a",
        "accent3": "#e6b566",
        "status_ok": "#8bc34a",
        "status_warn": "#e6b566",
        "status_bad": "#ff6b6b",
        "status_idle": "#a8b8aa",
    },
    "Signal": {
        "scheme": "dark2",
        "mode": "dark",
        "bg": "#0c1216",
        "surface": "#141b20",
        "surface_2": "#1a2228",
        "surface_3": "#0a0f12",
        "surface_4": "#10171c",
        "border": "#22303a",
        "text_primary": "#eaf2f7",
        "text_secondary": "#96a5b3",
        "text_muted": "#7f8f9f",
        "accent": "#17c3b2",
        "accent2": "#ffcb77",
        "accent3": "#fe6d73",
        "status_ok": "#17c3b2",
        "status_warn": "#ffcb77",
        "status_bad": "#fe6d73",
        "status_idle": "#96a5b3",
    },
    "Circuit": {
        "scheme": "turbo",
        "mode": "dark",
        "bg": "#0b1210",
        "surface": "#141b18",
        "surface_2": "#1a211e",
        "surface_3": "#0a0e0c",
        "surface_4": "#101613",
        "border": "#212a25",
        "text_primary": "#ecfdf5",
        "text_secondary": "#9bb3a8",
        "text_muted": "#7f948a",
        "accent": "#00d1b2",
        "accent2": "#3a86ff",
        "accent3": "#ffbe0b",
        "status_ok": "#00d1b2",
        "status_warn": "#ffbe0b",
        "status_bad": "#ff6b6b",
        "status_idle": "#9bb3a8",
    },
}
theme_names = list(palette_options.keys()) + ["Custom"]
palette_param = st.query_params.get("palette")
if isinstance(palette_param, list):
    palette_param = palette_param[0] if palette_param else None
initial_theme = palette_param if palette_param in theme_names else "Aurora"
if "custom_theme" not in st.session_state:
    st.session_state.custom_theme = palette_options["Aurora"].copy()
    st.session_state.custom_theme["scheme"] = "tableau10"
    st.session_state.custom_theme["mode"] = "dark"
if "theme_name" in st.session_state and st.session_state.theme_name not in theme_names:
    st.session_state.theme_name = "Aurora"
def persist_palette_choice():
    selected = st.session_state.theme_name
    current = st.query_params.get("palette")
    if isinstance(current, list):
        current = current[0] if current else None
    if current != selected:
        st.query_params["palette"] = selected

if filters_visible:
    st.sidebar.selectbox(
        "Palette",
        theme_names,
        index=theme_names.index(initial_theme),
        key="theme_name",
        on_change=persist_palette_choice,
    )
    theme_name = st.session_state.theme_name
    custom_theme = st.session_state.custom_theme
    if theme_name == "Custom":
        custom_theme["mode"] = st.sidebar.radio(
            "Mode",
            ["dark", "light"],
            index=["dark", "light"].index(custom_theme.get("mode", "dark")),
            horizontal=True,
        )
        custom_theme["bg"] = st.sidebar.color_picker("Background", value=custom_theme["bg"])
        custom_theme["surface"] = st.sidebar.color_picker("Surface", value=custom_theme["surface"])
        custom_theme["surface_2"] = st.sidebar.color_picker("Surface 2", value=custom_theme["surface_2"])
        custom_theme["surface_3"] = st.sidebar.color_picker("Surface 3", value=custom_theme["surface_3"])
        custom_theme["surface_4"] = st.sidebar.color_picker("Surface 4", value=custom_theme["surface_4"])
        custom_theme["border"] = st.sidebar.color_picker("Border", value=custom_theme["border"])
        custom_theme["text_primary"] = st.sidebar.color_picker("Text Primary", value=custom_theme["text_primary"])
        custom_theme["text_secondary"] = st.sidebar.color_picker("Text Secondary", value=custom_theme["text_secondary"])
        custom_theme["text_muted"] = st.sidebar.color_picker("Text Muted", value=custom_theme["text_muted"])
        custom_theme["accent"] = st.sidebar.color_picker("Accent", value=custom_theme["accent"])
        custom_theme["accent2"] = st.sidebar.color_picker("Accent 2", value=custom_theme["accent2"])
        custom_theme["accent3"] = st.sidebar.color_picker("Accent 3", value=custom_theme["accent3"])
        custom_theme["status_ok"] = st.sidebar.color_picker("Status OK", value=custom_theme["status_ok"])
        custom_theme["status_warn"] = st.sidebar.color_picker("Status Warn", value=custom_theme["status_warn"])
        custom_theme["status_bad"] = st.sidebar.color_picker("Status Bad", value=custom_theme["status_bad"])
        custom_theme["status_idle"] = st.sidebar.color_picker("Status Idle", value=custom_theme["status_idle"])
        custom_theme["scheme"] = st.sidebar.selectbox(
            "Chart scheme",
            ["tableau10", "viridis", "plasma", "magma", "inferno", "cividis", "set2", "set3", "dark2", "turbo"],
            index=[
                "tableau10",
                "viridis",
                "plasma",
                "magma",
                "inferno",
                "cividis",
                "set2",
                "set3",
                "dark2",
                "turbo",
            ].index(custom_theme.get("scheme", "tableau10")),
        )
        st.session_state.custom_theme = custom_theme
else:
    if "theme_name" not in st.session_state:
        st.session_state.theme_name = initial_theme
    theme_name = st.session_state.theme_name
    custom_theme = st.session_state.custom_theme

if theme_name == "Custom":
    theme = custom_theme
else:
    theme = palette_options[theme_name].copy()

CHART_SCHEME = theme["scheme"]
accent_soft = hex_to_rgba(theme["accent"], 0.18)
accent_border = hex_to_rgba(theme["accent"], 0.55)
accent2_soft = hex_to_rgba(theme["accent2"], 0.18)
accent2_border = hex_to_rgba(theme["accent2"], 0.35)
accent2_glow = hex_to_rgba(theme["accent2"], 0.4)
accent3_soft = hex_to_rgba(theme["accent3"], 0.12)
accent3_border = hex_to_rgba(theme["accent3"], 0.35)
accent3_glow = hex_to_rgba(theme["accent3"], 0.6)
border_muted = hex_to_rgba(theme["border"], 0.55)
status_ok = theme["status_ok"]
status_warn = theme["status_warn"]
status_bad = theme["status_bad"]
status_idle = theme["status_idle"]
status_ok_border = hex_to_rgba(status_ok, 0.4)
status_warn_border = hex_to_rgba(status_warn, 0.45)
status_bad_border = hex_to_rgba(status_bad, 0.45)
status_bad_border_strong = hex_to_rgba(status_bad, 0.55)
status_idle_border = hex_to_rgba(status_idle, 0.35)
status_warn_soft = hex_to_rgba(status_warn, 0.08)
status_bad_soft = hex_to_rgba(status_bad, 0.08)
status_bad_strong = hex_to_rgba(status_bad, 0.14)
THEME_MODE = theme["mode"]
THEME_COLORS = {
    "accent": theme["accent"],
    "accent2": theme["accent2"],
    "accent3": theme["accent3"],
    "status_ok": status_ok,
    "status_warn": status_warn,
    "status_bad": status_bad,
    "status_idle": status_idle,
    "text_primary": theme["text_primary"],
    "text_secondary": theme["text_secondary"],
    "text_muted": theme["text_muted"],
    "border": theme["border"],
    "surface_3": theme["surface_3"],
}
CHART_LABEL_COLOR = theme["text_secondary"]
CHART_TITLE_COLOR = theme["text_primary"]
CHART_TEXT_COLOR = theme["text_secondary"]
CHART_GRID_COLOR = border_muted
GAUGE_COLORS = {
    "temp": theme["accent2"],
    "air_temp": theme["accent"],
    "feels": theme["accent"],
    "hum": theme["accent"],
    "pressure": theme["accent3"],
    "wind": theme["accent2"],
    "gust": theme["accent3"],
}
COLLECTOR_COLORS = {
    "airlink_collector": (theme["accent"], theme["accent2"]),
    "tempest_collector": (theme["accent2"], theme["accent3"]),
}
WATCHDOG_COLORS = (status_warn, theme["accent3"])
st.markdown(
    f"""
    <style>
    :root {{
      --color-scheme: {theme['mode']};
      --bg: {theme['bg']};
      --surface: {theme['surface']};
      --surface-2: {theme['surface_2']};
      --surface-3: {theme['surface_3']};
      --surface-4: {theme['surface_4']};
      --border: {theme['border']};
      --border-muted: {border_muted};
      --text-primary: {theme['text_primary']};
      --text-secondary: {theme['text_secondary']};
      --text-muted: {theme['text_muted']};
      --chart-text: {theme['text_secondary']};
      --chart-title: {theme['text_primary']};
      --chart-grid: {border_muted};
      --accent: {theme['accent']};
      --accent-2: {theme['accent2']};
      --accent-3: {theme['accent3']};
      --accent-soft: {accent_soft};
      --accent-border: {accent_border};
      --accent-2-soft: {accent2_soft};
      --accent-2-border: {accent2_border};
      --accent-2-glow: {accent2_glow};
      --accent-3-soft: {accent3_soft};
      --accent-3-border: {accent3_border};
      --accent-3-glow: {accent3_glow};
      --status-ok: {status_ok};
      --status-warn: {status_warn};
      --status-bad: {status_bad};
      --status-idle: {status_idle};
      --status-ok-border: {status_ok_border};
      --status-warn-border: {status_warn_border};
      --status-bad-border: {status_bad_border};
      --status-bad-border-strong: {status_bad_border_strong};
      --status-idle-border: {status_idle_border};
      --status-warn-soft: {status_warn_soft};
      --status-bad-soft: {status_bad_soft};
      --status-bad-strong: {status_bad_strong};
    }}
    </style>
    """,
    unsafe_allow_html=True,
)

st.markdown(
"""
<style>
.daily-card {
    background: var(--surface-3);
    border: 1px solid var(--border);
    border-radius: 12px;
    padding: 10px 12px;
    margin-top: 8px;
    box-shadow: 0 6px 16px rgba(0,0,0,0.15);
}
.daily-card .daily-date { font-weight: 700; color: var(--text-primary); font-size: 0.95rem; }
.daily-card .daily-cond { color: var(--text-secondary); margin: 2px 0 6px; }
.daily-card .daily-temps { color: var(--text-primary); font-size: 0.9rem; }
.daily-card .daily-temps .hi { color: var(--status-warn); }
.daily-card .daily-temps .lo { color: var(--accent-2); }
.daily-card .daily-precip, .daily-card .daily-sun { color: var(--text-muted); font-size: 0.85rem; }
</style>
""",
unsafe_allow_html=True,
)

if "station_lat" not in st.session_state:
    st.session_state.station_lat = None
if "station_lon" not in st.session_state:
    st.session_state.station_lon = None
if "override_location_enabled" not in st.session_state:
    st.session_state.override_location_enabled = False
if "location_config_loaded" not in st.session_state:
    st.session_state.location_config_loaded = False

override_enabled = False
lat_override = None
lon_override = None
try:
    with closing(config_connect(DB_PATH)) as conn:
        override_enabled = bool(get_bool(conn, "override_location_enabled") or False)
        lat_override = get_float(conn, "station_lat_override")
        lon_override = get_float(conn, "station_lon_override")
except Exception:
    override_enabled = False

if not st.session_state.location_config_loaded:
    st.session_state.override_location_enabled = override_enabled
    if override_enabled:
        if lat_override is not None:
            st.session_state.station_lat = lat_override
        if lon_override is not None:
            st.session_state.station_lon = lon_override
    st.session_state.location_config_loaded = True

tempest_token = os.getenv("TEMPEST_API_TOKEN")
token_present = bool(tempest_token)
auto_location = fetch_station_location(tempest_token, TEMPEST_STATION_ID) if tempest_token else None
auto_has_coords = auto_location and auto_location.get("lat") is not None and auto_location.get("lon") is not None
if auto_has_coords and not st.session_state.override_location_enabled:
    st.session_state.station_lat = float(auto_location["lat"])
    st.session_state.station_lon = float(auto_location["lon"])

if filters_visible:
    with st.sidebar.expander("Location", expanded=False):
        if auto_has_coords:
            st.caption(
                f"Using Tempest station: {auto_location.get('name', 'Tempest Station')} "
                f"({float(auto_location['lat']):.4f}, {float(auto_location['lon']):.4f})"
            )

        override_location = st.checkbox(
            "Override location",
            value=st.session_state.override_location_enabled,
        )
        st.session_state.override_location_enabled = override_location

        lat_value = st.session_state.station_lat
        lon_value = st.session_state.station_lon
        if lat_value is None and auto_has_coords:
            lat_value = float(auto_location["lat"])
        if lon_value is None and auto_has_coords:
            lon_value = float(auto_location["lon"])
        if lat_value is None:
            lat_value = 0.0
        if lon_value is None:
            lon_value = 0.0

        station_lat = st.number_input(
            "Latitude",
            min_value=-90.0,
            max_value=90.0,
            value=float(lat_value),
            format="%.4f",
            help="Used for sunrise/sunset times.",
            disabled=not override_location,
        )
        station_lon = st.number_input(
            "Longitude",
            min_value=-180.0,
            max_value=180.0,
            value=float(lon_value),
            format="%.4f",
            help="Used for sunrise/sunset times.",
            disabled=not override_location,
        )

        if st.button("Save location"):
            try:
                with closing(config_connect(DB_PATH)) as conn:
                    set_bool(conn, "override_location_enabled", override_location)
                    set_float(conn, "station_lat_override", station_lat)
                    set_float(conn, "station_lon_override", station_lon)
                st.session_state.override_location_enabled = override_location
                if override_location:
                    st.session_state.station_lat = station_lat
                    st.session_state.station_lon = station_lon
                    st.success("Saved location override.")
                else:
                    st.success("Saved location and disabled override.")
            except Exception:
                st.error("Unable to save location override.")

forecast_hourly = None
forecast_daily = None
forecast_source = None
forecast_tz = LOCAL_TZ
forecast_updated = None
forecast_status = None
try:
    lat_for_forecast = st.session_state.get("station_lat")
    lon_for_forecast = st.session_state.get("station_lon")
    api_key = os.getenv("TEMPEST_API_KEY")
    # Prefer Open-Meteo (no key), fallback to Tempest credentials when present.
    if lat_for_forecast is not None and lon_for_forecast is not None:
        om_hourly, om_daily, om_status = fetch_openmeteo_forecast(
            lat_for_forecast,
            lon_for_forecast,
            LOCAL_TZ,
        )
        if om_hourly is not None or om_daily is not None:
            forecast_hourly = om_hourly
            forecast_daily = om_daily
            forecast_source = "Open-Meteo"
            forecast_status = om_status
            try:
                forecast_updated = pd.Timestamp.utcnow().tz_convert(LOCAL_TZ)
            except Exception:
                forecast_updated = pd.Timestamp.utcnow()
    # If no Open-Meteo data, try Tempest.
    if (forecast_hourly is None and forecast_daily is None) and (tempest_token or api_key):
        payload, forecast_status = fetch_tempest_forecast(
            tempest_token,
            TEMPEST_STATION_ID,
            lat=lat_for_forecast,
            lon=lon_for_forecast,
            api_key=api_key,
        )
        if payload:
            hourly_df, daily_df, forecast_tz = parse_tempest_forecast(payload, LOCAL_TZ)
            forecast_hourly = hourly_df
            forecast_daily = daily_df
            forecast_source = "Tempest Better Forecast"
            try:
                forecast_updated = pd.Timestamp.utcnow().tz_convert(forecast_tz)
            except Exception:
                forecast_updated = pd.Timestamp.utcnow()
except Exception as exc:
    forecast_hourly = None
    forecast_daily = None
    forecast_status = f"Request failed: {exc}"

saved_alert_config, _ = load_alert_config(DB_PATH)
saved_alert_email = saved_alert_config.get("alert_email_to", "")
saved_alert_sms = saved_alert_config.get("alert_sms_to", "")
if "alert_email_to" not in st.session_state:
    st.session_state.alert_email_to = saved_alert_email or os.getenv("ALERT_EMAIL_TO", "")
if "alert_sms_to" not in st.session_state:
    st.session_state.alert_sms_to = saved_alert_sms or os.getenv("VERIZON_SMS_TO", "")
if "smtp_username" not in st.session_state:
    st.session_state.smtp_username = os.getenv("SMTP_USERNAME", "")
if "smtp_password" not in st.session_state:
    st.session_state.smtp_password = os.getenv("SMTP_PASSWORD") or ""
if "smtp_from" not in st.session_state:
    st.session_state.smtp_from = os.getenv("ALERT_EMAIL_FROM") or st.session_state.smtp_username

if filters_visible:
    with st.sidebar.expander("Alerts", expanded=False):
        if ALERTS_WORKER_ENABLED:
            st.caption("Background alert worker enabled; UI will not send alerts.")
        alert_email_to = st.text_input(
            "Alert recipient email",
            value=st.session_state.alert_email_to,
            help="Defaults to your Gmail address if left blank.",
        )
        alert_sms_to = st.text_input(
            "Verizon SMS number",
            value=st.session_state.alert_sms_to,
            help="Digits only; leave blank to use VERIZON_SMS_TO from the environment.",
        )
        st.session_state.alert_email_to = alert_email_to.strip()
        st.session_state.alert_sms_to = alert_sms_to.strip()
        saved_bits = []
        if saved_alert_email:
            saved_bits.append(f"Email: {saved_alert_email}")
        if saved_alert_sms:
            saved_bits.append(f"SMS: {saved_alert_sms}")
        if saved_bits:
            st.caption("Saved recipients for worker: " + " | ".join(saved_bits))
            st.caption("Stored in data/tempest.db (not committed).")
        else:
            st.caption("No saved recipients for the background worker yet.")
        if st.button("Save recipients for worker"):
            saved_keys, cleared_keys = save_alert_config(
                DB_PATH,
                {
                    "alert_email_to": st.session_state.alert_email_to,
                    "alert_sms_to": st.session_state.alert_sms_to,
                },
            )
            if saved_keys:
                st.success("Saved recipients for background alerts.")
            elif cleared_keys:
                st.success("Cleared saved recipients.")
            else:
                st.warning("Nothing to save yet.")
        if saved_bits and st.button("Clear saved recipients"):
            delete_alert_config(DB_PATH, ["alert_email_to", "alert_sms_to"])
            st.success("Cleared saved recipients.")
        st.markdown("---")
        st.caption("Email auth (Gmail SMTP: smtp.gmail.com:587 TLS). Use a Gmail app password.")
        smtp_username = st.text_input(
            "Gmail address",
            value=st.session_state.smtp_username,
        )
        smtp_password = st.text_input(
            "Gmail app password",
            value=st.session_state.smtp_password,
            type="password",
        )
        st.caption("Password is stored only for this session.")
        if smtp_password:
            sanitized_password = re.sub(r"\\s+", "", smtp_password)
            if sanitized_password != smtp_password:
                st.caption("Removed spaces from the app password.")
            smtp_password = sanitized_password
        smtp_from = st.text_input(
            "From address",
            value=st.session_state.smtp_from,
            help="Defaults to the Gmail address.",
        )
        st.session_state.smtp_username = smtp_username.strip()
        st.session_state.smtp_password = smtp_password
        st.session_state.smtp_from = smtp_from.strip()
        if st.button("Send test alert"):
            test_temp = st.session_state.get("latest_temp_for_alerts")
            if test_temp is None:
                test_temp = FREEZE_WARNING_F
            now_value = st.session_state.get("latest_now_local")
            if now_value is None:
                now_value = pd.Timestamp.now(tz="UTC").tz_convert(LOCAL_TZ)
            email_sent, sms_sent, email_error, sms_error = send_test_alerts(float(test_temp), now_value)
            if email_sent or sms_sent:
                st.success("Test alert sent.")
            else:
                st.warning("Test alert not sent.")
            if email_error:
                st.caption(f"Email: {email_error}")
            if sms_error:
                st.caption(f"SMS: {sms_error}")

render_alert_overrides_sync()

# ------------------------
# Data load and transforms
# ------------------------
now_ts = pd.Timestamp.utcnow()
recent_cutoff_epoch = int((now_ts - pd.Timedelta(hours=1)).timestamp())
hub_recent_cutoff_epoch = int((now_ts - pd.Timedelta(hours=24)).timestamp())

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
        wind_gust,
        wind_dir,
        rain_accumulated,
        lightning_strike_count,
        battery,
        solar_radiation,
        uv
    FROM obs_st
    WHERE obs_epoch >= :since
    {tempest_until_clause}
    ORDER BY obs_epoch
    """,
    {"since": since_epoch, **({"until": until_epoch} if until_epoch is not None else {})},
)

if AIRLINK_TABLE:
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
        FROM {AIRLINK_TABLE}
        WHERE ts >= :since
        {airlink_until_clause}
        ORDER BY ts
        """,
        {"since": since_epoch, **({"until": until_epoch} if until_epoch is not None else {})},
    )
else:
    airlink = pd.DataFrame()

# Tempest transforms
tempest_latest = None
tempest_temp_delta = None
tempest_hum_delta = None
tempest_pressure_delta = None
tempest_wind_delta = None
tempest_extremes = {}
rain_total_mm = None
lightning_48h = 0

if not tempest.empty:
    tempest["time"] = epoch_to_dt(tempest["obs_epoch"])
    tempest["air_temperature_f"] = c_to_f(tempest["air_temperature"])
    tempest["heat_index_f"] = compute_heat_index(
        tempest["air_temperature_f"],
        tempest["relative_humidity"],
    )
    tempest["pressure_inhg"] = hpa_to_inhg(tempest["station_pressure"])
    tempest["wind_speed_mph"] = mps_to_mph(tempest["wind_avg"])
    if "wind_gust" in tempest:
        tempest["wind_gust_mph"] = mps_to_mph(tempest["wind_gust"])
    if "rain_accumulated" in tempest:
        tempest["rain_mm"] = tempest["rain_accumulated"].astype(float)
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
    if "rain_mm" in tempest:
        rain_total_mm = max(
            0.0,
            float(tempest["rain_mm"].iloc[-1]) - float(tempest["rain_mm"].iloc[0]),
        )
    if "lightning_strike_count" in tempest and "obs_epoch" in tempest:
        cutoff = (now_ts - pd.Timedelta(hours=48)).timestamp()
        lightning_48h = int(tempest.loc[tempest["obs_epoch"] >= cutoff, "lightning_strike_count"].sum())

# AirLink transforms
airlink_latest = None
aqi_share_df = pd.DataFrame()
if not airlink.empty:
    airlink["time"] = epoch_to_dt(airlink["ts"])
    airlink["aqi_pm25"] = airlink["pm_2p5"].apply(compute_pm25_aqi)
    airlink_latest = airlink.iloc[-1]
    aqi_share_df = aqi_zone_share(airlink["aqi_pm25"])

# Current metrics
current_temp = float(tempest_latest.air_temperature_f) if tempest_latest is not None else None
current_feels = float(tempest_latest.heat_index_f) if tempest_latest is not None else None
current_humidity = float(tempest_latest.relative_humidity) if tempest_latest is not None else None
current_pressure = float(tempest_latest.pressure_inhg) if tempest_latest is not None else None
current_wind = float(tempest_latest.wind_speed_mph) if tempest_latest is not None else None
current_gust = (
    float(tempest_latest.wind_gust_mph)
    if tempest_latest is not None and "wind_gust_mph" in tempest_latest
    else None
)
current_wind_deg = (
    float(tempest_latest.wind_dir_deg)
    if tempest_latest is not None and "wind_dir_deg" in tempest_latest
    else None
)
current_wind_dir = (
    compass_dir(tempest_latest.wind_dir_deg)
    if tempest_latest is not None and "wind_dir_deg" in tempest_latest
    else "--"
)
current_aqi = (
    float(airlink_latest.aqi_pm25)
    if airlink_latest is not None and pd.notna(airlink_latest.aqi_pm25)
    else None
)
current_dew = (
    float(airlink_latest.dew_point_f)
    if airlink_latest is not None and pd.notna(airlink_latest.dew_point_f)
    else None
)
current_lightning = lightning_48h
current_battery = (
    float(tempest_latest.battery)
    if tempest_latest is not None and "battery" in tempest_latest
    else None
)
current_solar = (
    float(tempest_latest.solar_radiation)
    if tempest_latest is not None and "solar_radiation" in tempest_latest
    else None
)
current_uv = (
    float(tempest_latest.uv)
    if tempest_latest is not None and "uv" in tempest_latest
    else None
)
if not tempest.empty:
    if (current_solar is None or current_solar == 0) and "solar_radiation" in tempest:
        solar_series = tempest["solar_radiation"].dropna()
        solar_nonzero = solar_series[solar_series > 0]
        if not solar_nonzero.empty:
            current_solar = float(solar_nonzero.iloc[-1])
    if (current_uv is None or current_uv == 0) and "uv" in tempest:
        uv_series = tempest["uv"].dropna()
        uv_nonzero = uv_series[uv_series > 0]
        if not uv_nonzero.empty:
            current_uv = float(uv_nonzero.iloc[-1])

now_local = pd.Timestamp.now(tz="UTC").tz_convert(LOCAL_TZ)
st.session_state.latest_temp_for_alerts = current_temp
st.session_state.latest_now_local = now_local
sun_times = None
sunrise_local = None
sunset_local = None
if st.session_state.station_lat is not None and st.session_state.station_lon is not None:
    sun_times = fetch_sun_times(
        st.session_state.station_lat,
        st.session_state.station_lon,
        now_local.date().isoformat(),
    )
if sun_times:
    sunrise_local = pd.to_datetime(sun_times.get("sunrise"), utc=True).tz_convert(LOCAL_TZ)
    sunset_local = pd.to_datetime(sun_times.get("sunset"), utc=True).tz_convert(LOCAL_TZ)

alert_overrides = alert_overrides_from_session()
alert_email_to, alert_sms_to = resolve_alert_recipients(DB_PATH, overrides=alert_overrides)
alert_state = load_alert_state(DB_PATH)
alerts_to_send, reset_updates = determine_freeze_alerts(current_temp, alert_state)
if reset_updates:
    save_alert_state(DB_PATH, reset_updates)
alert_banner_html = build_freeze_banner(current_temp, now_local)
if alerts_to_send and not ALERTS_WORKER_ENABLED and current_temp is not None:
    temp_value = float(current_temp)
    for alert in alerts_to_send:
        message_body = build_freeze_alert_message(alert["title"], temp_value, now_local)
        subject = f"{alert['title']} - Tempest {temp_value:.1f} F"
        email_sent = send_email(
            subject,
            message_body,
            to_address=alert_email_to,
            overrides=alert_overrides,
        )
        sms_sent = send_verizon_sms(
            message_body,
            sms_number=alert_sms_to,
            overrides=alert_overrides,
        )
        if email_sent or sms_sent:
            save_alert_state(DB_PATH, alert["state_updates"])


def metric_text(value, fmt_str="{:.1f}", suffix=""):
    text = fmt_value(value, fmt_str)
    if text == "--":
        return "--"
    return f"{text}{suffix}"


sun_chip_text = "--"
if sunrise_local and sunset_local:
    sun_chip_text = f"{fmt_time(sunrise_local)} to {fmt_time(sunset_local)}"
wind_dir_text = current_wind_dir if current_wind_dir is not None else "--"

header_html = f"""
<div class="header-row">
  <div class="header-title">Tempest</div>
  <div class="metric-chip"><span class="chip-icon">T</span><span>{metric_text(current_temp, "{:.1f}", "F")}</span></div>
  <div class="metric-chip"><span class="chip-icon">AQ</span><span>{metric_text(current_aqi, "{:.0f}")}</span></div>
  <div class="metric-chip"><span class="chip-icon">W</span><span>{metric_text(current_wind, "{:.1f}", " mph")}</span></div>
  <div class="metric-chip"><span class="chip-icon">P</span><span>{metric_text(current_pressure, "{:.2f}", " inHg")}</span></div>
  <div class="metric-chip"><span class="chip-icon">Sun</span><span>{sun_chip_text}</span></div>
</div>
"""
render_header_strip(header_html)

# Ingest health
ingest_sources = []
if AIRLINK_TABLE:
    airlink_activity = recent_activity(AIRLINK_TABLE, "ts", recent_cutoff_epoch)
else:
    airlink_activity = {"count": 0, "last_epoch": None}
station_activity = recent_activity("obs_st", "obs_epoch", recent_cutoff_epoch, "device_id", TEMPEST_STATION_ID)
hub_activity = recent_activity(
    "raw_events",
    "received_at_epoch",
    hub_recent_cutoff_epoch,
    message_col="message_type",
    message_types=["connection_opened", "ack"],
)

for label, activity, colors in [
    ("AirLink", airlink_activity, (THEME_COLORS["accent"], THEME_COLORS["accent2"])),
    ("Tempest Station", station_activity, (THEME_COLORS["accent2"], THEME_COLORS["accent3"])),
    ("Tempest Hub", hub_activity, (THEME_COLORS["accent3"], THEME_COLORS["accent"])),
]:
    latency_minutes = minutes_since_epoch(activity["last_epoch"], now_ts)
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

latency_values = [
    s["latency_minutes"]
    for s in ingest_sources
    if s["latency_minutes"] is not None and s["name"] != "Tempest Hub"
]
avg_latency_minutes = sum(latency_values) / len(latency_values) if latency_values else None
avg_latency_text = latency_label(avg_latency_minutes) if avg_latency_minutes is not None else None
total_recent = sum(s["recent_count"] for s in ingest_sources if s["recent_count"] is not None)

collector_statuses = build_collector_statuses(now_ts)

# Forecast charts
forecast_chart = forecast_hourly_chart(forecast_hourly) if forecast_hourly is not None else None
forecast_outlook = None
if forecast_daily is not None and not forecast_daily.empty:
    outlook_df = forecast_daily[["day_start_local", "air_temp_high", "air_temp_low"]].rename(
        columns={"day_start_local": "time", "air_temp_high": "High", "air_temp_low": "Low"}
    )
    outlook_long = outlook_df.melt(id_vars=["time"], var_name="metric", value_name="value")
    forecast_outlook = clean_chart(outlook_long, height=220, title=None)

# Daily brief
brief_rows = []
try:
    with sqlite3.connect(DB_PATH) as conn:
        brief_rows = load_daily_briefs(conn)
except Exception:
    brief_rows = []

brief_today = brief_rows[0] if brief_rows else None
brief_yesterday = brief_rows[1] if len(brief_rows) > 1 else None

rain_total_in = rain_total_mm / 25.4 if rain_total_mm is not None else None
include_feels = st.session_state.get("show_feels", True)
include_aqi = st.session_state.get("show_aqi", True)

metrics_ctx = []
metrics_ctx.append(
    {
        "icon": "T",
        "label": "Temperature",
        "value": metric_text(current_temp, "{:.1f}", "F"),
        "subvalue": (
            f"Feels {metric_text(current_feels, '{:.1f}', 'F')}"
            if include_feels and current_feels is not None
            else None
        ),
    }
)
if include_aqi:
    metrics_ctx.append(
        {
            "icon": "AQ",
            "label": "AQI",
            "value": metric_text(current_aqi, "{:.0f}"),
            "subvalue": aqi_badge_label(current_aqi),
        }
    )
metrics_ctx.extend(
    [
        {
            "icon": "W",
            "label": "Wind",
            "value": metric_text(current_wind, "{:.1f}", " mph"),
            "subvalue": f"Gust {metric_text(current_gust, '{:.1f}', ' mph')}" if current_gust is not None else None,
        },
        {
            "icon": "H",
            "label": "Humidity",
            "value": metric_text(current_humidity, "{:.0f}", "%"),
            "subvalue": f"Dew {metric_text(current_dew, '{:.1f}', 'F')}" if current_dew is not None else None,
        },
        {
            "icon": "P",
            "label": "Pressure",
            "value": metric_text(current_pressure, "{:.2f}", " inHg"),
            "subvalue": None,
        },
        {
            "icon": "R",
            "label": "Rain",
            "value": metric_text(rain_total_in, "{:.2f}", " in"),
            "subvalue": "Window total" if rain_total_in is not None else None,
        },
    ]
)

trend_series = {}
if not tempest.empty:
    temp_df = tempest[["time", "air_temperature_f"]].rename(columns={"air_temperature_f": "value"})
    temp_df["metric"] = "Temperature"
    trend_series["Temperature"] = temp_df

    if include_feels:
        feels_df = tempest[["time", "heat_index_f"]].rename(columns={"heat_index_f": "value"})
        feels_df["metric"] = "Feels Like"
        trend_series["Feels Like"] = feels_df

    wind_df = tempest[["time", "wind_speed_mph"]].rename(columns={"wind_speed_mph": "value"})
    wind_df["metric"] = "Wind"
    trend_series["Wind"] = wind_df

    if "wind_gust_mph" in tempest:
        gust_df = tempest[["time", "wind_gust_mph"]].rename(columns={"wind_gust_mph": "value"})
        gust_df["metric"] = "Gust"
        trend_series["Gust"] = gust_df

    pressure_df = tempest[["time", "pressure_inhg"]].rename(columns={"pressure_inhg": "value"})
    pressure_df["metric"] = "Pressure"
    trend_series["Pressure"] = pressure_df

    humidity_df = tempest[["time", "relative_humidity"]].rename(columns={"relative_humidity": "value"})
    humidity_df["metric"] = "Humidity"
    trend_series["Humidity"] = humidity_df

if include_aqi and not airlink.empty:
    aqi_df = airlink[["time", "aqi_pm25"]].rename(columns={"aqi_pm25": "value"})
    aqi_df["metric"] = "AQI"
    trend_series["AQI"] = aqi_df

trend_defaults = ["Temperature", "Wind"]
if include_aqi:
    trend_defaults.insert(1, "AQI")

raw_tables = []
raw_limit = 200

raw_tempest = load_df(
    f"""
    SELECT *
    FROM obs_st
    WHERE obs_epoch >= :since
    {tempest_until_clause}
    ORDER BY obs_epoch DESC
    LIMIT :limit
    """,
    {
        "since": since_epoch,
        "limit": raw_limit,
        **({"until": until_epoch} if until_epoch is not None else {}),
    },
)
if not raw_tempest.empty:
    raw_tables.append({"title": "Tempest Station", "df": raw_tempest})

if AIRLINK_TABLE:
    airlink_obs_raw = load_df(
        f"""
        SELECT *
        FROM {AIRLINK_TABLE}
        WHERE ts >= :since
        {airlink_until_clause}
        ORDER BY ts DESC
        LIMIT :limit
        """,
        {
            "since": since_epoch,
            "limit": raw_limit,
            **({"until": until_epoch} if until_epoch is not None else {}),
        },
    )
    if not airlink_obs_raw.empty:
        raw_tables.append({"title": "AirLink", "df": airlink_obs_raw})

last_updated = {
    "Tempest": latest_ts_str(tempest_latest.obs_epoch) if tempest_latest is not None else "--",
    "AirLink": latest_ts_str(airlink_latest.ts) if airlink_latest is not None else "--",
    "Hub": latest_ts_str(hub_activity.get("last_epoch")) if hub_activity else "--",
}

page_ctx = {
    "forecast_chart": forecast_chart,
    "forecast_outlook": forecast_outlook,
    "forecast_hourly": forecast_hourly,
    "forecast_daily": forecast_daily,
    "forecast_source": forecast_source,
    "forecast_status": forecast_status,
    "forecast_updated": forecast_updated,
    "chart_renderer": clean_chart,
    "tz_name": LOCAL_TZ,
    "metrics": metrics_ctx,
    "brief_today": brief_today,
    "brief_yesterday": brief_yesterday,
    "trend_series": trend_series,
    "trend_defaults": trend_defaults,
    "tempest": tempest,
    "airlink": airlink,
    "raw_tables": raw_tables,
    "health": {
        "ingest_sources": ingest_sources,
        "avg_latency_text": avg_latency_text,
        "total_recent": total_recent,
        "collector_statuses": collector_statuses,
    },
    "alerts_html": alert_banner_html,
    "last_updated": last_updated,
}

page = st.session_state.page
if page == "home":
    main_col, right_col = render_main_layout()
    with main_col:
        page_home.render(page_ctx)
    with right_col:
        right_col.markdown("<div class='right-rail'>", unsafe_allow_html=True)
        if alert_banner_html:
            right_col.markdown(alert_banner_html, unsafe_allow_html=True)
        if metrics_ctx:
            right_col.markdown("<div class='section-title'>Now at a glance</div>", unsafe_allow_html=True)
            right_col.markdown("<div class='cards-grid'>", unsafe_allow_html=True)
            for metric in metrics_ctx:
                metric_card(
                    metric.get("icon", ""),
                    metric.get("label", "--"),
                    metric.get("value", "--"),
                    metric.get("subvalue"),
                )
            right_col.markdown("</div>", unsafe_allow_html=True)
        else:
            right_col.info("No current metrics available.")
        with right_col.expander("Station health", expanded=False):
            render_ingest_banner(
                ingest_sources,
                total_recent,
                avg_latency_text=avg_latency_text,
                collector_statuses=collector_statuses,
                target=st,
                include_diagnostics=False,
                title="",
            )
        last_updated_html = """<div class="card status-card">
  <div class="section-title">Last updated</div>
  <div class="status-line"><span>Tempest</span><span>{tempest}</span></div>
  <div class="status-line"><span>AirLink</span><span>{airlink}</span></div>
  <div class="status-line"><span>Hub</span><span>{hub}</span></div>
</div>""".format(
            tempest=last_updated["Tempest"],
            airlink=last_updated["AirLink"],
            hub=last_updated["Hub"],
        )
        right_col.markdown(last_updated_html, unsafe_allow_html=True)
        right_col.markdown("</div>", unsafe_allow_html=True)
elif page == "trends":
    page_trends.render(page_ctx)
elif page == "compare":
    page_compare.render(page_ctx)
else:
    page_data.render(page_ctx)
