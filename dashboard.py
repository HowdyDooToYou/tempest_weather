import sqlite3
import json
import html
import subprocess
import time
from datetime import datetime
import requests
from pathlib import Path
import os

import altair as alt
import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

DB_PATH = "data/tempest.db"
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
LOCAL_TZ = "America/New_York"

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

# ------------------------
# Local UI state (tabs/scroll)
# ------------------------
components.html(
    """
    <script>
    (function() {
      const storage = window.parent.localStorage || window.localStorage;
      const TAB_KEY = "tempest:last_tab";
      const SCROLL_KEY = "tempest:last_scroll";
      function tabButtons() {
        return Array.from(window.parent.document.querySelectorAll('button[role="tab"]'));
      }
      function activeTabLabel() {
        const tabs = tabButtons();
        const active = tabs.find((btn) => btn.getAttribute("aria-selected") === "true");
        return active ? active.textContent.trim() : "";
      }
      function attachTabHandlers() {
        const tabs = tabButtons();
        if (!tabs.length) return false;
        const active = activeTabLabel();
        if (active) storage.setItem(TAB_KEY, active);
        tabs.forEach((btn) => {
          btn.addEventListener("click", () => {
            storage.setItem(TAB_KEY, btn.textContent.trim());
          });
        });
        return true;
      }
      let tries = 0;
      const timer = window.parent.setInterval(() => {
        tries += 1;
        attachTabHandlers();
        if (tries > 12) {
          window.parent.clearInterval(timer);
        }
      }, 300);
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
    """,
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

# ------------------------
# Theming
# ------------------------
st.markdown(
    """
    <style>
    html { font-size: 110%; }
    :root {
        --accent: #7be7d9;
        --accent-2: #61a5ff;
        --accent-3: #f2a85b;
        --accent-soft: rgba(123,231,217,0.18);
        --accent-border: rgba(123,231,217,0.5);
        --text-primary: #f4f7ff;
        --text-secondary: #9aa4b5;
    }
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
    .chart-header {
        display: flex;
        align-items: center;
        gap: 8px;
        font-weight: 600;
        color: #e7ecf3;
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
        color: #9fb2cc;
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
        border: 1px solid rgba(110,140,190,0.18);
        background: linear-gradient(160deg, rgba(22,30,44,0.92), rgba(12,16,24,0.95));
        box-shadow: 0 18px 36px rgba(0,0,0,0.35);
    }
    .metric-card .label {
        font-size: 0.78rem;
        text-transform: uppercase;
        letter-spacing: 0.12em;
        color: #8aa4c8;
        margin-bottom: 6px;
    }
    .metric-card .value {
        font-size: 1.7rem;
        font-weight: 700;
        color: #f4f7ff;
    }
    .metric-card .sub {
        font-size: 0.78rem;
        color: #9fb2cc;
        margin-top: 4px;
    }
    .metric-expanders {
        display: block;
        margin-bottom: 14px;
    }
    .metric-expanders [data-testid="stExpander"] {
        border: 1px solid rgba(110,140,190,0.18);
        border-radius: 14px;
        background: linear-gradient(160deg, rgba(22,30,44,0.92), rgba(12,16,24,0.95));
        margin-bottom: 10px;
    }
    .metric-expanders [data-testid="stExpander"] summary {
        padding: 12px 14px;
        font-weight: 600;
        color: #f4f7ff;
    }
    .metric-expanders [data-testid="stExpander"] summary:hover {
        background: rgba(123,231,217,0.08);
    }
    .section-gap { margin-top: 18px; }
    .gauge-block {
        margin-top: 10px;
        padding: 12px 12px 10px 12px;
        border-radius: 12px;
        background: #161920;
        border: 1px solid #202636;
    }
    .wind-flow {
        position: relative;
        width: 92px;
        height: 92px;
        border-radius: 50%;
        border: 1px solid #233045;
        background: radial-gradient(circle at 50% 50%, rgba(97,165,255,0.12), transparent 60%);
        display: flex;
        align-items: center;
        justify-content: center;
        overflow: hidden;
    }
    .wind-flow .needle {
        position: absolute;
        width: 8px;
        height: 38px;
        background: linear-gradient(180deg, #7be7d9, #61a5ff);
        border-radius: 999px;
        transform-origin: 50% 100%;
        transform: translate(-50%, -50%) rotate(var(--wind-angle, 0deg));
        left: 50%;
        top: 50%;
        box-shadow: 0 0 12px rgba(97,165,255,0.6);
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
        border-bottom: 10px solid #7be7d9;
    }
    .wind-flow .tail {
        position: absolute;
        width: 10px;
        height: 10px;
        border-radius: 999px;
        background: rgba(123,231,217,0.9);
        box-shadow: 0 0 14px rgba(123,231,217,0.8);
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
    .dash-title {
        font-size: 2.4rem;
        font-weight: 800;
        letter-spacing: -0.02em;
        color: var(--text-primary);
    }
    .overview-title {
        font-size: 1.5rem;
    }
    .dash-clock {
        text-align: right;
        padding: 6px 10px;
        border-radius: 12px;
        border: 1px solid rgba(123,231,217,0.25);
        background: rgba(13,16,22,0.7);
        color: #d8deed;
        font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, "Liberation Mono", monospace;
    }
    .dash-clock .time {
        font-size: 1.1rem;
        font-weight: 700;
    }
    .dash-clock .date {
        font-size: 0.78rem;
        color: var(--text-secondary);
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
        gap: 8px;
        padding: 4px 10px;
        border-radius: 999px;
        border: 1px solid rgba(97,165,255,0.35);
        background: rgba(97,165,255,0.12);
        color: #dbe7ff;
        font-weight: 600;
        font-size: 0.82rem;
    }
    .wind-flag .arrow {
        display: inline-flex;
        align-items: center;
        justify-content: center;
        width: 20px;
        height: 20px;
        border-radius: 50%;
        border: 1px solid rgba(255,255,255,0.2);
        color: #f4f7ff;
        font-size: 0.7rem;
        transform: rotate(0deg);
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
    .header-badges .sun-badge {
        font-size: 0.75rem;
        padding: 4px 8px;
    }
    .sun-badge {
        display: inline-flex;
        align-items: center;
        gap: 8px;
        padding: 4px 10px;
        border-radius: 999px;
        border: 1px solid rgba(242,168,91,0.35);
        background: rgba(242,168,91,0.12);
        color: #f6e0c3;
        font-weight: 600;
        font-size: 0.82rem;
    }
    .sun-badge .sun-icon {
        width: 20px;
        height: 20px;
        border-radius: 50%;
        background: radial-gradient(circle at 30% 30%, #ffe29b, #f2a85b 60%);
        box-shadow: 0 0 10px rgba(242,168,91,0.6);
        position: relative;
    }
    .sun-badge .sun-icon::after {
        content: "";
        position: absolute;
        inset: -4px;
        border-radius: 50%;
        border: 1px dashed rgba(242,168,91,0.45);
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
        background: radial-gradient(circle at 35% 35%, #d7dcff, #7a8cff 65%);
        box-shadow: 0 0 10px rgba(122,140,255,0.6);
        position: relative;
    }
    .sun-badge .moon-icon::after {
        content: "";
        position: absolute;
        inset: -4px;
        border-radius: 50%;
        border: 1px dashed rgba(122,140,255,0.4);
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
    @media (prefers-color-scheme: light) {
        body { background: #f5f7fb; }
        .main { background: #f5f7fb; }
    }
    body[data-theme="light"],
    .stApp[data-theme="light"],
    [data-testid="stAppViewContainer"][data-theme="light"] {
        color: #111827;
        background: #f5f7fb;
    }
    body.theme-light,
    .stApp.theme-light {
        color: #111827;
        background: #f5f7fb;
        --text-primary: #111827;
        --text-secondary: #4b5563;
    }
    body[data-theme="light"] .main,
    .stApp[data-theme="light"] .main,
    [data-testid="stAppViewContainer"][data-theme="light"] .main {
        background: #f5f7fb;
    }
    body.theme-light .main,
    .stApp.theme-light .main {
        background: #f5f7fb;
    }
    body[data-theme="light"] .stMarkdown,
    body[data-theme="light"] .stCaption,
    body[data-theme="light"] .stText,
    body[data-theme="light"] .stSubheader,
    body[data-theme="light"] h1,
    body[data-theme="light"] h2,
    body[data-theme="light"] h3,
    body[data-theme="light"] h4,
    .stApp[data-theme="light"] .stMarkdown,
    .stApp[data-theme="light"] .stCaption,
    .stApp[data-theme="light"] .stText,
    .stApp[data-theme="light"] .stSubheader,
    .stApp[data-theme="light"] h1,
    .stApp[data-theme="light"] h2,
    .stApp[data-theme="light"] h3,
    .stApp[data-theme="light"] h4 {
        color: #111827;
    }
    body.theme-light .stMarkdown,
    body.theme-light .stCaption,
    body.theme-light .stText,
    body.theme-light .stSubheader,
    body.theme-light h1,
    body.theme-light h2,
    body.theme-light h3,
    body.theme-light h4,
    .stApp.theme-light .stMarkdown,
    .stApp.theme-light .stCaption,
    .stApp.theme-light .stText,
    .stApp.theme-light .stSubheader,
    .stApp.theme-light h1,
    .stApp.theme-light h2,
    .stApp.theme-light h3,
    .stApp.theme-light h4 {
        color: #111827;
    }
    body[data-theme="light"] .card,
    .stApp[data-theme="light"] .card {
        background: #ffffff;
        border-color: #dde3ee;
        color: #1b2432;
    }
    body.theme-light .card,
    .stApp.theme-light .card {
        background: #ffffff;
        border-color: #dde3ee;
        color: #1b2432;
    }
    body[data-theme="light"] .chart-header,
    .stApp[data-theme="light"] .chart-header {
        color: #1b2432;
    }
    body.theme-light .chart-header,
    .stApp.theme-light .chart-header { color: #1b2432; }
    body[data-theme="light"] .metric-card,
    .stApp[data-theme="light"] .metric-card {
        background: linear-gradient(160deg, #ffffff, #f2f5fb);
        border-color: #dde3ee;
    }
    body.theme-light .metric-card,
    .stApp.theme-light .metric-card {
        background: linear-gradient(160deg, #ffffff, #f2f5fb);
        border-color: #dde3ee;
    }
    body[data-theme="light"] .metric-card .label,
    .stApp[data-theme="light"] .metric-card .label { color: #58708f; }
    body[data-theme="light"] .metric-card .value,
    .stApp[data-theme="light"] .metric-card .value { color: #1b2432; }
    body[data-theme="light"] .metric-card .sub,
    .stApp[data-theme="light"] .metric-card .sub { color: #5c6b7c; }
    body.theme-light .metric-card .label,
    .stApp.theme-light .metric-card .label { color: #58708f; }
    body.theme-light .metric-card .value,
    .stApp.theme-light .metric-card .value { color: #1b2432; }
    body.theme-light .metric-card .sub,
    .stApp.theme-light .metric-card .sub { color: #5c6b7c; }
    body[data-theme="light"] .metric-expanders [data-testid="stExpander"],
    .stApp[data-theme="light"] .metric-expanders [data-testid="stExpander"] {
        background: #ffffff;
        border-color: #dde3ee;
    }
    body.theme-light .metric-expanders [data-testid="stExpander"],
    .stApp.theme-light .metric-expanders [data-testid="stExpander"] {
        background: #ffffff;
        border-color: #dde3ee;
    }
    body[data-theme="light"] .metric-expanders [data-testid="stExpander"] summary,
    .stApp[data-theme="light"] .metric-expanders [data-testid="stExpander"] summary {
        color: #111827;
    }
    body.theme-light .metric-expanders [data-testid="stExpander"] summary,
    .stApp.theme-light .metric-expanders [data-testid="stExpander"] summary {
        color: #111827;
    }
    body[data-theme="light"] .metric-expanders [data-testid="stExpander"] svg,
    .stApp[data-theme="light"] .metric-expanders [data-testid="stExpander"] svg {
        color: #111827;
        fill: #111827;
    }
    body.theme-light .metric-expanders [data-testid="stExpander"] svg,
    .stApp.theme-light .metric-expanders [data-testid="stExpander"] svg {
        color: #111827;
        fill: #111827;
    }
    body[data-theme="light"] .dash-title,
    body[data-theme="light"] .overview-title,
    .stApp[data-theme="light"] .dash-title,
    .stApp[data-theme="light"] .overview-title {
        color: #111827;
    }
    body.theme-light .dash-title,
    body.theme-light .overview-title,
    .stApp.theme-light .dash-title,
    .stApp.theme-light .overview-title {
        color: #111827 !important;
    }
    body[data-theme="light"] .wind-flag,
    .stApp[data-theme="light"] .wind-flag {
        background: rgba(37,99,235,0.08);
        color: #1f2a44;
        border-color: rgba(37,99,235,0.25);
    }
    body.theme-light .wind-flag,
    .stApp.theme-light .wind-flag {
        background: rgba(37,99,235,0.08);
        color: #1f2a44;
        border-color: rgba(37,99,235,0.25);
    }
    body.theme-light .sun-badge,
    .stApp.theme-light .sun-badge {
        background: rgba(242,168,91,0.15);
        color: #5a3b1d;
        border-color: rgba(242,168,91,0.35);
    }
    body[data-theme="light"] .wind-flag .arrow,
    .stApp[data-theme="light"] .wind-flag .arrow { color: #1f2a44; }
    body.theme-light .wind-flag .arrow,
    .stApp.theme-light .wind-flag .arrow { color: #1f2a44; }
    body[data-theme="light"] .gauge-block,
    .stApp[data-theme="light"] .gauge-block {
        background: #ffffff;
        border-color: #dde3ee;
    }
    body.theme-light .gauge-block,
    .stApp.theme-light .gauge-block {
        background: #ffffff;
        border-color: #dde3ee;
    }
    body[data-theme="light"] .gauge-header,
    .stApp[data-theme="light"] .gauge-header { color: #1b2432; }
    body.theme-light .gauge-header,
    .stApp.theme-light .gauge-header { color: #1b2432; }
    body[data-theme="light"] .gauge-muted,
    .stApp[data-theme="light"] .gauge-muted { color: #5c6b7c; }
    body.theme-light .gauge-muted,
    .stApp.theme-light .gauge-muted { color: #5c6b7c; }
    body[data-theme="light"] .ingest-shell,
    .stApp[data-theme="light"] .ingest-shell { background: #ffffff; border-color: #dde3ee; }
    body.theme-light .ingest-shell,
    .stApp.theme-light .ingest-shell { background: #ffffff; border-color: #dde3ee; }
    body[data-theme="light"] .ingest-chip,
    .stApp[data-theme="light"] .ingest-chip { background: #f4f6fb; border-color: #dde3ee; color: #1b2432; }
    body.theme-light .ingest-chip,
    .stApp.theme-light .ingest-chip { background: #f4f6fb; border-color: #dde3ee; color: #1b2432; }
    body[data-theme="light"] .ingest-meta,
    .stApp[data-theme="light"] .ingest-meta { color: #5c6b7c; }
    body.theme-light .ingest-meta,
    .stApp.theme-light .ingest-meta { color: #5c6b7c; }
    body[data-theme="light"] .ingest-pill,
    .stApp[data-theme="light"] .ingest-pill { color: #1b2432; }
    body.theme-light .ingest-pill,
    .stApp.theme-light .ingest-pill { color: #1b2432; }
    body[data-theme="light"] .info-icon,
    .stApp[data-theme="light"] .info-icon { color: #1f2a44; border-color: rgba(37,99,235,0.35); }
    body.theme-light .info-icon,
    .stApp.theme-light .info-icon { color: #1f2a44; border-color: rgba(37,99,235,0.35); }
    body[data-theme="light"] [data-baseweb="tab"],
    .stApp[data-theme="light"] [data-baseweb="tab"] { color: #3b4252; }
    body.theme-light [data-baseweb="tab"],
    .stApp.theme-light [data-baseweb="tab"] { color: #3b4252; }
    body[data-theme="light"] [data-baseweb="tab"][aria-selected="true"],
    .stApp[data-theme="light"] [data-baseweb="tab"][aria-selected="true"] { color: #111827; }
    body.theme-light [data-baseweb="tab"][aria-selected="true"],
    .stApp.theme-light [data-baseweb="tab"][aria-selected="true"] { color: #111827; }
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
        background: #101722;
        border: 1px solid #1c2432;
        color: #e7ecf3;
        font-weight: 600;
        font-size: 0.85rem;
        min-height: 34px;
    }
    .ingest-dot {
        width: 10px;
        height: 10px;
        border-radius: 999px;
        box-shadow: 0 0 10px currentColor;
    }
    .ingest-chip.ok { border-color: rgba(123,231,217,0.35); }
    .ingest-chip.warn { border-color: rgba(255,209,102,0.35); }
    .ingest-chip.offline { border-color: rgba(255,123,123,0.35); }
    .ingest-chip.standby { border-color: rgba(154,164,181,0.35); }
    .ingest-body {
        display: flex;
        flex-direction: column;
        gap: 2px;
    }
    .ingest-title {
        font-weight: 700;
    }
    .ingest-meta {
        font-size: 0.72rem;
        color: #9aa4b5;
        font-weight: 500;
    }
    .ingest-pill {
        padding: 2px 6px;
        border-radius: 999px;
        font-size: 0.72rem;
        letter-spacing: 0.6px;
        text-transform: uppercase;
        border: 1px solid rgba(255,255,255,0.12);
        color: #cfd6e5;
        margin-left: auto;
    }
    .ingest-pill.ok { color: #7be7d9; border-color: rgba(123,231,217,0.4); }
    .ingest-pill.warn { color: #ffd166; border-color: rgba(255,209,102,0.4); }
    .ingest-pill.offline { color: #ff7b7b; border-color: rgba(255,123,123,0.4); }
    .ingest-pill.standby { color: #9aa4b5; border-color: rgba(154,164,181,0.4); }
    .ingest-help {
        margin-top: 6px;
        color: #9aa4b5;
        font-size: 0.78rem;
    }
    .ingest-divider {
        margin: 10px 0;
        height: 1px;
        background: #1f2635;
        opacity: 0.7;
    }
    .ingest-snapshot {
        margin-top: 6px;
        color: #9aa4b5;
        font-size: 0.78rem;
    }
    .ping-toast {
        margin: 6px 0 2px 0;
        font-size: 0.82rem;
        color: #cfd6e5;
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
        border: 1px solid #1b2332;
        background: #0f1520;
    }
    .ingest-detail-row.warn {
        border-color: rgba(255,209,102,0.45);
        box-shadow: 0 0 0 1px rgba(255,209,102,0.08) inset;
    }
    .ingest-detail-row.offline {
        border-color: rgba(255,123,123,0.45);
        box-shadow: 0 0 0 1px rgba(255,123,123,0.08) inset;
    }
    .ingest-detail-row.alert {
        border-color: rgba(255,123,123,0.55);
        box-shadow: 0 0 0 1px rgba(255,123,123,0.14) inset;
    }
    .ingest-detail-row .meta {
        display: flex;
        align-items: center;
        gap: 10px;
        flex-wrap: wrap;
        font-weight: 700;
        color: #e7ecf3;
    }
    .ingest-detail-row .detail {
        color: #9aa4b5;
        font-size: 0.85rem;
    }
    .ingest-detail-row .last {
        text-align: right;
        color: #9aa4b5;
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
    chart = (
        alt.Chart(data)
        .mark_line(interpolate="monotone", strokeWidth=2)
        .encode(
            x=alt.X("time:T", title="Time"),
            y=alt.Y("value:Q", title=None),
            color=alt.Color("metric:N", legend=alt.Legend(title=None), scale=alt.Scale(scheme=CHART_SCHEME)),
        )
        .properties(height=height)
    )
    if title:
        chart = chart.properties(title=title)

    return (
        chart
        .configure_axis(labelColor="#cfd6e5", titleColor="#cfd6e5", gridColor="#1f252f")
        .configure_legend(labelColor="#cfd6e5", titleColor="#cfd6e5")
        .configure_title(color="#cfd6e5")
    )


def bar_chart(data, height=200, title=None, color="#61a5ff"):
    chart = (
        alt.Chart(data)
        .mark_bar(color=color)
        .encode(
            x=alt.X("label:N", title=None, sort=None),
            y=alt.Y("value:Q", title=None),
        )
        .properties(height=height)
        .configure_axis(labelColor="#cfd6e5", titleColor="#cfd6e5", gridColor="#1f252f")
        .configure_title(color="#cfd6e5")
    )
    if title:
        chart = chart.properties(title=title)
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
        colors = COLLECTOR_COLORS.get(name_key, ("#9aa4b5", "#c4cad6"))

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


def render_ingest_banner(sources, total_recent, avg_latency_text=None, collector_statuses=None):
    if not sources:
        return

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
    overall = "ok" if all(s["status"] in ("ok", "standby") for s in statuses) else "warn"
    header_title = "Signals healthy" if overall == "ok" else "Signals need attention"
    badge_text = f"{total_recent} events/hr" if total_recent else "No recent events"
    if avg_latency_text:
        badge_text = f"{badge_text} - avg data age {avg_latency_text}"

    summary_html = "".join(
        indicator_chip(s["name"], s["status"], s["colors"])
        for s in statuses
    )


    details_html = "".join(
        f"""<div class="ingest-detail-row">
  <div class="meta">
    <span class="ingest-dot" style="background:{s['colors'][0]};"></span>
    <span>{html_escape(s['name'])}</span>
    <span class="ingest-pill {s['status']}">{html_escape(status_labels[s['status']])}</span>
  </div>
  <div class="detail">{html_escape(s['latency_text'])} - {html_escape(s['load_text'])}</div>
  <div class="last">{html_escape(s['last_seen'])}</div>
</div>"""
        for s in statuses
    )
    collector_section_html = ""
    collector_summary_html = ""
    collector_detail_html = ""
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
        snapshot_text = " | ".join(
            s["snapshot_text"] for s in collector_statuses if s.get("snapshot_text")
        )
        snapshot_line = (
            f"<div class=\"ingest-snapshot\">Health snapshot: {html_escape(snapshot_text)}</div>"
            if snapshot_text
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
        collector_detail_html = "".join(
            f"""<div class="{collector_row_class(s['status'], s.get('error_recent'))}">
  <div class="meta">
    <span class="ingest-dot" style="background:{s['colors'][0]};"></span>
    <span>{html_escape(s['name'])}</span>
    <span class="ingest-pill {s['status']}">{html_escape(s.get('pill_text') or status_labels.get(s['status'], 'Live'))}</span>
  </div>
  <div class="detail">{html_escape(s['latency_text'])}</div>
  <div class="last">{html_escape(s['error_text'])}</div>
</div>"""
            for s in collector_statuses
        )


    st.sidebar.subheader("Connection settings")
    with st.sidebar.container():
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

        header_cols = st.columns([1.4, 0.8])
        with header_cols[0]:
            hub_uptime_text = fmt_duration((time.time() - hub_activity["last_epoch"]) if hub_activity.get("last_epoch") else None)
            st.markdown(
                f"""
<div class="ingest-shell hero-glow">
  <div class="ingest-header-row">
    <div>
      <div class="ingest-eyebrow">Connection status</div>
      <div class="ingest-summary">{header_title}</div>
      <div class="ingest-help">Avg data age is the mean time since last packets across sources.</div>
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
            with header_cols[1]:
                st.markdown(
                    f"<div class='ingest-badge'>{html_escape(badge_text)} <span title='Avg data age reflects how long ago each source last reported.'>(i)</span></div>",
                    unsafe_allow_html=True,
                )
                for src in statuses:
                    target = PING_TARGETS.get(src["name"]) or (PING_TARGETS.get("Tempest Hub") if src["name"] == "Tempest Station" else None)
                    disabled = not target
                    if st.button(f"Ping {src['name']}", key=f"ping_{src['name']}", disabled=disabled):
                        ok, msg = ping_device(target)
                        st.session_state.ping_results[src["name"]] = (ok, msg, time.time())
                    result = st.session_state.ping_results.get(src["name"])
                    if result and time.time() - result[2] < 6:
                        ok, msg, _ = result
                        status_label = "OK" if ok else "WARN"
                        st.markdown(
                            f"<div class='ping-toast'>{status_label}: {src['name']} - {msg}</div>",
                            unsafe_allow_html=True,
                        )
                    elif disabled:
                        st.markdown("<div class='ingest-help'>Set ping target in PING_TARGETS.</div>", unsafe_allow_html=True)

            # Detail blocks (no extra ping buttons inside)
            for src in statuses:
                expander_title = f"{src['name']} — {status_labels[src['status']]}"
                with st.expander(expander_title, expanded=False):
                    st.markdown(f"**Status:** {status_labels[src['status']]}")
                    st.markdown(f"**Latency/Load:** {src['latency_text']} - {src['load_text']}")
                    st.markdown(f"**Last seen:** {src['last_seen']}")

            if collector_statuses:
                with st.expander("Collector details", expanded=False):
                    st.markdown(
                        f"<div class='ingest-details'>{collector_detail_html}</div>",
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
        color-scheme: dark;
      }}
      .dash-shell {{
        padding: 10px 6px 20px 6px;
        border-radius: 18px;
        background: radial-gradient(circle at 20% 10%, rgba(97,165,255,0.16), transparent 45%), #0c111a;
        border: 1px solid #1c2434;
        box-shadow: inset 0 0 0 1px rgba(255,255,255,0.02);
      }}
      .grid-stack-item-content {{
        background: linear-gradient(160deg, rgba(22,30,44,0.92), rgba(12,16,24,0.95));
        border-radius: 16px;
        border: 1px solid rgba(110,140,190,0.18);
        box-shadow: 0 20px 40px rgba(0,0,0,0.35);
        color: #e8edf7;
        padding: 14px;
        overflow: hidden;
      }}
      .tile-title {{
        font-size: 0.78rem;
        text-transform: uppercase;
        letter-spacing: 0.12em;
        color: #8aa4c8;
        margin-bottom: 6px;
      }}
      .tile-value {{
        font-size: 1.9rem;
        font-weight: 700;
        color: #f4f7ff;
      }}
      .tile-sub {{
        font-size: 0.82rem;
        color: #9fb2cc;
      }}
      .tile-meta {{
        display: flex;
        gap: 10px;
        flex-wrap: wrap;
        margin-top: 6px;
        font-size: 0.78rem;
        color: #9fb2cc;
      }}
      .tile-pill {{
        padding: 2px 8px;
        border-radius: 999px;
        background: rgba(123,231,217,0.12);
        border: 1px solid rgba(123,231,217,0.3);
        font-size: 0.72rem;
        color: #bfeee6;
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
        color: #9fb2cc;
      }}
      .raw-table {{
        width: 100%;
        border-collapse: collapse;
        font-size: 0.78rem;
      }}
      .raw-table th, .raw-table td {{
        padding: 6px 8px;
        border-bottom: 1px solid rgba(255,255,255,0.06);
        text-align: left;
      }}
      .raw-table th {{
        color: #8aa4c8;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        font-size: 0.7rem;
      }}
      .raw-scroll {{
        max-height: 460px;
        overflow: auto;
        border: 1px solid rgba(255,255,255,0.05);
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
          ctx.strokeStyle = "rgba(255,255,255,0.12)";
          ctx.lineWidth = 1;
          ctx.beginPath();
          ctx.moveTo(pad, pad);
          ctx.lineTo(pad, pad + h);
          ctx.lineTo(pad + w, pad + h);
          ctx.stroke();
          seriesData.forEach((series, idx) => {{
            const color = colors[idx] || "#61a5ff";
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
          ctx.fillStyle = "#9fb2cc";
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

        function drawDial(canvasId, value, minV, maxV, accent) {{
          const canvas = document.getElementById(canvasId);
          if (!canvas) return;
          const {{ ctx, width, height }} = setupCanvas(canvas, 200, 180);
          const cx = width / 2;
          const cy = height * 0.62;
          const radius = Math.min(width, height) * 0.42;
          ctx.clearRect(0, 0, width, height);
          ctx.strokeStyle = "rgba(255,255,255,0.12)";
          ctx.lineWidth = 10;
          ctx.beginPath();
          ctx.arc(cx, cy, radius, Math.PI, 0);
          ctx.stroke();
          const ratio = Math.max(0, Math.min(1, (value - minV) / (maxV - minV)));
          ctx.strokeStyle = accent || "#61a5ff";
          ctx.beginPath();
          ctx.arc(cx, cy, radius, Math.PI, Math.PI + ratio * Math.PI);
          ctx.stroke();
          ctx.fillStyle = "#f4f7ff";
          ctx.font = "700 28px sans-serif";
          ctx.textAlign = "center";
          ctx.fillText(value.toFixed(1), cx, cy);
          ctx.font = "12px sans-serif";
          ctx.fillStyle = "#9fb2cc";
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
          ctx.fillStyle = color || "rgba(123,231,217,0.7)";
          points.forEach((p) => {{
            const x = pad + w * (p.x - minX) / Math.max(1e-6, maxX - minX);
            const y = pad + h - h * (p.y - minY) / Math.max(1e-6, maxY - minY);
            ctx.beginPath();
            ctx.arc(x, y, 3, 0, Math.PI * 2);
            ctx.fill();
          }});
          ctx.fillStyle = "#9fb2cc";
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
          ctx.fillStyle = "rgba(97,165,255,0.12)";
          ctx.fillRect(pad, pad, w, h);
          ctx.fillStyle = "rgba(123,231,217,0.25)";
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
          ctx.fillStyle = "#f4f7ff";
          ctx.beginPath();
          ctx.arc(x, y, 5, 0, Math.PI * 2);
          ctx.fill();
          ctx.fillStyle = "#9fb2cc";
          ctx.font = "12px sans-serif";
          ctx.fillText("Temp", pad, height - 6);
          ctx.save();
          ctx.translate(8, pad + h);
          ctx.rotate(-Math.PI / 2);
          ctx.fillText("Humidity", 0, 0);
          ctx.restore();
        }}

        function renderAll() {{
          drawLine("overviewTempChart", [payload.temp, payload.heat], ["#61a5ff", "#7be7d9"]);
          drawLine("overviewAqiChart", [payload.aqi], ["#c6f36b"]);
          drawLine("trendTempChart", [payload.temp, payload.heat], ["#61a5ff", "#7be7d9"]);
          drawLine("trendAqiChart", [payload.aqi], ["#c6f36b"]);
          drawLine("trendWindChart", [payload.wind, payload.gust], ["#61a5ff", "#f2a85b"]);
          drawLine("compareTempChart", [payload.temp_today, payload.temp_yesterday], ["#61a5ff", "#f2a85b"]);
          drawScatter("compareScatterChart", payload.aqi_wind, "#7be7d9");
          drawComfort("comfortChart", payload.current_temp, payload.current_humidity);
          if (payload.current_wind !== null) {{
            drawDial("windDial", payload.current_wind, 0, 40, "#61a5ff");
          }}
          if (payload.current_pressure !== null) {{
            drawDial("pressureDial", payload.current_pressure, 28, 31, "#f2a85b");
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
# Sidebar controls
# ------------------------
st.sidebar.header("Controls")

if "fast_view" not in st.session_state:
    st.session_state.fast_view = True
fast_view = st.sidebar.toggle(
    "Fast view",
    key="fast_view",
    help="Show Overview only for quicker loads. Turn off for full tabs.",
)

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
    preset_defs = [(6, "6h"), (12, "12h"), (24, "24h"), (168, "7d"), ("all", "All")]
    preset_cols = st.sidebar.columns(len(preset_defs))
    for col, val, label in zip(preset_cols, [p[0] for p in preset_defs], [p[1] for p in preset_defs]):
        if col.button(label):
            if val == "all":
                filter_mode = "All time"
                st.session_state.filter_mode = filter_mode
            else:
                st.session_state.hours = val
                filter_mode = "Window (hours)"
                st.session_state.filter_mode = filter_mode

    if filter_mode == "Window (hours)":
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

if filter_mode == "Custom dates":
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
elif filter_mode == "All time":
    # All time
    since_epoch = 0
    until_epoch = None
    window_desc = "all time"
else:
    # Fallback to window if mode was changed mid-render
    since_epoch = int((pd.Timestamp.utcnow() - pd.Timedelta(hours=st.session_state.hours)).timestamp())
    window_desc = f"the last {st.session_state.hours}h"

st.sidebar.subheader("Theme")
palette_options = {
    "Aurora": {"scheme": "viridis", "accent": "#7be7d9", "accent2": "#61a5ff", "accent3": "#f2a85b"},
    "Solstice": {"scheme": "plasma", "accent": "#ffcc66", "accent2": "#ff8a5c", "accent3": "#6f79ff"},
    "Monsoon": {"scheme": "magma", "accent": "#5eead4", "accent2": "#38bdf8", "accent3": "#f472b6"},
    "Ember": {"scheme": "inferno", "accent": "#f97316", "accent2": "#f43f5e", "accent3": "#facc15"},
}
if "theme_name" not in st.session_state:
    st.session_state.theme_name = "Aurora"
theme_name = st.sidebar.selectbox(
    "Palette",
    list(palette_options.keys()),
    index=list(palette_options.keys()).index(st.session_state.theme_name),
)
st.session_state.theme_name = theme_name
theme = palette_options[theme_name]
accent_override = st.sidebar.color_picker("Accent color", value=theme["accent"])
theme["accent"] = accent_override
CHART_SCHEME = theme["scheme"]
accent_soft = hex_to_rgba(theme["accent"], 0.18)
accent_border = hex_to_rgba(theme["accent"], 0.55)
st.markdown(
    f"""
    <style>
    :root {{
      --accent: {theme['accent']};
      --accent-2: {theme['accent2']};
      --accent-3: {theme['accent3']};
      --accent-soft: {accent_soft};
      --accent-border: {accent_border};
    }}
    </style>
    """,
    unsafe_allow_html=True,
)

st.sidebar.subheader("Location")
if "station_lat" not in st.session_state:
    st.session_state.station_lat = 0.0
if "station_lon" not in st.session_state:
    st.session_state.station_lon = 0.0

tempest_token = os.getenv("TEMPEST_API_TOKEN")
auto_location = fetch_station_location(tempest_token, TEMPEST_STATION_ID) if tempest_token else None
if auto_location and auto_location.get("lat") is not None and auto_location.get("lon") is not None:
    if st.session_state.station_lat in (0.0, None):
        st.session_state.station_lat = float(auto_location["lat"])
    if st.session_state.station_lon in (0.0, None):
        st.session_state.station_lon = float(auto_location["lon"])
    st.sidebar.caption(
        f"Using Tempest station: {auto_location.get('name', 'Tempest Station')} "
        f"({st.session_state.station_lat:.4f}, {st.session_state.station_lon:.4f})"
    )

override_location = st.sidebar.checkbox("Override location", value=False)
if override_location or not auto_location:
    station_lat = st.sidebar.number_input(
        "Latitude",
        min_value=-90.0,
        max_value=90.0,
        value=float(st.session_state.station_lat),
        format="%.4f",
        help="Used for sunrise/sunset times.",
    )
    station_lon = st.sidebar.number_input(
        "Longitude",
        min_value=-180.0,
        max_value=180.0,
        value=float(st.session_state.station_lon),
        format="%.4f",
        help="Used for sunrise/sunset times.",
    )
    st.session_state.station_lat = station_lat
    st.session_state.station_lon = station_lon

gauge_container = st.sidebar.container()

# ------------------------
# Data load and transforms
# ------------------------
now_ts = pd.Timestamp.utcnow()
recent_cutoff_epoch = int((pd.Timestamp.utcnow() - pd.Timedelta(hours=1)).timestamp())
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

if not tempest.empty:
    tempest["time"] = epoch_to_dt(tempest["obs_epoch"])
    tempest["air_temperature_f"] = c_to_f(tempest["air_temperature"])
    tempest["heat_index_f"] = compute_heat_index(
        tempest["air_temperature_f"],
        tempest["relative_humidity"]
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
    rain_total_mm = None
    if "rain_mm" in tempest:
        rain_total_mm = max(0.0, float(tempest["rain_mm"].iloc[-1]) - float(tempest["rain_mm"].iloc[0]))
    lightning_strikes_window = int(tempest["lightning_strike_count"].sum()) if "lightning_strike_count" in tempest else 0
    lightning_48h = 0
    if "lightning_strike_count" in tempest and "obs_epoch" in tempest:
        cutoff_48h = int((pd.Timestamp.utcnow() - pd.Timedelta(hours=48)).timestamp())
        lightning_48h = int(tempest.loc[tempest["obs_epoch"] >= cutoff_48h, "lightning_strike_count"].sum())
    lightning_avg_dist_km = None
    lightning_avg_dist_mi = None
    if "lightning_avg_dist" in tempest:
        nonzero_dist = tempest.loc[tempest["lightning_avg_dist"] > 0, "lightning_avg_dist"]
        if not nonzero_dist.empty:
            lightning_avg_dist_km = float(nonzero_dist.iloc[-1])
            lightning_avg_dist_mi = lightning_avg_dist_km * 0.621371
else:
    lightning_strikes_window = 0
    lightning_48h = 0
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

latency_values = [
    s["latency_minutes"]
    for s in ingest_sources
    if s["latency_minutes"] is not None and s["name"] != "Tempest Hub"
]
avg_latency_minutes = sum(latency_values) / len(latency_values) if latency_values else None
avg_latency_text = latency_label(avg_latency_minutes) if avg_latency_minutes is not None else None
total_recent = sum(s["recent_count"] for s in ingest_sources if s["recent_count"] is not None)

collector_statuses = build_collector_statuses(now_ts)
render_ingest_banner(
    ingest_sources,
    total_recent,
    avg_latency_text=avg_latency_text,
    collector_statuses=collector_statuses,
)

# ------------------------
# Tabs layout: Overview, Trends, Comparisons, Raw
# ------------------------
overview_payload = build_overview_payload(tempest, airlink)
comparison_payload = build_comparison_payload(tempest, airlink)
raw_rows = build_raw_table(tempest, airlink)
current_temp = float(tempest_latest.air_temperature_f) if tempest_latest is not None else None
current_feels = float(tempest_latest.heat_index_f) if tempest_latest is not None else None
current_humidity = float(tempest_latest.relative_humidity) if tempest_latest is not None else None
current_pressure = float(tempest_latest.pressure_inhg) if tempest_latest is not None else None
current_wind = float(tempest_latest.wind_speed_mph) if tempest_latest is not None else None
current_gust = float(tempest_latest.wind_gust_mph) if tempest_latest is not None and "wind_gust_mph" in tempest_latest else None
current_wind_deg = float(tempest_latest.wind_dir_deg) if tempest_latest is not None and "wind_dir_deg" in tempest_latest else None
current_wind_dir = compass_dir(tempest_latest.wind_dir_deg) if tempest_latest is not None and "wind_dir_deg" in tempest_latest else "--"
current_aqi = float(airlink_latest.aqi_pm25) if airlink_latest is not None and pd.notna(airlink_latest.aqi_pm25) else None
current_dew = float(airlink_latest.dew_point_f) if airlink_latest is not None and pd.notna(airlink_latest.dew_point_f) else None
current_lightning = lightning_48h if "lightning_48h" in globals() else 0
current_battery = float(tempest_latest.battery) if tempest_latest is not None and "battery" in tempest_latest else None
current_solar = float(tempest_latest.solar_radiation) if tempest_latest is not None and "solar_radiation" in tempest_latest else None
current_uv = float(tempest_latest.uv) if tempest_latest is not None and "uv" in tempest_latest else None
if tempest is not None and not tempest.empty:
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
sun_times = None
sunrise_local = None
sunset_local = None
day_length = None
if st.session_state.station_lat is not None and st.session_state.station_lon is not None:
    sun_times = fetch_sun_times(
        st.session_state.station_lat,
        st.session_state.station_lon,
        now_local.date().isoformat(),
    )
if sun_times:
    sunrise_local = pd.to_datetime(sun_times.get("sunrise"), utc=True).tz_convert(LOCAL_TZ)
    sunset_local = pd.to_datetime(sun_times.get("sunset"), utc=True).tz_convert(LOCAL_TZ)
    if sunrise_local is not None and sunset_local is not None:
        day_length = (sunset_local - sunrise_local).total_seconds()
is_daytime = False
if sunrise_local and sunset_local:
    is_daytime = sunrise_local <= now_local <= sunset_local
wind_angle = current_wind_deg if current_wind_deg is not None else 0
wind_dir_text = current_wind_dir if current_wind_dir is not None else "--"
wind_chip_text = f"{wind_dir_text} {current_wind_deg:.0f}°" if current_wind_deg is not None else "--"
sun_chip_text = f"{fmt_time(sunrise_local)} · {fmt_time(sunset_local)}"

title_cols = st.columns([5, 1])
with title_cols[0]:
    st.markdown(
        f"""
        <div class='hero-glow'>
          <div class='dash-title'>Tempest Air & Weather</div>
          <div class="header-badges">
            <div class="sun-badge">
              <span class="{ 'sun-icon sunrise-day' if is_daytime else 'moon-icon sunrise-night' }"></span>
              <span>{sun_chip_text}</span>
            </div>
            <div class="wind-flag">
              <span class="arrow" style="transform: rotate({wind_angle}deg);">^</span>
              <span>{wind_chip_text}</span>
            </div>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
with title_cols[1]:
    components.html(
        """
        <script>
        (function() {
          const doc = window.parent && window.parent.document;
          if (!doc) return;

          const styleId = "dash-clock-style";
          if (!doc.getElementById(styleId)) {
            const style = doc.createElement("style");
            style.id = styleId;
            style.textContent = `
              .dash-clock {
                position: fixed;
                left: 18px;
                bottom: 18px;
                z-index: 9999;
                display: inline-block;
                width: fit-content;
                margin: 0;
                text-align: left;
                padding: 6px 10px;
                border-radius: 12px;
                border: 1px solid rgba(123,231,217,0.25);
                background: rgba(13,16,22,0.7);
                color: #f4f7ff;
                font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, "Liberation Mono", monospace;
              }
              .dash-clock .time {
                font-size: 1.1rem;
                font-weight: 700;
              }
              .dash-clock .date {
                font-size: 0.78rem;
                color: #9aa4b5;
              }
              @media (prefers-color-scheme: light) {
                .dash-clock {
                  background: #ffffff;
                  color: #111827;
                  border-color: #c9d4e6;
                }
                .dash-clock .date { color: #4b5563; }
              }
              .dash-clock.light {
                background: #ffffff;
                color: #111827;
                border-color: #c9d4e6;
              }
              .dash-clock.light .date { color: #4b5563; }
            `;
            doc.head.appendChild(style);
          }

          let clock = doc.getElementById("dash-clock");
          if (!clock) {
            clock = doc.createElement("div");
            clock.id = "dash-clock";
            clock.className = "dash-clock";
            clock.innerHTML = `
              <div class="time" id="dash-clock-time">--:--</div>
              <div class="date" id="dash-clock-date">--</div>
            `;
            doc.body.appendChild(clock);
          }

          function applyTheme() {
            try {
              const theme = doc.body.getAttribute("data-theme") || doc.documentElement.getAttribute("data-theme");
              const lightClass = doc.body.classList.contains("theme-light");
              if (theme === "light" || lightClass) {
                clock.classList.add("light");
              } else {
                clock.classList.remove("light");
              }
            } catch (e) {}
          }

          const timeEl = doc.getElementById("dash-clock-time");
          const dateEl = doc.getElementById("dash-clock-date");
          function updateClock() {
            const now = new Date();
            const timeText = now.toLocaleTimeString([], { hour: "numeric", minute: "2-digit", second: "2-digit" });
            const dateText = now.toLocaleDateString([], { weekday: "short", month: "short", day: "numeric" });
            if (timeEl) timeEl.textContent = timeText;
            if (dateEl) dateEl.textContent = dateText;
          }

          updateClock();
          applyTheme();
          setInterval(applyTheme, 2000);
          setInterval(updateClock, 1000);
        })();
        </script>
        """,
        height=0,
    )
dashboard_payload = {
    **overview_payload,
    **comparison_payload,
    "current_temp": current_temp,
    "current_humidity": current_humidity,
    "current_wind": current_wind,
    "current_pressure": current_pressure,
}
tab_labels = ["Overview"] if fast_view else ["Overview", "Trends", "Comparisons", "Raw"]
tabs = st.tabs(tab_labels)

# Overview tab
with tabs[0]:
    st.markdown(
        f"""
        <div class="overview-header">
          <div><span class="dash-title overview-title">Overview</span></div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    if fast_view:
        st.info("Fast view is on. Disable it in the sidebar to see Trends, Comparisons, and Raw.")
    aqi_delta = delta_over_window(airlink["aqi_pm25"]) if airlink is not None and not airlink.empty and "aqi_pm25" in airlink else None

    def metric_label(label, value, unit="", sub=None):
        base = f"{label}: {value}{unit}" if value is not None else f"{label}: --"
        return f"{base} ({sub})" if sub else base

    metrics = [
        {
            "key": "temp",
            "label": "Temperature",
            "value": fmt_value(current_temp),
            "unit": " F",
            "delta": None if tempest_temp_delta is None else f"{tempest_temp_delta:+.1f} vs start",
            "df": tempest[["time", "air_temperature_f", "heat_index_f"]].melt(
                id_vars=["time"], value_vars=["air_temperature_f", "heat_index_f"], var_name="metric"
            ).assign(metric=lambda d: d["metric"].map({"air_temperature_f": "Air Temperature", "heat_index_f": "Heat Index"})) if tempest is not None and not tempest.empty else None,
        },
        {
            "key": "humidity",
            "label": "Humidity",
            "value": fmt_value(current_humidity, "{:.0f}"),
            "unit": "%",
            "delta": None if tempest_hum_delta is None else f"{tempest_hum_delta:+.0f} vs start",
            "df": tempest[["time", "relative_humidity"]].rename(columns={"relative_humidity": "value"}).assign(metric="Humidity") if tempest is not None and not tempest.empty else None,
        },
        {
            "key": "pressure",
            "label": "Pressure",
            "value": fmt_value(current_pressure, "{:.2f}"),
            "unit": " inHg",
            "delta": None if tempest_pressure_delta is None else f"{tempest_pressure_delta:+.2f} vs start",
            "df": tempest[["time", "pressure_inhg"]].rename(columns={"pressure_inhg": "value"}).assign(metric="Pressure") if tempest is not None and not tempest.empty else None,
        },
        {
            "key": "wind",
            "label": "Wind",
            "value": fmt_value(current_wind),
            "unit": " mph",
            "delta": f"Gust {fmt_value(current_gust)} / {current_wind_dir}",
            "df": tempest[["time", "wind_speed_mph", "wind_gust_mph"]].melt(
                id_vars=["time"], value_vars=["wind_speed_mph", "wind_gust_mph"], var_name="metric", value_name="value"
            ).assign(metric=lambda d: d["metric"].map({"wind_speed_mph": "Wind Speed", "wind_gust_mph": "Gust"})) if tempest is not None and not tempest.empty else None,
        },
        {
            "key": "aqi",
            "label": "AQI (PM2.5)",
            "value": fmt_value(current_aqi, "{:.0f}"),
            "unit": "",
            "delta": None if aqi_delta is None else f"{aqi_delta:+.0f} vs start",
            "df": airlink[["time", "aqi_pm25"]].rename(columns={"aqi_pm25": "value"}).assign(metric="AQI") if airlink is not None and not airlink.empty else None,
        },
        {
            "key": "rain",
            "label": "Rain (window)",
            "value": fmt_value(rain_total_mm, "{:.1f}"),
            "unit": " mm",
            "delta": window_desc,
            "df": tempest[["time", "rain_mm"]].rename(columns={"rain_mm": "value"}).assign(metric="Rain") if tempest is not None and not tempest.empty and "rain_mm" in tempest else None,
        },
        {
            "key": "lightning",
            "label": "Lightning (48h)",
            "value": str(current_lightning),
            "unit": "",
            "delta": "strikes",
            "df": tempest[["time", "lightning_strike_count"]].rename(columns={"lightning_strike_count": "value"}).assign(metric="Lightning") if tempest is not None and not tempest.empty and "lightning_strike_count" in tempest else None,
        },
        {
            "key": "battery",
            "label": "Battery",
            "value": fmt_value(current_battery, "{:.2f}"),
            "unit": " V",
            "delta": None,
            "df": tempest[["time", "battery"]].rename(columns={"battery": "value"}).assign(metric="Battery") if tempest is not None and not tempest.empty and "battery" in tempest else None,
        },
        {
            "key": "solar",
            "label": "Solar Radiation",
            "value": fmt_value(current_solar, "{:.0f}"),
            "unit": " W/m2",
            "delta": None,
            "df": tempest[["time", "solar_radiation"]].rename(columns={"solar_radiation": "value"}).assign(metric="Solar Radiation") if tempest is not None and not tempest.empty and "solar_radiation" in tempest else None,
        },
        {
            "key": "uv",
            "label": "UV Index",
            "value": fmt_value(current_uv, "{:.1f}"),
            "unit": "",
            "delta": None,
            "df": tempest[["time", "uv"]].rename(columns={"uv": "value"}).assign(metric="UV") if tempest is not None and not tempest.empty and "uv" in tempest else None,
        },
    ]

    metrics_by_key = {m["key"]: m for m in metrics}
    options = [m["key"] for m in metrics]
    labels = {m["key"]: metric_label(m["label"], m["value"], m["unit"], m["delta"]) for m in metrics}
    if "overview_order" not in st.session_state:
        st.session_state["overview_order"] = options

    order_selection = st.multiselect(
        "Overview order (top-to-bottom)",
        options=options,
        default=[k for k in st.session_state["overview_order"] if k in options],
        format_func=lambda k: labels.get(k, k),
        help="Drag to reorder or deselect metrics. Renders top-to-bottom for mobile readability.",
    )
    st.session_state["overview_order"] = order_selection

    st.markdown("<div class='metric-expanders'>", unsafe_allow_html=True)
    if not order_selection:
        st.info("Select at least one metric to display.")
    else:
        for key in order_selection:
            metric = metrics_by_key.get(key)
            if not metric:
                continue
            st.metric(metric["label"], metric["value"], metric["delta"])
            if metric["df"] is not None:
                if metric.get("key") == "aqi" and "value" in metric["df"]:
                    avg_val = float(metric["df"]["value"].mean())
                    base_chart = (
                        alt.Chart(metric["df"])
                        .mark_line(interpolate="monotone", strokeWidth=2)
                        .encode(
                            x=alt.X("time:T", title="Time"),
                            y=alt.Y("value:Q", title=None),
                            color=alt.Color(
                                "metric:N",
                                legend=alt.Legend(title=None),
                                scale=alt.Scale(scheme=CHART_SCHEME),
                            ),
                        )
                    )
                    avg_df = pd.DataFrame({"avg": [avg_val], "label": [f"Avg {avg_val:.0f}"]})
                    avg_rule = alt.Chart(avg_df).mark_rule(color="#f2a85b", strokeDash=[4, 4]).encode(y="avg:Q")
                    avg_label = (
                        alt.Chart(avg_df)
                        .mark_text(align="left", dx=6, dy=-6, color="#f2a85b")
                        .encode(y="avg:Q", text="label:N")
                    )
                    layered = (
                        alt.layer(base_chart, avg_rule, avg_label)
                        .properties(height=220)
                        .configure_axis(labelColor="#cfd6e5", titleColor="#cfd6e5", gridColor="#1f252f")
                        .configure_legend(labelColor="#cfd6e5", titleColor="#cfd6e5")
                        .configure_title(color="#cfd6e5")
                        .interactive()
                    )
                    st.altair_chart(layered, width="stretch")
                else:
                    st.altair_chart(
                        clean_chart(metric["df"], height=220, title=None),
                        width="stretch",
                    )
            else:
                st.info("No data available for this metric in the selected window.")
    st.markdown("</div>", unsafe_allow_html=True)


    # Trends tab

if not fast_view:
    with tabs[1]:
        st.subheader("Trends")
        chart_specs = []

        if tempest is not None and not tempest.empty:
            def render_temp_heat():
                temp_long = tempest.melt(
                    id_vars=["time"],
                    value_vars=["air_temperature_f", "heat_index_f"],
                    var_name="metric",
                )
                temp_long["metric"] = temp_long["metric"].map(
                    {"air_temperature_f": "Air Temperature", "heat_index_f": "Heat Index"}
                )
                st.markdown(
                    "<div class='chart-header'>Temperature vs Heat Index"
                    "<span class='info-icon' title='Core temperature trend alongside perceived heat index.'>i</span></div>",
                    unsafe_allow_html=True,
                )
                st.altair_chart(clean_chart(temp_long, height=280, title=None), width="stretch")

            chart_specs.append({"key": "temp_heat", "label": "Temperature vs Heat Index", "render": render_temp_heat})

            def render_wind():
                wind_long = tempest.melt(
                    id_vars=["time"],
                    value_vars=["wind_speed_mph", "wind_gust_mph"],
                    var_name="metric",
                    value_name="value",
                )
                wind_long["metric"] = wind_long["metric"].map(
                    {"wind_speed_mph": "Wind Speed", "wind_gust_mph": "Gust"}
                )
                st.markdown(
                    "<div class='chart-header'>Wind Speed & Gust"
                    "<span class='info-icon' title='Sustained wind versus gust peaks over the selected window.'>i</span></div>",
                    unsafe_allow_html=True,
                )
                st.altair_chart(clean_chart(wind_long, height=240, title=None), width="stretch")

            chart_specs.append({"key": "wind", "label": "Wind Speed & Gust", "render": render_wind})

            def render_pressure():
                pressure_long = tempest[["time", "pressure_inhg"]].rename(columns={"pressure_inhg": "value"})
                pressure_long["metric"] = "Pressure (inHg)"
                st.markdown(
                    "<div class='chart-header'>Pressure Trend"
                    "<span class='info-icon' title='Barometric pressure changes indicate approaching or clearing systems.'>i</span></div>",
                    unsafe_allow_html=True,
                )
                st.altair_chart(clean_chart(pressure_long, height=240, title=None), width="stretch")

            chart_specs.append({"key": "pressure", "label": "Pressure Trend", "render": render_pressure})

            if "pressure_inhg" in tempest:
                def render_pressure_rate():
                    pressure_rate = tempest[["time", "pressure_inhg"]].copy()
                    pressure_rate["hours"] = pressure_rate["time"].diff().dt.total_seconds() / 3600
                    pressure_rate["value"] = pressure_rate["pressure_inhg"].diff() / pressure_rate["hours"]
                    pressure_rate["metric"] = "Pressure Change (inHg/hr)"
                    pressure_rate = pressure_rate.dropna()
                    if pressure_rate.empty:
                        st.info("No pressure change data in window.")
                        return
                    st.markdown(
                        "<div class='chart-header'>Pressure Change Rate"
                        "<span class='info-icon' title='Rate of pressure change per hour; faster drops can signal storms.'>i</span></div>",
                        unsafe_allow_html=True,
                    )
                    st.altair_chart(
                        clean_chart(pressure_rate[["time", "value", "metric"]], height=220, title=None),
                        width="stretch",
                    )

                chart_specs.append({"key": "pressure_rate", "label": "Pressure Change Rate", "render": render_pressure_rate})

            if "battery" in tempest:
                def render_battery():
                    battery_long = tempest[["time", "battery"]].rename(columns={"battery": "value"})
                    battery_long["metric"] = "Battery (V)"
                    st.markdown(
                        "<div class='chart-header'>Battery Trend"
                        "<span class='info-icon' title='Battery voltage trend to catch power drops early.'>i</span></div>",
                        unsafe_allow_html=True,
                    )
                    st.altair_chart(clean_chart(battery_long, height=220, title=None), width="stretch")

                chart_specs.append({"key": "battery", "label": "Battery Trend", "render": render_battery})

            if "rain_mm" in tempest:
                def render_rain():
                    rain_long = tempest[["time", "rain_mm"]].rename(columns={"rain_mm": "value"})
                    rain_long["metric"] = "Rain Accumulation (mm)"
                    rain_chart = (
                    alt.Chart(rain_long)
                    .mark_line(interpolate="step-after")
                    .encode(
                        x=alt.X("time:T", title="Time"),
                        y=alt.Y("value:Q", title="Rain (mm)"),
                    )
                        .properties(height=220)
                        .interactive()
                        .configure_axis(labelColor="#cfd6e5", titleColor="#cfd6e5", gridColor="#1f252f")
                        .configure_title(color="#cfd6e5")
                    )
                    st.markdown(
                        "<div class='chart-header'>Rain Accumulation"
                        "<span class='info-icon' title='Cumulative rain over the selected window.'>i</span></div>",
                        unsafe_allow_html=True,
                    )
                    st.altair_chart(rain_chart, width="stretch")

                chart_specs.append({"key": "rain", "label": "Rain Accumulation", "render": render_rain})

            if "wind_dir_deg" in tempest:
                def render_wind_dir():
                    def bin_dir(deg):
                        if pd.isna(deg):
                            return None
                        return compass_dir(deg)

                    dir_counts = tempest["wind_dir_deg"].apply(bin_dir).value_counts().reset_index()
                    dir_counts.columns = ["label", "value"]
                    if dir_counts.empty:
                        st.info("No wind direction data in window.")
                        return
                    st.markdown(
                        "<div class='chart-header'>Wind Direction Frequency"
                        "<span class='info-icon' title='Dominant wind directions during the window.'>i</span></div>",
                        unsafe_allow_html=True,
                    )
                    st.altair_chart(bar_chart(dir_counts, height=200, title=None, color="#61a5ff"), width="stretch")

                chart_specs.append({"key": "wind_dir", "label": "Wind Direction Frequency", "render": render_wind_dir})

            if "solar_radiation" in tempest and "uv" in tempest:
                def render_solar_uv():
                    solar_uv = tempest.melt(
                        id_vars=["time"],
                        value_vars=["solar_radiation", "uv"],
                        var_name="metric",
                        value_name="value",
                    )
                    solar_uv["metric"] = solar_uv["metric"].map(
                        {"solar_radiation": "Solar Radiation (W/m?)", "uv": "UV Index"}
                    )
                    st.markdown(
                        "<div class='chart-header'>Solar Radiation & UV"
                        "<span class='info-icon' title='Sunlight intensity and UV index across the window.'>i</span></div>",
                        unsafe_allow_html=True,
                    )
                    st.altair_chart(clean_chart(solar_uv, height=240, title=None), width="stretch")

                chart_specs.append({"key": "solar_uv", "label": "Solar Radiation & UV", "render": render_solar_uv})

        if airlink is not None and not airlink.empty:
            def render_aqi():
                st.subheader("?? Outdoor ? AirLink")
                aqi_long = airlink[["time", "aqi_pm25"]].rename(columns={"aqi_pm25": "value"})
                aqi_long["metric"] = "AQI (PM2.5)"
                st.markdown(
                    "<div class='chart-header'>AQI Over Time"
                    "<span class='info-icon' title='Tracks PM2.5 air quality index across the selected window.'>i</span></div>",
                    unsafe_allow_html=True,
                )
                st.altair_chart(clean_chart(aqi_long, height=240, title=None), width="stretch")

            chart_specs.append({"key": "aqi", "label": "AQI Over Time", "render": render_aqi})

            def render_pm_components():
                pm_long = airlink.melt(
                    id_vars=["time"],
                    value_vars=["pm_1", "pm_2p5", "pm_10"],
                    var_name="metric",
                    value_name="value",
                )
                pm_long["metric"] = pm_long["metric"].map({"pm_1": "PM1", "pm_2p5": "PM2.5", "pm_10": "PM10"})
                st.markdown(
                    "<div class='chart-header'>PM Components"
                    "<span class='info-icon' title='Breakdown of particulate sizes to compare PM1/PM2.5/PM10.'>i</span></div>",
                    unsafe_allow_html=True,
                )
                st.altair_chart(clean_chart(pm_long, height=240, title=None), width="stretch")

            chart_specs.append({"key": "pm_components", "label": "PM Components", "render": render_pm_components})

        if chart_specs:
            options = [spec["key"] for spec in chart_specs]
            labels = {spec["key"]: spec["label"] for spec in chart_specs}
            saved_order = st.session_state.get("chart_order_trends", [])
            default_order = [k for k in saved_order if k in options] + [k for k in options if k not in saved_order]
            selection = st.multiselect(
                "Choose chart order",
                options=options,
                default=default_order,
                format_func=lambda k: labels[k],
                help="Drag to reorder or deselect to hide charts.",
            )
            st.session_state["chart_order_trends"] = selection
            for key in selection:
                spec = next((s for s in chart_specs if s["key"] == key), None)
                if spec:
                    spec["render"]()
        else:
            st.info("No chartable data in this window.")

if not fast_view:
    # Comparisons tab
    with tabs[2]:
        st.subheader("Comparisons")
        if tempest is not None and not tempest.empty:
            compare = tempest.copy()
            compare["date"] = compare["time"].dt.date
            latest_date = compare["date"].max()
            yesterday_date = latest_date - pd.Timedelta(days=1)
            compare = compare[compare["date"].isin([latest_date, yesterday_date])]
            compare["day"] = compare["date"].apply(lambda d: "Today" if d == latest_date else "Yesterday")
            temp_compare = compare[["time", "air_temperature_f", "day"]].rename(columns={"air_temperature_f": "value"})
            temp_compare["metric"] = temp_compare["day"]
            st.markdown(
                "<div class='chart-header'>Today vs Yesterday (Temp)"
                "<span class='info-icon' title='Overlay of today vs yesterday temperature patterns.'>i</span></div>",
                unsafe_allow_html=True,
            )
            st.altair_chart(
                clean_chart(temp_compare, height=260, title=None),
                width="stretch",
            )

        if airlink is not None and not airlink.empty and tempest is not None and not tempest.empty:
            merged = pd.merge_asof(
                airlink.sort_values("time"),
                tempest.sort_values("time"),
                on="time",
                direction="nearest",
            )
            scatter = merged[["wind_speed_mph", "aqi_pm25"]].dropna()
            if not scatter.empty:
                chart = (
                    alt.Chart(scatter)
                    .mark_circle(size=60)
                    .encode(
                        x=alt.X("wind_speed_mph", title="Wind Speed (mph)"),
                        y=alt.Y("aqi_pm25", title="AQI (PM2.5)"),
                    color=alt.Color(
                        "aqi_pm25:Q",
                        scale=alt.Scale(scheme="turbo"),
                        legend=alt.Legend(title="AQI"),
                    ),
                )
                    .properties(height=240)
                    .interactive()
                )
                st.markdown(
                    "<div class='chart-header'>AQI vs Wind"
                    "<span class='info-icon' title='Shows how air quality shifts with wind speed.'>i</span></div>",
                    unsafe_allow_html=True,
                )
                st.altair_chart(chart, width="stretch")

            comfort = merged[["air_temperature_f", "relative_humidity"]].dropna()
            if not comfort.empty:
                def comfort_bucket(row):
                    temp = row["air_temperature_f"]
                    hum = row["relative_humidity"]
                    if temp >= 78 and hum >= 60:
                        return "Hot"
                    if hum >= 65:
                        return "Humid"
                    if hum <= 35:
                        return "Dry"
                    return "Comfortable"

                comfort = comfort.copy()
                comfort["comfort"] = comfort.apply(comfort_bucket, axis=1)
                comfort_chart = (
                    alt.Chart(comfort)
                    .mark_circle(size=50, color="#61a5ff")
                    .encode(
                        x=alt.X("air_temperature_f", title="Temp (F)"),
                        y=alt.Y("relative_humidity", title="Humidity (%)"),
                    color=alt.Color(
                        "comfort:N",
                        scale=alt.Scale(
                            domain=["Comfortable", "Dry", "Humid", "Hot"],
                            range=["#7be7d9", "#61a5ff", "#f2a85b", "#ef565f"],
                        ),
                        legend=alt.Legend(title="Comfort"),
                    ),
                )
                    .properties(height=240)
                    .interactive()
                )
                st.markdown(
                    "<div class='chart-header'>Comfort Scatter"
                    "<span class='info-icon' title='Clusters conditions into Comfortable/Dry/Humid/Hot buckets.'>i</span></div>",
                    unsafe_allow_html=True,
                )
                st.altair_chart(comfort_chart, width="stretch")

if not fast_view:
    # Raw tab
    with tabs[3]:
        st.subheader("Raw")
        storage = get_storage_stats()
        total_size = storage["db_size"] + storage["assets_size"]
        st.markdown(
            f"""
            <div class="gauge-block">
              <div class="gauge-header">
                <div>Storage Health</div>
                <div>{fmt_bytes(total_size)} total</div>
              </div>
              <div class="gauge-muted">
                Database: {fmt_bytes(storage["db_size"])} · Assets: {fmt_bytes(storage["assets_size"])}
              </div>
              <div class="gauge-muted">
                Rows stored: {storage["total_rows"]:,} · Measurements: {storage["measurements"]:,}
              </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        raw_limit = 500
        raw_tabs = st.tabs(["Tempest Station", "AirLink", "Tempest Raw", "AirLink Raw", "Hub Raw"])

        with raw_tabs[0]:
            tempest_raw = load_df(
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
            if tempest_raw.empty:
                st.info("No Tempest data in selected window.")
            else:
                if "obs_epoch" in tempest_raw:
                    tempest_raw["time"] = epoch_to_dt(tempest_raw["obs_epoch"])
                if "air_temperature" in tempest_raw:
                    tempest_raw["air_temperature_f"] = c_to_f(tempest_raw["air_temperature"])
                if "station_pressure" in tempest_raw:
                    tempest_raw["pressure_inhg"] = hpa_to_inhg(tempest_raw["station_pressure"])
                if "wind_avg" in tempest_raw:
                    tempest_raw["wind_speed_mph"] = mps_to_mph(tempest_raw["wind_avg"])
                if "wind_gust" in tempest_raw:
                    tempest_raw["wind_gust_mph"] = mps_to_mph(tempest_raw["wind_gust"])
                if "rain_accumulated" in tempest_raw:
                    tempest_raw["rain_mm"] = tempest_raw["rain_accumulated"].astype(float)
                st.caption(f"Showing latest {min(raw_limit, len(tempest_raw))} rows.")
                st.dataframe(tempest_raw, width="stretch")

        with raw_tabs[1]:
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
            else:
                airlink_obs_raw = pd.DataFrame()
            if airlink_obs_raw.empty:
                st.info("No AirLink data in selected window.")
            else:
                if "ts" in airlink_obs_raw:
                    airlink_obs_raw["time"] = epoch_to_dt(airlink_obs_raw["ts"])
                st.caption(f"Showing latest {min(raw_limit, len(airlink_obs_raw))} rows.")
                st.dataframe(airlink_obs_raw, width="stretch")

        with raw_tabs[2]:
            if RAW_EVENTS_TABLE:
                tempest_events = load_df(
                    f"""
                    SELECT *
                    FROM {RAW_EVENTS_TABLE}
                    WHERE received_at_epoch >= :since
                    ORDER BY received_at_epoch DESC
                    LIMIT :limit
                    """,
                    {
                        "since": since_epoch,
                        "limit": raw_limit,
                    },
                )
            else:
                tempest_events = pd.DataFrame()
            if tempest_events.empty:
                st.info("No Tempest raw events in selected window.")
            else:
                if "received_at_epoch" in tempest_events:
                    tempest_events["received_at_time"] = epoch_to_dt(tempest_events["received_at_epoch"])
                st.caption(f"Showing latest {min(raw_limit, len(tempest_events))} rows.")
                st.dataframe(tempest_events, width="stretch")

        with raw_tabs[3]:
            if AIRLINK_RAW_TABLE:
                airlink_raw = load_df(
                    f"""
                    SELECT *
                    FROM {AIRLINK_RAW_TABLE}
                    WHERE received_at_epoch >= :since
                    ORDER BY received_at_epoch DESC
                    LIMIT :limit
                    """,
                    {
                        "since": since_epoch,
                        "limit": raw_limit,
                    },
                )
            else:
                airlink_raw = pd.DataFrame()
            if airlink_raw.empty:
                st.info("No AirLink raw payloads in selected window.")
            else:
                if "received_at_epoch" in airlink_raw:
                    airlink_raw["received_at_time"] = epoch_to_dt(airlink_raw["received_at_epoch"])
                st.caption(f"Showing latest {min(raw_limit, len(airlink_raw))} rows.")
                st.dataframe(airlink_raw, width="stretch")

        with raw_tabs[4]:
            if RAW_EVENTS_TABLE:
                hub_raw = load_df(
                    f"""
                    SELECT *
                    FROM {RAW_EVENTS_TABLE}
                    WHERE received_at_epoch >= :since
                      AND (
                        device_id = :hub_id
                        OR message_type IN ("connection_opened", "ack")
                      )
                    ORDER BY received_at_epoch DESC
                    LIMIT :limit
                    """,
                    {
                        "since": since_epoch,
                        "limit": raw_limit,
                        "hub_id": TEMPEST_HUB_ID,
                    },
                )
            else:
                hub_raw = pd.DataFrame()
            if hub_raw.empty:
                st.info("No Hub raw events in selected window.")
            else:
                if "received_at_epoch" in hub_raw:
                    hub_raw["received_at_time"] = epoch_to_dt(hub_raw["received_at_epoch"])
                st.caption(f"Showing latest {min(raw_limit, len(hub_raw))} rows.")
                st.dataframe(hub_raw, width="stretch")
