import sqlite3
import json
import subprocess
import time
from datetime import datetime
import requests
from pathlib import Path
import os
from PIL import Image
import numpy as np
import xml.etree.ElementTree as ET
import io
import zipfile
try:
    import pytesseract
    from pytesseract import Output as TesseractOutput
except Exception:
    pytesseract = None
    TesseractOutput = None

import altair as alt
import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

DB_PATH = "data/tempest.db"
UI_SKIN_STATE_PATH = Path("data/ui_skin.json")
TEMPEST_STATION_ID = 475329
TEMPEST_HUB_ID = 475327

SEQUENCE_BUILDER_PATH = Path("static/sequence_store.json")
SEQUENCE_DB_PATH = Path("data/sprite_sequences.db")

PING_TARGETS = {
    "AirLink": "192.168.1.19",
    "Tempest Hub": "192.168.1.26",
}

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

st.set_page_config(
    page_title="Weather & Air Quality Dashboard",
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
      const RESTORE_KEY = "tempest:restored_tab";
      const FORCE_KEY = "tempest:force_tab";
      function tabButtons() {
        return Array.from(window.parent.document.querySelectorAll('button[role="tab"]'));
      }
      function logSprite(msg) {
        try {
          const log = window.parent.localStorage || window.localStorage;
          const key = "sprite:tab_log";
          const stamp = new Date().toLocaleTimeString();
          const existing = (log.getItem(key) || "").split("\n").filter(Boolean);
          existing.unshift("[" + stamp + "] " + msg);
          log.setItem(key, existing.slice(0, 8).join("\n"));
        } catch (e) {}
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
            logSprite("Tab click -> " + btn.textContent.trim());
          });
        });
        if (!storage.getItem(RESTORE_KEY)) {
          const fileInputs = window.parent.document.querySelectorAll('input[type="file"]');
          const hasFiles = Array.from(fileInputs).some((input) => input.files && input.files.length);
          if (hasFiles) {
            storage.setItem(RESTORE_KEY, "1");
            return true;
          }
          const forced = storage.getItem(FORCE_KEY);
          const desired = forced || storage.getItem(TAB_KEY);
          if (desired) {
            const match = tabs.find((btn) => btn.textContent.trim() === desired);
            if (match) {
              storage.setItem(RESTORE_KEY, "1");
              storage.removeItem(FORCE_KEY);
              logSprite("Restore tab -> " + desired);
              match.click();
              return true;
            }
          }
          storage.setItem(RESTORE_KEY, "1");
        }
        return true;
      }
      function bindFileInputs() {
        const inputs = Array.from(window.parent.document.querySelectorAll('input[type="file"]'));
        inputs.forEach((input) => {
          if (input.dataset.tabBound) return;
          input.dataset.tabBound = "1";
          input.addEventListener("change", () => {
            const label = activeTabLabel() || "Sprite Lab";
            storage.setItem(TAB_KEY, label);
            storage.setItem(FORCE_KEY, "Sprite Lab");
            logSprite("File selected, forcing Sprite Lab");
            const tabs = tabButtons();
            const spriteTab = tabs.find((btn) => btn.textContent.trim() === "Sprite Lab");
            if (spriteTab) {
              spriteTab.click();
            }
          });
        });
      }
      let tries = 0;
      const timer = window.parent.setInterval(() => {
        tries += 1;
        attachTabHandlers();
        bindFileInputs();
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
    }
    .ingest-chip .dot {
        width: 10px;
        height: 10px;
        border-radius: 999px;
        box-shadow: 0 0 10px currentColor;
    }
    .ingest-chip.ok { color: #7be7d9; }
    .ingest-chip.warn { color: #ffd166; }
    .ingest-chip.offline { color: #ff7b7b; }
    .ingest-chip.standby { color: #9aa4b5; }
    .ingest-pill {
        padding: 2px 6px;
        border-radius: 999px;
        font-size: 0.72rem;
        letter-spacing: 0.6px;
        text-transform: uppercase;
        border: 1px solid rgba(255,255,255,0.12);
        color: #cfd6e5;
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
    /* Connection details toggle (uiverse-inspired) */
    .conn-toggle {
        --toggle-w: 115px;
        --toggle-h: 55px;
        --toggle-pad: 6px;
        --toggle-knob: 42px;
    }
    .conn-toggle [data-testid="stCheckbox"] label {
        position: relative;
        display: flex;
        align-items: center;
        justify-content: space-between;
        gap: 12px;
        padding-right: calc(var(--toggle-w) + 12px);
        min-height: var(--toggle-h);
        cursor: pointer;
    }
    .conn-toggle [data-testid="stCheckbox"] label > div {
        display: none;
    }
    .conn-toggle [data-testid="stCheckbox"] input {
        position: absolute;
        opacity: 0;
        appearance: none;
    }
    .conn-toggle [data-testid="stCheckbox"] label::before {
        content: "";
        position: absolute;
        right: 0;
        top: 50%;
        transform: translateY(-50%);
        width: var(--toggle-w);
        height: var(--toggle-h);
        border-radius: 165px;
        background: #252532;
        box-shadow: inset 0px 5px 10px 0px #16151c, 0px 3px 6px -2px #403f4e;
        border: 1px solid #32303e;
    }
    .conn-toggle [data-testid="stCheckbox"] label::after {
        content: "";
        position: absolute;
        right: calc(var(--toggle-w) - var(--toggle-knob) - var(--toggle-pad));
        top: 50%;
        transform: translateY(-50%);
        width: var(--toggle-knob);
        height: var(--toggle-knob);
        border-radius: 100%;
        background: linear-gradient(#3b3a4e, #272733);
        box-shadow: inset 0px 5px 4px 0px #424151, 0px 4px 15px 0px #0f0e17;
        transition: right 0.3s ease-in;
        z-index: 2;
    }
    .conn-toggle [data-testid="stCheckbox"] label:has(input:checked)::after {
        right: var(--toggle-pad);
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
    .ingest-detail-row .meta {
        display: flex;
        align-items: center;
        gap: 10px;
        font-weight: 700;
        color: #e7ecf3;
    }
    .ingest-detail-row .meta .dot {
        width: 10px;
        height: 10px;
        border-radius: 999px;
        box-shadow: 0 0 10px currentColor;
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
def init_sequence_db():
    SEQUENCE_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(SEQUENCE_DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS sequences (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE,
            payload_json TEXT,
            updated_at TEXT
        )
        """
    )
    conn.commit()
    conn.close()


def list_sequences():
    init_sequence_db()
    conn = sqlite3.connect(SEQUENCE_DB_PATH)
    df = pd.read_sql_query("SELECT id, name, updated_at FROM sequences ORDER BY updated_at DESC", conn)
    conn.close()
    return df


def save_sequence(name: str, payload: dict):
    init_sequence_db()
    conn = sqlite3.connect(SEQUENCE_DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "INSERT OR REPLACE INTO sequences (id, name, payload_json, updated_at) VALUES ((SELECT id FROM sequences WHERE name=?), ?, ?, ?)",
        (name, name, json.dumps(payload), pd.Timestamp.utcnow().isoformat()),
    )
    conn.commit()
    conn.close()


def load_sequence_payload(name: str):
    init_sequence_db()
    conn = sqlite3.connect(SEQUENCE_DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT payload_json FROM sequences WHERE name=?", (name,))
    row = cur.fetchone()
    conn.close()
    if not row:
        return None
    try:
        return json.loads(row[0])
    except Exception:
        return None

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
        "sprite_sheets": 0,
        "sequences": 0,
        "measurements": 0,
    }
    try:
        db_path = Path(DB_PATH)
        if db_path.exists():
            stats["db_size"] = db_path.stat().st_size
    except Exception:
        pass

    total_assets = 0
    for root in [Path("images"), Path("static"), SEQUENCE_DB_PATH]:
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

    try:
        manifest_path = Path("static/sprite_manifest.json")
        if manifest_path.exists():
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            sheets = manifest.get("sheets", {}) if isinstance(manifest, dict) else {}
            stats["sprite_sheets"] = len(sheets)
    except Exception:
        pass

    try:
        init_sequence_db()
        conn = sqlite3.connect(SEQUENCE_DB_PATH)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(1) FROM sequences")
        stats["sequences"] = int(cur.fetchone()[0])
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


def clean_chart(data, height=240, title=None):
    chart = (
        alt.Chart(data)
        .mark_line()
        .encode(
            x=alt.X("time:T", title="Time"),
            y=alt.Y("value:Q", title=None),
            color=alt.Color(
                "metric:N",
                legend=alt.Legend(title=None),
                scale=alt.Scale(scheme=CHART_SCHEME),
            ),
            tooltip=["time:T", "metric:N", alt.Tooltip("value:Q", format=".2f")],
        )
        .properties(height=height)
        .configure_axis(
            labelColor="#cfd6e5",
            titleColor="#cfd6e5",
            gridColor="#1f252f"
        )
        .configure_legend(labelColor="#cfd6e5", titleColor="#cfd6e5")
        .configure_title(color="#cfd6e5")
    )
    if title:
        chart = chart.properties(title=title)
    return chart.interactive()


def bar_chart(data, height=200, title=None, color="#61a5ff"):
    chart = (
        alt.Chart(data)
        .mark_bar(color=color)
        .encode(
            x=alt.X("label:N", title=None, sort=None),
            y=alt.Y("value:Q", title=None),
            tooltip=["label:N", alt.Tooltip("value:Q", format=".1f")],
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

    if "ping_results" not in st.session_state:
        st.session_state.ping_results = {}
    if "conn_expanded" not in st.session_state:
        st.session_state.conn_expanded = False
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
        f"""<div class="ingest-chip {s['status']}">
  <span class="dot" style="background:{s['colors'][0]};"></span>
  <span>{s['name']}</span>
  <span class="ingest-pill {s['status']}">{status_labels[s['status']]}</span>
</div>"""
        for s in statuses
    )


    details_html = "".join(
        f"""<div class="ingest-detail-row">
  <div class="meta">
    <span class="dot" style="background:{s['colors'][0]};"></span>
    <span>{s['name']}</span>
    <span class="ingest-pill {s['status']}">{status_labels[s['status']]}</span>
  </div>
  <div class="detail">{s['latency_text']} - {s['load_text']}</div>
  <div class="last">{s['last_seen']}</div>
</div>"""
        for s in statuses
    )


    st.sidebar.markdown('<div class="conn-toggle">', unsafe_allow_html=True)
    conn_toggle = st.sidebar.checkbox(
        "Connection details",
        value=st.session_state.conn_expanded,
        key="conn_details_toggle",
    )
    st.sidebar.markdown("</div>", unsafe_allow_html=True)
    st.session_state.conn_expanded = conn_toggle

    if st.session_state.conn_expanded:
        with st.sidebar.container():
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
    </div>
  </div>
</div>
                    """,
                    unsafe_allow_html=True,
                )
            with header_cols[1]:
                st.markdown(
                    f"<div class='ingest-badge'>{badge_text} <span title='Avg data age reflects how long ago each source last reported.'>(i)</span></div>",
                    unsafe_allow_html=True,
                )
            guard_assets = {
                "AirLink": ("archer", Path("images/Skeleton_Archer/Walk.png"), Path("images/Skeleton_Archer/Dead.png")),
                "Tempest Station": ("spearman", Path("images/Skeleton_Spearman/Walk.png"), Path("images/Skeleton_Spearman/Dead.png")),
                "Tempest Hub": ("warrior", Path("images/Skeleton_Warrior/Walk.png"), Path("images/Skeleton_Warrior/Dead.png")),
            }
            guard_sprites = {}
            if all(idle.exists() and dead.exists() for _, (__, idle, dead) in guard_assets.items()):
                import base64

                for key, (_, idle_path, dead_path) in guard_assets.items():
                    guard_sprites[key] = {
                        "idleUri": "data:image/png;base64," + base64.b64encode(idle_path.read_bytes()).decode("ascii"),
                        "deadUri": "data:image/png;base64," + base64.b64encode(dead_path.read_bytes()).decode("ascii"),
                        "idleFrames": strip_frame_count(idle_path) or 1,
                        "deadFrames": strip_frame_count(dead_path) or 1,
                    }

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

            for src in statuses:
                target = PING_TARGETS.get(src["name"])
                if not target and src["name"] == "Tempest Station":
                    target = PING_TARGETS.get("Tempest Hub")
                ping_disabled = not target
                expander_title = f"{src['name']} — {status_labels[src['status']]}"
                with st.expander(expander_title, expanded=False):
                    st.markdown(f"**Status:** {status_labels[src['status']]}")
                    st.markdown(f"**Latency/Load:** {src['latency_text']} - {src['load_text']}")
                    st.markdown(f"**Last seen:** {src['last_seen']}")
                    if st.button(
                        "ping",
                        key=f"ping_{src['name']}",
                        disabled=ping_disabled,
                    ):
                        st.session_state.conn_expanded = True
                        ok, msg = ping_device(target)
                        st.session_state.ping_results[src["name"]] = (ok, msg, time.time())
                    guard_sprite = guard_sprites.get(src["name"])
                    if guard_sprite:
                        guard_state = "guard" if src["status"] in ("ok", "standby") else "down"
                        canvas_id = f"guard_{src['name'].replace(' ', '_')}"
                        guard_html = f"""
<style>
.conn-guard {{
  display: flex;
  align-items: flex-start;
  justify-content: center;
  margin-top: -6px;
}}
.conn-guard canvas {{
  width: 140px;
  height: 140px;
  display: block;
}}
</style>
<style>
body {{
  margin: 0;
  background: transparent;
}}
canvas {{
  background: transparent;
}}
</style>
<div class="conn-guard">
  <canvas id="{canvas_id}" width="160" height="120"></canvas>
</div>
<script>
(function() {{
  const canvas = document.getElementById("{canvas_id}");
  if (!canvas) return;
  const ctx = canvas.getContext("2d");
  const frameSize = 128;
  const idleImg = new Image();
  const deadImg = new Image();
  idleImg.src = "{guard_sprite['idleUri']}";
  deadImg.src = "{guard_sprite['deadUri']}";
  const idleFrames = {guard_sprite['idleFrames']};
  const deadFrames = {guard_sprite['deadFrames']};
  const state = "{guard_state}";
  let t = 0;
  let offset = 0;
  let dir = 1;
  let last = performance.now();
  function draw() {{
    ctx.clearRect(0, 0, canvas.width, canvas.height);
    const scale = 0.9;
    const drawW = frameSize * scale;
    const drawH = frameSize * scale;
    const x = (canvas.width - drawW) / 2 + offset;
    const y = 2;
    if (state === "guard") {{
      const frame = Math.floor((t * 7) % idleFrames);
      const bob = Math.sin(t * 2) * 0.6;
      ctx.drawImage(idleImg, frame * frameSize, 0, frameSize, frameSize, x, y + bob, drawW, drawH);
    }} else {{
      const frame = Math.max(0, deadFrames - 1);
      ctx.drawImage(deadImg, frame * frameSize, 0, frameSize, frameSize, x, y + 6, drawW, drawH);
    }}
  }}
  function loop(now) {{
    const dt = (now - last) / 1000;
    last = now;
    t += dt;
    offset += dir * dt * 18;
    if (offset > 10) dir = -1;
    if (offset < -10) dir = 1;
    draw();
    requestAnimationFrame(loop);
  }}
  requestAnimationFrame(loop);
}})();
</script>
"""
                        components.html(guard_html, height=150)
                    result = st.session_state.ping_results.get(src["name"])
                    if result:
                        ok, msg, ts = result
                        if time.time() - ts < 6:
                            status_label = "OK" if ok else "WARN"
                            st.markdown(
                                f"<div class='ping-toast'>{status_label}: {src['name']} - {msg}</div>",
                                unsafe_allow_html=True,
                            )
                    elif ping_disabled:
                        st.markdown(f"INFO: {src['name']}: set ping target in `PING_TARGETS` to enable.")

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


def get_png_size(path: Path):
    try:
        with path.open("rb") as handle:
            header = handle.read(24)
        if len(header) < 24 or header[:8] != b"\x89PNG\r\n\x1a\n":
            return None, None
        width = int.from_bytes(header[16:20], "big")
        height = int.from_bytes(header[20:24], "big")
        return width, height
    except Exception:
        return None, None


def get_png_size_from_bytes(data: bytes):
    try:
        header = data[:24]
        if len(header) < 24 or header[:8] != b"\x89PNG\r\n\x1a\n":
            return None, None
        width = int.from_bytes(header[16:20], "big")
        height = int.from_bytes(header[20:24], "big")
        return width, height
    except Exception:
        return None, None


def strip_frame_count(path: Path, frame_size: int = 128):
    width, height = get_png_size(path)
    if not width or not height or height < frame_size:
        return 0
    return max(1, width // frame_size)


def sanitize_sprite_component(value: str):
    if not value:
        return ""
    cleaned = []
    for ch in value.strip():
        if ch.isalnum() or ch in ("_", "-", "/"):
            cleaned.append(ch)
        elif ch.isspace():
            cleaned.append("_")
    return "".join(cleaned).strip("_")


def load_sprite_manifest_file(path: Path):
    if not path.exists():
    return {"generated_at": datetime.utcnow().isoformat() + "Z", "frame_size": 128, "sheet_columns": 8, "sheets": []}
    try:
        return json.loads(path.read_text())
    except Exception:
        return {"generated_at": datetime.utcnow().isoformat() + "Z", "frame_size": 128, "sheet_columns": 8, "sheets": []}


def save_sprite_manifest_file(path: Path, manifest: dict):
    manifest["generated_at"] = datetime.utcnow().isoformat() + "Z"
    path.write_text(json.dumps(manifest, indent=2))


def load_ui_skin_state():
    try:
        if UI_SKIN_STATE_PATH.exists():
            payload = json.loads(UI_SKIN_STATE_PATH.read_text(encoding="utf-8"))
            if isinstance(payload, dict):
                return payload.get("skin")
    except Exception:
        return None
    return None


def save_ui_skin_state(skin_name: str):
    try:
        UI_SKIN_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
        UI_SKIN_STATE_PATH.write_text(json.dumps({"skin": skin_name}, indent=2), encoding="utf-8")
    except Exception:
        pass


def build_sheet_entry(sheet_name: str, sheet_path: Path, sheet_type: str, frame_size: int):
    width, height = get_png_size(sheet_path)
    frames = 1
    if sheet_type == "strip" and width:
        frames = max(1, int(width // frame_size))
    if sheet_type == "atlas":
        frames = 0
    frame_names = [f"{sheet_name}/{idx}" for idx in range(frames)]
    return {
        "name": sheet_name,
        "path": sheet_path.as_posix(),
        "type": sheet_type,
        "frame_size": frame_size,
        "width": width,
        "height": height,
        "frames": frames,
        "frame_names": frame_names,
    }


def auto_map_sprite_sheet(image_path: Path, tolerance: int = 28, min_area: int = 180, padding: int = 2):
    try:
        img = Image.open(image_path).convert("RGB")
    except Exception:
        return [], []
    arr = np.array(img)
    h, w, _ = arr.shape
    samples = [
        arr[0, 0],
        arr[0, w - 1],
        arr[h - 1, 0],
        arr[h - 1, w - 1],
    ]
    bg = np.median(np.stack(samples, axis=0), axis=0)
    diff = np.abs(arr.astype(np.int16) - bg.astype(np.int16))
    mask = (diff.sum(axis=2) > tolerance).astype(np.uint8)
    visited = np.zeros((h, w), dtype=np.uint8)
    rects = []
    for y in range(h):
        for x in range(w):
            if mask[y, x] == 0 or visited[y, x] == 1:
                continue
            stack = [(y, x)]
            visited[y, x] = 1
            minx = maxx = x
            miny = maxy = y
            area = 0
            while stack:
                cy, cx = stack.pop()
                area += 1
                if cx < minx:
                    minx = cx
                if cx > maxx:
                    maxx = cx
                if cy < miny:
                    miny = cy
                if cy > maxy:
                    maxy = cy
                if cy > 0 and mask[cy - 1, cx] and not visited[cy - 1, cx]:
                    visited[cy - 1, cx] = 1
                    stack.append((cy - 1, cx))
                if cy < h - 1 and mask[cy + 1, cx] and not visited[cy + 1, cx]:
                    visited[cy + 1, cx] = 1
                    stack.append((cy + 1, cx))
                if cx > 0 and mask[cy, cx - 1] and not visited[cy, cx - 1]:
                    visited[cy, cx - 1] = 1
                    stack.append((cy, cx - 1))
                if cx < w - 1 and mask[cy, cx + 1] and not visited[cy, cx + 1]:
                    visited[cy, cx + 1] = 1
                    stack.append((cy, cx + 1))
            if area < min_area:
                continue
            minx = max(0, minx - padding)
            miny = max(0, miny - padding)
            maxx = min(w - 1, maxx + padding)
            maxy = min(h - 1, maxy + padding)
            rects.append((minx, miny, maxx, maxy))
    if not rects:
        return [], []
    rects = sorted(rects, key=lambda r: (r[1], r[0]))
    heights = [r[3] - r[1] for r in rects]
    row_thresh = max(4, int(np.median(heights) * 0.6)) if heights else 6
    rows = []
    for rect in rects:
        placed = False
        for row in rows:
            if abs(rect[1] - row["y"]) <= row_thresh:
                row["rects"].append(rect)
                row["y"] = int((row["y"] + rect[1]) / 2)
                placed = True
                break
        if not placed:
            rows.append({"y": rect[1], "rects": [rect]})
    frame_rects = []
    frame_labels = []
    row_groups = []
    for r_idx, row in enumerate(sorted(rows, key=lambda r: r["y"])):
        row_rects = sorted(row["rects"], key=lambda r: r[0])
        row_indices = []
        for c_idx, rect in enumerate(row_rects):
            row_indices.append(len(frame_rects))
            frame_rects.append(
                {
                    "sx": int(rect[0]),
                    "sy": int(rect[1]),
                    "w": int(rect[2] - rect[0] + 1),
                    "h": int(rect[3] - rect[1] + 1),
                }
            )
            frame_labels.append(f"Row{r_idx + 1}_Col{c_idx + 1}")
        row_groups.append({"row": r_idx + 1, "y": row["y"], "indices": row_indices})
    return frame_rects, frame_labels, row_groups


def ocr_sheet_labels(image_path: Path, min_conf: int = 60):
    if pytesseract is None or TesseractOutput is None:
        return []
    try:
        img = Image.open(image_path).convert("RGB")
        data = pytesseract.image_to_data(img, output_type=TesseractOutput.DICT)
    except Exception:
        return []
    words = []
    for i in range(len(data.get("text", []))):
        text = (data["text"][i] or "").strip()
        if not text:
            continue
        try:
            conf = int(float(data["conf"][i]))
        except Exception:
            conf = -1
        if conf < min_conf:
            continue
        x, y, w, h = data["left"][i], data["top"][i], data["width"][i], data["height"][i]
        words.append({"text": text, "x": x, "y": y, "w": w, "h": h})
    if not words:
        return []
    words = sorted(words, key=lambda w: (w["y"], w["x"]))
    lines = []
    current = []
    for word in words:
        if not current:
            current.append(word)
            continue
        avg_y = sum(w["y"] for w in current) / len(current)
        if abs(word["y"] - avg_y) <= max(8, word["h"]):
            current.append(word)
        else:
            lines.append(current)
            current = [word]
    if current:
        lines.append(current)
    line_labels = []
    for line in lines:
        line = sorted(line, key=lambda w: w["x"])
        text = " ".join(w["text"] for w in line)
        if text:
            center_y = sum(w["y"] + w["h"] / 2 for w in line) / len(line)
            line_labels.append({"text": text, "y": center_y})
    return line_labels


def assign_row_labels(row_groups, line_labels, max_distance: int = 90):
    labels = {}
    for row in row_groups:
        row_y = row.get("y", 0)
        candidates = [
            l for l in line_labels if 0 <= (row_y - l["y"]) <= max_distance
        ]
        if not candidates:
            labels[row["row"]] = f"Row{row['row']}"
            continue
        nearest = min(candidates, key=lambda l: row_y - l["y"])
        labels[row["row"]] = nearest["text"]
    return labels


def build_sprite_preview(sheet_entry: dict, max_frames: int = 12):
    try:
        sheet_path = Path(sheet_entry.get("path", ""))
        if not sheet_path.exists():
            return None
        img = Image.open(sheet_path).convert("RGBA")
    except Exception:
        return None
    sheet_type = sheet_entry.get("type", "strip")
    frame_size = int(sheet_entry.get("frame_size") or 128)
    columns = int(sheet_entry.get("columns") or 8)
    frames = int(sheet_entry.get("frames") or 1)
    rects = []
    if sheet_type == "single":
        rects = [(0, 0, img.width, img.height)]
    elif sheet_type == "atlas" and sheet_entry.get("frame_rects"):
        for rect in sheet_entry.get("frame_rects", []):
            rects.append((rect["sx"], rect["sy"], rect["sx"] + rect["w"], rect["sy"] + rect["h"]))
    elif sheet_type == "strip":
        for idx in range(frames):
            rects.append((idx * frame_size, 0, (idx + 1) * frame_size, frame_size))
    else:
        for idx in range(frames):
            row = idx // columns
            col = idx % columns
            rects.append(
                (
                    col * frame_size,
                    row * frame_size,
                    (col + 1) * frame_size,
                    (row + 1) * frame_size,
                )
            )
    if not rects:
        return None
    rects = rects[: max_frames if max_frames > 0 else len(rects)]
    frames_out = []
    for rect in rects:
        crop = img.crop(rect)
        target_h = 96
        scale = target_h / max(1, crop.height)
        target_w = max(1, int(crop.width * scale))
        frames_out.append(crop.resize((target_w, target_h), Image.NEAREST))
    preview_dir = Path("static/previews")
    preview_dir.mkdir(parents=True, exist_ok=True)
    safe_name = sanitize_sprite_component(sheet_entry.get("name", "preview")).replace("/", "_")
    out_path = preview_dir / f"{safe_name}.gif"
    try:
        frames_out[0].save(
            out_path,
            save_all=True,
            append_images=frames_out[1:],
            duration=120,
            loop=0,
            optimize=False,
        )
        return str(out_path)
    except Exception:
        return None


def ingest_sprite_sheet(
    file_name: str,
    file_bytes: bytes,
    character: str,
    action: str,
    frame_size: int,
    sheet_type: str,
    auto_tolerance: int = 28,
    auto_min_area: int = 180,
    auto_padding: int = 2,
    run_ocr: bool = False,
    split_groups: bool = False,
    keep_master: bool = True,
):
    character = sanitize_sprite_component(character)
    action = sanitize_sprite_component(action)
    if not character:
        character = "Custom"
    if not action:
        action = Path(file_name).stem
    sheet_name = f"{character}/{action}"
    uploads_dir = Path("images/uploads") / character
    uploads_dir.mkdir(parents=True, exist_ok=True)
    safe_name = sanitize_sprite_component(Path(file_name).stem) or "sprite"
    dest_path = uploads_dir / f"{safe_name}.png"
    dest_path.write_bytes(file_bytes)
    manifest_path = Path("static/sprite_manifest.json")
    manifest = load_sprite_manifest_file(manifest_path)
    if "sheets" not in manifest or not isinstance(manifest["sheets"], list):
        manifest["sheets"] = []
    entry = build_sheet_entry(sheet_name, dest_path, sheet_type, frame_size)
    if sheet_type == "atlas":
        rects, labels, row_groups = auto_map_sprite_sheet(
            dest_path,
            tolerance=int(auto_tolerance),
            min_area=int(auto_min_area),
            padding=int(auto_padding),
        )
        line_labels = ocr_sheet_labels(dest_path) if run_ocr else []
        row_labels = assign_row_labels(row_groups, line_labels) if row_groups else {}
        entry["frame_rects"] = rects
        entry["frame_labels"] = labels
        entry["frames"] = len(rects)
        entry["frame_names"] = [f"{sheet_name}/{idx}" for idx in range(len(rects))]
        entry["row_groups"] = [
            {
                "row": row["row"],
                "label": sanitize_sprite_component(row_labels.get(row["row"], f"Row{row['row']}")),
                "indices": row["indices"],
            }
            for row in row_groups
        ]
    replaced = False
    for idx, sheet in enumerate(manifest["sheets"]):
        if sheet.get("name") == sheet_name:
            manifest["sheets"][idx] = entry
            replaced = True
            break
    if not replaced:
        if keep_master:
            manifest["sheets"].append(entry)
    if sheet_type == "atlas" and entry.get("row_groups") and split_groups:
        for group in entry["row_groups"]:
            label = group.get("label") or f"Row{group.get('row', 0)}"
            safe_label = sanitize_sprite_component(label) or f"Row{group.get('row', 0)}"
            group_name = f"{character}/{safe_label}"
            group_entry = {
                "name": group_name,
                "path": str(dest_path),
                "type": "atlas",
                "frame_size": frame_size,
                "width": entry.get("width"),
                "height": entry.get("height"),
                "frames": len(group.get("indices", [])),
                "frame_names": [f"{group_name}/{idx}" for idx in range(len(group.get("indices", [])))],
                "frame_rects": [entry["frame_rects"][i] for i in group.get("indices", [])],
                "frame_labels": [entry["frame_labels"][i] for i in group.get("indices", [])],
                "group_label": label,
                "source_sheet": sheet_name,
            }
            manifest["sheets"].append(group_entry)
    save_sprite_manifest_file(manifest_path, manifest)
    return {"name": sheet_name, "path": str(dest_path), "frames": entry["frames"], "updated": True}


def analyze_atlas_sheet(
    sheet_name: str,
    run_ocr: bool = True,
    split_groups: bool = True,
    keep_master: bool = True,
    auto_tolerance: int = 28,
    auto_min_area: int = 180,
    auto_padding: int = 2,
):
    manifest_path = Path("static/sprite_manifest.json")
    manifest = load_sprite_manifest_file(manifest_path)
    sheets = manifest.get("sheets", []) if isinstance(manifest.get("sheets", []), list) else []
    target = next((s for s in sheets if s.get("name") == sheet_name), None)
    if not target:
        return {"ok": False, "message": "Sheet not found."}
    def resolve_sheet_path(path_value: str):
        if not path_value:
            return None
        candidates = []
        raw = Path(path_value)
        candidates.append(raw)
        candidates.append(Path(str(path_value).replace("\\", "/")))
        candidates.append(Path.cwd() / path_value)
        candidates.append(Path.cwd() / str(path_value).replace("\\", "/"))
        for cand in candidates:
            if cand.exists():
                return cand
        return None

    dest_path = resolve_sheet_path(target.get("path", ""))
    if not dest_path:
        return {"ok": False, "message": f"Sheet image missing on disk: {target.get('path', '')}"}
    rects, labels, row_groups = auto_map_sprite_sheet(
        dest_path,
        tolerance=int(auto_tolerance),
        min_area=int(auto_min_area),
        padding=int(auto_padding),
    )
    line_labels = ocr_sheet_labels(dest_path) if run_ocr else []
    row_labels = assign_row_labels(row_groups, line_labels) if row_groups else {}
    target["type"] = "atlas"
    target["frame_rects"] = rects
    target["frame_labels"] = labels
    target["frames"] = len(rects)
    target["frame_names"] = [f"{sheet_name}/{idx}" for idx in range(len(rects))]
    target["row_groups"] = [
        {
            "row": row["row"],
            "label": sanitize_sprite_component(row_labels.get(row["row"], f"Row{row['row']}")),
            "indices": row["indices"],
        }
        for row in row_groups
    ]
    sheets = [s for s in sheets if s.get("source_sheet") != sheet_name]
    if keep_master:
        sheets = [s for s in sheets if s.get("name") != sheet_name] + [target]
    else:
        sheets = [s for s in sheets if s.get("name") != sheet_name]
    if split_groups and target.get("row_groups"):
        for group in target["row_groups"]:
            label = group.get("label") or f"Row{group.get('row', 0)}"
            safe_label = sanitize_sprite_component(label) or f"Row{group.get('row', 0)}"
            group_name = f"{sheet_name}/{safe_label}"
            group_entry = {
                "name": group_name,
                "path": str(dest_path),
                "type": "atlas",
                "frame_size": target.get("frame_size"),
                "width": target.get("width"),
                "height": target.get("height"),
                "frames": len(group.get("indices", [])),
                "frame_names": [f"{group_name}/{idx}" for idx in range(len(group.get("indices", [])))],
                "frame_rects": [target["frame_rects"][i] for i in group.get("indices", [])],
                "frame_labels": [target["frame_labels"][i] for i in group.get("indices", [])],
                "group_label": label,
                "source_sheet": sheet_name,
            }
            sheets.append(group_entry)
    manifest["sheets"] = sheets
    save_sprite_manifest_file(manifest_path, manifest)
    return {
        "ok": True,
        "message": f"Analyzed {sheet_name} ({len(rects)} frames, {len(row_groups)} groups).",
        "row_groups": target.get("row_groups", []),
        "sheet": target,
    }


def parse_texture_atlas(xml_bytes: bytes):
    try:
        root = ET.fromstring(xml_bytes)
    except Exception:
        return None, []
    image_path = root.attrib.get("imagePath", "")
    subtextures = []
    for node in root.findall(".//SubTexture"):
        name = node.attrib.get("name", "")
        try:
            x = int(float(node.attrib.get("x", 0)))
            y = int(float(node.attrib.get("y", 0)))
            w = int(float(node.attrib.get("width", 0)))
            h = int(float(node.attrib.get("height", 0)))
        except Exception:
            continue
        if name and w > 0 and h > 0:
            subtextures.append({"name": name, "x": x, "y": y, "w": w, "h": h})
    return image_path, subtextures


def ingest_ui_pack(xml_bytes: bytes, png_bytes: bytes, pack_name: str):
    pack_name = sanitize_sprite_component(pack_name) or "UI_Pack"
    ui_dir = Path("assets/ui_packs") / pack_name
    ui_dir.mkdir(parents=True, exist_ok=True)
    xml_path = ui_dir / "spritesheet.xml"
    png_path = ui_dir / "spritesheet.png"
    xml_path.write_bytes(xml_bytes)
    png_path.write_bytes(png_bytes)
    _, subtextures = parse_texture_atlas(xml_bytes)
    if not subtextures:
        return {"ok": False, "message": "No subtextures found in XML."}
    manifest_path = Path("static/sprite_manifest.json")
    manifest = load_sprite_manifest_file(manifest_path)
    if "sheets" not in manifest or not isinstance(manifest["sheets"], list):
        manifest["sheets"] = []
    sheet_name = f"UI/{pack_name}"
    frame_rects = [{"sx": s["x"], "sy": s["y"], "w": s["w"], "h": s["h"]} for s in subtextures]
    frame_labels = [Path(s["name"]).stem for s in subtextures]
    entry = {
        "name": sheet_name,
        "path": png_path.as_posix(),
        "type": "atlas",
        "frame_size": 0,
        "width": get_png_size(png_path)[0],
        "height": get_png_size(png_path)[1],
        "frames": len(frame_rects),
        "frame_names": [f"{sheet_name}/{idx}" for idx in range(len(frame_rects))],
        "frame_rects": frame_rects,
        "frame_labels": frame_labels,
        "ui_pack": True,
    }
    replaced = False
    for idx, sheet in enumerate(manifest["sheets"]):
        if sheet.get("name") == sheet_name:
            manifest["sheets"][idx] = entry
            replaced = True
            break
    if not replaced:
        manifest["sheets"].append(entry)
    save_sprite_manifest_file(manifest_path, manifest)
    materialize_ui_pack_assets(entry, pack_name)
    save_ui_skin_state(pack_name)
    return {"ok": True, "message": f"Imported {len(frame_rects)} UI assets.", "sheet": entry}


def build_ui_previews(sheet_entry: dict, max_items: int = 24):
    try:
        img = Image.open(Path(sheet_entry.get("path", ""))).convert("RGBA")
    except Exception:
        return []
    rects = sheet_entry.get("frame_rects", [])[:max_items]
    labels = sheet_entry.get("frame_labels", [])[:max_items]
    previews = []
    for rect, label in zip(rects, labels):
        crop = img.crop((rect["sx"], rect["sy"], rect["sx"] + rect["w"], rect["sy"] + rect["h"]))
        previews.append((label, crop))
    return previews


def materialize_ui_pack_assets(sheet_entry: dict, pack_name: str):
    try:
        img = Image.open(Path(sheet_entry.get("path", ""))).convert("RGBA")
    except Exception:
        return None
    labels = sheet_entry.get("frame_labels", [])
    rects = sheet_entry.get("frame_rects", [])
    if not labels or not rects:
        return None
    out_dir = Path("static/ui_packs") / sanitize_sprite_component(pack_name)
    out_dir.mkdir(parents=True, exist_ok=True)
    for label, rect in zip(labels, rects):
        safe = sanitize_sprite_component(Path(label).stem) or "asset"
        out_path = out_dir / f"{safe}.png"
        crop = img.crop((rect["sx"], rect["sy"], rect["sx"] + rect["w"], rect["sy"] + rect["h"]))
        crop.save(out_path)
    return out_dir


def find_ui_packs(root_path: Path):
    if not root_path.exists():
        return {}
    packs = {}
    for folder in root_path.rglob("*"):
        if not folder.is_dir():
            continue
        xml_files = list(folder.glob("*.xml"))
        png_files = list(folder.glob("*.png"))
        if not xml_files or not png_files:
            continue
        xml_file = xml_files[0]
        png_file = next((p for p in png_files if "spritesheet" in p.name.lower()), png_files[0])
        pack_name = sanitize_sprite_component(folder.name) or folder.name
        packs[pack_name] = {"xml": xml_file, "png": png_file}
    return packs


def list_ui_pack_entries():
    manifest_path = Path("static/sprite_manifest.json")
    manifest = load_sprite_manifest_file(manifest_path)
    sheets = manifest.get("sheets", []) if isinstance(manifest.get("sheets", []), list) else []
    return [s for s in sheets if s.get("ui_pack")]


def pick_ui_asset(labels, candidates):
    if not labels:
        return None
    lower = [l.lower() for l in labels]
    for cand in candidates:
        cand = cand.lower()
        for idx, label in enumerate(lower):
            if cand in label:
                return labels[idx]
    return labels[0]


def extract_ui_pack_zip(zip_bytes: bytes, dest_root: Path, pack_name: str):
    pack_name = sanitize_sprite_component(pack_name) or "UI_Pack"
    target_dir = dest_root / pack_name
    target_dir.mkdir(parents=True, exist_ok=True)
    target_root = target_dir.resolve()
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        for info in zf.infolist():
            name = info.filename
            if not name or name.endswith("/"):
                continue
            safe_name = name.replace("\\", "/")
            safe_name = safe_name.lstrip("/").replace("../", "")
            if ":" in safe_name.split("/")[0]:
                continue
            dest = (target_dir / safe_name).resolve()
            if os.path.commonpath([str(dest), str(target_root)]) != str(target_root):
                continue
            dest.parent.mkdir(parents=True, exist_ok=True)
            with zf.open(info) as src, open(dest, "wb") as out:
                out.write(src.read())
    return target_dir


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

st.sidebar.subheader("UI Skin")
ui_packs = list_ui_pack_entries()
ui_pack_names = [p.get("name", "").split("/")[-1] for p in ui_packs]
ui_pack_names = [n for n in ui_pack_names if n]
ui_pack_names = ["Default"] + sorted(set(ui_pack_names))
if "ui_skin" not in st.session_state:
    st.session_state.ui_skin = "Default"
selected_skin = st.sidebar.selectbox("Skin pack", ui_pack_names, index=ui_pack_names.index(st.session_state.ui_skin))
st.session_state.ui_skin = selected_skin
ui_sheet = None
if selected_skin != "Default":
    ui_sheet = next(
        (p for p in ui_packs if p.get("name", "").endswith("/" + selected_skin)),
        None,
    )
if ui_sheet:
    labels = ui_sheet.get("frame_labels", [])
    primary_label = pick_ui_asset(labels, ["button_red", "button_brown", "button_grey", "button"])
    panel_label = pick_ui_asset(labels, ["panel", "banner", "frame", "window"])
    checkbox_label = pick_ui_asset(labels, ["checkbox", "toggle"])
    skin_root = Path("static/ui_packs") / sanitize_sprite_component(selected_skin)
    primary_path = (skin_root / f"{sanitize_sprite_component(primary_label)}.png") if primary_label else None
    panel_path = (skin_root / f"{sanitize_sprite_component(panel_label)}.png") if panel_label else None
    checkbox_path = (skin_root / f"{sanitize_sprite_component(checkbox_label)}.png") if checkbox_label else None
    st.markdown(
        f"""
        <style>
        :root {{
          --ui-btn-primary: {"url('" + primary_path.as_posix() + "')" if primary_path and primary_path.exists() else "none"};
          --ui-panel: {"url('" + panel_path.as_posix() + "')" if panel_path and panel_path.exists() else "none"};
          --ui-checkbox: {"url('" + checkbox_path.as_posix() + "')" if checkbox_path and checkbox_path.exists() else "none"};
        }}
        .btn-accent,
        [data-testid="stButton"] > button {{
          background-image: var(--ui-btn-primary);
          background-size: 100% 100%;
          background-repeat: no-repeat;
        }}
        .metric-expanders [data-testid="stExpander"] summary {{
          background-image: var(--ui-panel);
          background-size: 100% 100%;
          background-repeat: no-repeat;
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

render_ingest_banner(ingest_sources, total_recent, avg_latency_text=avg_latency_text)

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
          <div class='dash-title'>Weather & Air Quality Dashboard</div>
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
tabs = st.tabs(["Overview", "Trends", "Comparisons", "Raw", "Sprite Lab"])

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

    st.markdown("<div class='metric-expanders'>", unsafe_allow_html=True)
    cols = st.columns(2)
    for idx, metric in enumerate(metrics):
        col = cols[idx % 2]
        label = metric_label(metric["label"], metric["value"], metric["unit"], metric["delta"])
        with col.expander(label, expanded=False):
            st.metric(metric["label"], metric["value"], metric["delta"])
            if metric["df"] is not None:
                if metric.get("key") == "aqi" and "value" in metric["df"]:
                    avg_val = float(metric["df"]["value"].mean())
                    base_chart = (
                        alt.Chart(metric["df"])
                        .mark_line()
                        .encode(
                            x=alt.X("time:T", title="Time"),
                            y=alt.Y("value:Q", title=None),
                            color=alt.Color(
                                "metric:N",
                                legend=alt.Legend(title=None),
                                scale=alt.Scale(scheme=CHART_SCHEME),
                            ),
                            tooltip=["time:T", "metric:N", alt.Tooltip("value:Q", format=".2f")],
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
with tabs[1]:
    st.subheader("Trends")
    if tempest is not None and not tempest.empty:
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
        st.altair_chart(
            clean_chart(temp_long, height=280, title=None),
            width="stretch",
        )
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
        st.altair_chart(
            clean_chart(wind_long, height=240, title=None),
            width="stretch",
        )
        pressure_long = tempest[["time", "pressure_inhg"]].rename(columns={"pressure_inhg": "value"})
        pressure_long["metric"] = "Pressure (inHg)"
        st.markdown(
            "<div class='chart-header'>Pressure Trend"
            "<span class='info-icon' title='Barometric pressure changes indicate approaching or clearing systems.'>i</span></div>",
            unsafe_allow_html=True,
        )
        st.altair_chart(
            clean_chart(pressure_long, height=240, title=None),
            width="stretch",
        )
        if "pressure_inhg" in tempest:
            pressure_rate = tempest[["time", "pressure_inhg"]].copy()
            pressure_rate["hours"] = pressure_rate["time"].diff().dt.total_seconds() / 3600
            pressure_rate["value"] = pressure_rate["pressure_inhg"].diff() / pressure_rate["hours"]
            pressure_rate["metric"] = "Pressure Change (inHg/hr)"
            pressure_rate = pressure_rate.dropna()
            if not pressure_rate.empty:
                st.markdown(
                    "<div class='chart-header'>Pressure Change Rate"
                    "<span class='info-icon' title='Rate of pressure change per hour; faster drops can signal storms.'>i</span></div>",
                    unsafe_allow_html=True,
                )
                st.altair_chart(
                    clean_chart(pressure_rate[["time", "value", "metric"]], height=220, title=None),
                    width="stretch",
                )
        if "battery" in tempest:
            battery_long = tempest[["time", "battery"]].rename(columns={"battery": "value"})
            battery_long["metric"] = "Battery (V)"
            st.markdown(
                "<div class='chart-header'>Battery Trend"
                "<span class='info-icon' title='Battery voltage trend to catch power drops early.'>i</span></div>",
                unsafe_allow_html=True,
            )
            st.altair_chart(
                clean_chart(battery_long, height=220, title=None),
                width="stretch",
            )
        if "rain_mm" in tempest:
            rain_long = tempest[["time", "rain_mm"]].rename(columns={"rain_mm": "value"})
            rain_long["metric"] = "Rain Accumulation (mm)"
            rain_chart = (
                alt.Chart(rain_long)
                .mark_line(interpolate="step-after")
                .encode(
                    x=alt.X("time:T", title="Time"),
                    y=alt.Y("value:Q", title="Rain (mm)"),
                    tooltip=["time:T", alt.Tooltip("value:Q", format=".2f")],
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
        if "wind_dir_deg" in tempest:
            def bin_dir(deg):
                if pd.isna(deg):
                    return None
                return compass_dir(deg)

            dir_counts = tempest["wind_dir_deg"].apply(bin_dir).value_counts().reset_index()
            dir_counts.columns = ["label", "value"]
            if not dir_counts.empty:
                st.markdown(
                    "<div class='chart-header'>Wind Direction Frequency"
                    "<span class='info-icon' title='Dominant wind directions during the window.'>i</span></div>",
                    unsafe_allow_html=True,
                )
                st.altair_chart(
                    bar_chart(dir_counts, height=200, title=None, color="#61a5ff"),
                    width="stretch",
                )
    if airlink is not None and not airlink.empty:
        aqi_long = airlink[["time", "aqi_pm25"]].rename(columns={"aqi_pm25": "value"})
        aqi_long["metric"] = "AQI (PM2.5)"
        st.markdown(
            "<div class='chart-header'>AQI Over Time"
            "<span class='info-icon' title='Tracks PM2.5 air quality index across the selected window.'>i</span></div>",
            unsafe_allow_html=True,
        )
        st.altair_chart(
            clean_chart(aqi_long, height=240, title=None),
            width="stretch",
        )
    if tempest is not None and not tempest.empty and "solar_radiation" in tempest and "uv" in tempest:
        solar_uv = tempest.melt(
            id_vars=["time"],
            value_vars=["solar_radiation", "uv"],
            var_name="metric",
            value_name="value",
        )
        solar_uv["metric"] = solar_uv["metric"].map(
            {"solar_radiation": "Solar Radiation (W/m²)", "uv": "UV Index"}
        )
        st.markdown(
            "<div class='chart-header'>Solar Radiation & UV"
            "<span class='info-icon' title='Sunlight intensity and UV index across the window.'>i</span></div>",
            unsafe_allow_html=True,
        )
        st.altair_chart(
            clean_chart(solar_uv, height=240, title=None),
            width="stretch",
        )
    if airlink is not None and not airlink.empty:
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
        st.altair_chart(
            clean_chart(pm_long, height=240, title=None),
            width="stretch",
        )

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
                    tooltip=["wind_speed_mph", "aqi_pm25"],
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
                    tooltip=["air_temperature_f", "relative_humidity", "comfort"],
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

# Raw tab
with tabs[3]:
    st.subheader("Raw")
    storage = get_storage_stats()
    total_objects = storage["sprite_sheets"] + storage["sequences"]
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
          <div class="gauge-muted">
            UI objects: {total_objects:,}
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

# Sprite Lab tab
with tabs[4]:
    st.subheader("Sprite Lab")
    st.markdown("Upload, analyze, and organize sprite sheets into actions and frames.")
    st.markdown("##### 1) Upload & Ingest")
    with st.expander("Upload sprite sheets", expanded=True):
        upload_files = st.file_uploader(
            "Sprite sheets (PNG)",
            type=["png"],
            accept_multiple_files=True,
        )
        upload_cols = st.columns(3)
        upload_character = upload_cols[0].text_input("Character key", value="Skeleton_Custom")
        upload_action = upload_cols[1].text_input("Action name (optional)")
        upload_type = upload_cols[2].selectbox(
            "Sheet type",
            ["auto", "strip (uniform frames)", "single", "atlas (scattered frames)"],
            index=0,
        )
        st.caption("Atlas = scattered frames on one canvas (like the Sonic sheet). Strip = uniform frames in a row.")
        frame_cols = st.columns(2)
        frame_size = frame_cols[0].number_input("Frame size (px)", min_value=16, max_value=512, value=128, step=1)
        auto_frame = frame_cols[1].checkbox("Auto-detect frame size from height", value=True)
        auto_cols = st.columns(3)
        auto_tolerance = auto_cols[0].number_input("Auto-map tolerance", min_value=5, max_value=120, value=28, step=1)
        auto_min_area = auto_cols[1].number_input("Auto-map min area", min_value=20, max_value=5000, value=180, step=10)
        auto_padding = auto_cols[2].number_input("Auto-map padding", min_value=0, max_value=12, value=2, step=1)
        ocr_cols = st.columns(3)
        run_ocr = ocr_cols[0].checkbox("Run OCR labels", value=True)
        split_groups = ocr_cols[1].checkbox("Create actions from groups", value=True)
        keep_master = ocr_cols[2].checkbox("Keep master sheet", value=True)
        if run_ocr and pytesseract is None:
            st.warning("OCR requested but pytesseract is not installed. Install Tesseract + pytesseract to enable labels.")
        tesseract_path = st.text_input("Tesseract path (optional)", value="")
        if tesseract_path and pytesseract is not None:
            pytesseract.pytesseract.tesseract_cmd = tesseract_path
        if st.button("Ingest Sheets", width="stretch"):
            if not upload_files:
                st.warning("Upload at least one PNG sheet.")
            else:
                results = []
                previews = []
                progress = st.progress(0)
                status = st.empty()
                for uploaded in upload_files:
                    sheet_kind = upload_type
                    status.info(f"Ingesting {uploaded.name}...")
                    data = uploaded.getvalue()
                    use_frame = int(frame_size)
                    if auto_frame:
                        width, height = get_png_size_from_bytes(data)
                        if height:
                            use_frame = height
                    if sheet_kind == "auto":
                        width, height = get_png_size_from_bytes(data)
                        if width and height and height > 0:
                            is_strip = (width % height == 0) and height <= 512
                            sheet_kind = "strip" if is_strip else "atlas"
                        else:
                            sheet_kind = "atlas"
                    if sheet_kind == "atlas":
                        use_frame = int(frame_size)
                    action_name = upload_action or Path(uploaded.name).stem
                    info = ingest_sprite_sheet(
                        uploaded.name,
                        data,
                        upload_character,
                        action_name,
                        use_frame,
                        sheet_kind,
                        auto_tolerance=auto_tolerance,
                        auto_min_area=auto_min_area,
                        auto_padding=auto_padding,
                        run_ocr=run_ocr,
                        split_groups=split_groups,
                        keep_master=keep_master,
                    )
                    if sheet_kind == "atlas":
                        manifest_path = Path("static/sprite_manifest.json")
                        manifest = load_sprite_manifest_file(manifest_path)
                        matched = next((s for s in manifest.get("sheets", []) if s.get("name") == info["name"]), None)
                        if matched and matched.get("frame_rects"):
                            preview_path = build_sprite_preview(matched)
                            if preview_path:
                                previews.append((info["name"], preview_path))
                            results.append(f"{info['name']} -> {len(matched['frame_rects'])} auto-mapped frames")
                        else:
                            results.append(f"{info['name']} -> {info['frames']} frames")
                    else:
                        manifest_path = Path("static/sprite_manifest.json")
                        manifest = load_sprite_manifest_file(manifest_path)
                        matched = next((s for s in manifest.get("sheets", []) if s.get("name") == info["name"]), None)
                        if matched:
                            preview_path = build_sprite_preview(matched)
                            if preview_path:
                                previews.append((info["name"], preview_path))
                        results.append(f"{info['name']} -> {info['frames']} frames")
                    progress.progress(min(1.0, len(results) / max(1, len(upload_files))))
                progress.progress(1.0)
                status.success("Ingest complete.")
                st.success("Ingested sheets:\\n" + "\\n".join(results))
                if previews:
                    st.markdown("##### Preview clips")
                    for name, path in previews:
                        st.markdown(f"**{name}**")
                        st.image(path)
    st.markdown("##### UI Pack Import")
    with st.expander("Import UI pack (XML + PNG)", expanded=False):
        ui_files = st.file_uploader(
            "UI Pack files (spritesheet PNG + XML)",
            type=["png", "xml"],
            accept_multiple_files=True,
        )
        ui_pack_name = st.text_input("Pack name", value="Kenney_Adventure")
        if st.button("Import UI Pack", width="stretch"):
            if not ui_files:
                st.warning("Upload both the XML and PNG for the UI pack.")
            else:
                xml_file = next((f for f in ui_files if f.name.lower().endswith(".xml")), None)
                png_file = next((f for f in ui_files if f.name.lower().endswith(".png")), None)
                if not xml_file or not png_file:
                    st.warning("Please include one XML and one PNG.")
                else:
                    result = ingest_ui_pack(xml_file.getvalue(), png_file.getvalue(), ui_pack_name)
                    if result.get("ok"):
                        st.success(result.get("message"))
                        sheet = result.get("sheet", {})
                        previews = build_ui_previews(sheet)
                        if previews:
                            st.markdown("##### UI asset previews")
                            cols = st.columns(6)
                            for idx, (label, img) in enumerate(previews):
                                with cols[idx % 6]:
                                    st.image(img, caption=label)
                    else:
                        st.error(result.get("message", "Import failed."))
    st.markdown("##### UI Pack Library")
    with st.expander("Browse local UI packs", expanded=False):
        default_ui_root = Path("assets/ui_packs")
        ui_root_input = st.text_input("UI pack folder", value=str(default_ui_root))
        ui_root = Path(ui_root_input) if ui_root_input else default_ui_root
        st.markdown("##### Quick import")
        zip_file = st.file_uploader("Upload UI pack zip", type=["zip"])
        zip_name = st.text_input("Pack name for zip", value="Imported_Pack")
        if zip_file and st.button("Extract zip to UI pack library", width="stretch"):
            extracted = extract_ui_pack_zip(zip_file.getvalue(), ui_root, zip_name)
            st.success(f"Extracted to {extracted}")
        if st.button("Scan UI Pack Folder", width="stretch"):
            st.session_state.ui_pack_scan = find_ui_packs(ui_root)
        packs = st.session_state.get("ui_pack_scan") or find_ui_packs(ui_root)
        if not packs:
            st.info("Drop UI packs into the folder and scan again. Expect XML + PNG in each pack folder.")
        else:
            pack_names = sorted(packs.keys())
            selected_pack = st.selectbox("Available packs", pack_names)
            pack = packs.get(selected_pack)
            if pack:
                st.caption(f"XML: {pack['xml']}")
                st.caption(f"PNG: {pack['png']}")
                if st.button("Import Selected Pack", width="stretch"):
                    result = ingest_ui_pack(pack["xml"].read_bytes(), pack["png"].read_bytes(), selected_pack)
                    if result.get("ok"):
                        st.success(result.get("message"))
                        sheet = result.get("sheet", {})
                        previews = build_ui_previews(sheet)
                        if previews:
                            st.markdown("##### UI asset previews")
                            cols = st.columns(6)
                            for idx, (label, img) in enumerate(previews):
                                with cols[idx % 6]:
                                    st.image(img, caption=label)
                    else:
                        st.error(result.get("message", "Import failed."))
    st.markdown("##### 2) Analyze & Group")
    with st.expander("Analyze existing atlas sheets", expanded=False):
        manifest_for_analysis = load_sprite_manifest_file(Path("static/sprite_manifest.json"))
        atlas_sheets = [
            s for s in manifest_for_analysis.get("sheets", [])
            if s.get("name")
        ]
        sheet_names = [s.get("name") for s in atlas_sheets if s.get("name")]
        if not sheet_names:
            st.info("No atlas sheets found. Upload an atlas sheet first.")
        else:
            selected_sheet = st.selectbox("Atlas sheet", sheet_names)
            analyze_cols = st.columns(3)
            analyze_run_ocr = analyze_cols[0].checkbox("Run OCR now", value=True)
            analyze_split = analyze_cols[1].checkbox("Create actions", value=True)
            analyze_keep = analyze_cols[2].checkbox("Keep master", value=True)
            if analyze_run_ocr and pytesseract is None:
                st.warning("OCR requested but pytesseract is not installed.")
            if st.button("Analyze sheet", width="stretch"):
                result = analyze_atlas_sheet(
                    selected_sheet,
                    run_ocr=analyze_run_ocr,
                    split_groups=analyze_split,
                    keep_master=analyze_keep,
                    auto_tolerance=auto_tolerance,
                    auto_min_area=auto_min_area,
                    auto_padding=auto_padding,
                )
                if result.get("ok"):
                    st.success(result.get("message"))
                    groups = result.get("row_groups", [])
                    if groups:
                        st.markdown("##### Detected groups")
                        st.dataframe(
                            pd.DataFrame(
                                [
                                    {
                                        "Row": g.get("row"),
                                        "Label": g.get("label"),
                                        "Frames": len(g.get("indices", [])),
                                    }
                                    for g in groups
                                ]
                            ),
                            width="stretch",
                        )
                    preview_path = build_sprite_preview(result.get("sheet", {}))
                    if preview_path:
                        st.markdown("##### Preview clip")
                        st.image(preview_path)
                else:
                    st.error(result.get("message", "Analysis failed."))
    # Persistent sequence store UI (Python-backed)
    with st.expander("Sequence Store (persistent)", expanded=False):
        seq_name = st.text_input("Sequence name", key="seq_store_name")
        seq_payload_text = st.text_area("Sequence payload (JSON)", height=100, key="seq_store_payload")
        seq_actions = st.columns(3)
        if seq_actions[0].button("Save Sequence (SQLite)", width="stretch"):
            if seq_name and seq_payload_text:
                try:
                    payload = json.loads(seq_payload_text)
                    save_sequence(seq_name, payload)
                    st.success(f"Saved sequence '{seq_name}'")
                except Exception as e:
                    st.error(f"Save failed: {e}")
            else:
                st.warning("Provide a name and JSON payload.")
        all_sequences = list_sequences()
        seq_list = all_sequences["name"].tolist() if not all_sequences.empty else []
        selected_seq = seq_actions[1].selectbox("Load saved", options=[""] + seq_list, index=0, key="seq_load_select")
        if seq_actions[2].button("Load to payload box", width="stretch"):
            if selected_seq:
                payload = load_sequence_payload(selected_seq)
                if payload is None:
                    st.warning("No payload found.")
                else:
                    st.session_state.seq_store_payload = json.dumps(payload, indent=2)
                    st.success(f"Loaded '{selected_seq}' into payload box.")
            else:
                st.info("Select a saved sequence to load.")
        if not all_sequences.empty:
            st.dataframe(all_sequences, width="stretch")
    sprite_js = Path("static/sprite_player.js").read_text() if Path("static/sprite_player.js").exists() else ""
    sprite_manifest = Path("static/sprite_manifest.json").read_text() if Path("static/sprite_manifest.json").exists() else "{}"
    sprite_images = {}
    try:
        manifest_data = json.loads(sprite_manifest)
        if isinstance(manifest_data, dict):
            import base64
            for sheet in manifest_data.get("sheets", []):
                sheet_name = sheet.get("name")
                sheet_path = sheet.get("path")
                if not sheet_name or not sheet_path:
                    continue
                img_path = Path(sheet_path)
                if not img_path.exists():
                    continue
                encoded = base64.b64encode(img_path.read_bytes()).decode("ascii")
                sprite_images[sheet_name] = f"data:image/png;base64,{encoded}"
    except Exception:
        sprite_images = {}
    sprite_lab_html = f"""
    <style>
    .sprite-lab {{
      display: grid;
      gap: 12px;
      padding: 12px;
      border-radius: 16px;
      border: 1px solid #1f2635;
      background: radial-gradient(circle at 20% 10%, rgba(97,165,255,0.12), transparent 38%), #0d1119;
      color: #d8deed;
      overflow: visible;
    }}
    .sprite-lab .row {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
      gap: 10px;
    }}
    .sprite-lab label {{
      font-size: 0.78rem;
      color: #9aa4b5;
      text-transform: uppercase;
      letter-spacing: 0.08em;
    }}
    .sprite-lab select,
    .sprite-lab input[type="range"],
    .sprite-lab button {{
      width: 100%;
      border-radius: 10px;
      border: 1px solid #263041;
      background: #121824;
      color: #e7ecf3;
      padding: 6px 8px;
      font-size: 0.9rem;
    }}
    .sprite-lab button {{
      cursor: pointer;
      background: #1a2332;
    }}
    .sprite-lab .canvas-wrap {{
      display: flex;
      align-items: center;
      justify-content: center;
      padding: 8px;
      border-radius: 12px;
      background: #0b0f16;
      border: 1px solid #1f2635;
    }}
    .sprite-lab .pos-row {{
      display: flex;
      align-items: center;
      gap: 8px;
      margin-top: 6px;
      font-size: 0.78rem;
      color: #9aa4b5;
    }}
    .sprite-lab .pos-pill {{
      padding: 2px 6px;
      border-radius: 999px;
      border: 1px solid #2a3446;
      color: #b6c3d9;
      font-size: 0.7rem;
      text-transform: uppercase;
      letter-spacing: 0.04em;
    }}
    .sprite-lab .status {{
      font-size: 0.8rem;
      color: #9aa4b5;
    }}
    .sprite-lab .input {{
      width: 100%;
      border-radius: 10px;
      border: 1px solid #263041;
      background: #121824;
      color: #e7ecf3;
      padding: 6px 8px;
      font-size: 0.9rem;
      box-sizing: border-box;
    }}
    .sprite-lab textarea.input {{
      min-height: 80px;
      resize: vertical;
    }}
    .sprite-lab .grid-2 {{
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 10px;
    }}
    .sprite-lab .log {{
      max-height: 180px;
      overflow: auto;
      padding: 8px;
      border-radius: 10px;
      border: 1px solid #1f2635;
      background: #0b0f16;
      font-size: 0.78rem;
      color: #cfd6e5;
      white-space: pre-wrap;
    }}
    .sprite-lab .section-title {{
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 8px;
      font-weight: 700;
      color: #e7ecf3;
      margin: 6px 0;
    }}
    .sprite-lab .chip {{
      padding: 2px 6px;
      border-radius: 999px;
      border: 1px solid #2a3446;
      color: #9fb2cc;
      font-size: 0.7rem;
      text-transform: uppercase;
      letter-spacing: 0.04em;
    }}
    .sprite-lab .drag-hint {{
      color: #8aa0bd;
      font-size: 0.72rem;
    }}
    .sprite-lab .toggle {{
      display: flex;
      align-items: center;
      gap: 8px;
      color: #9aa4b5;
      font-size: 0.82rem;
    }}
    .sprite-lab .toggle input {{
      accent-color: #7be7d9;
    }}
    .sprite-lab .icon-button {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      gap: 6px;
      padding: 6px 8px;
    }}
    .sprite-lab .icon-button svg {{
      width: 16px;
      height: 16px;
      fill: currentColor;
    }}
    .sprite-lab .thumbs {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(54px, 1fr));
      gap: 8px;
      padding: 6px;
      border-radius: 12px;
      border: 1px solid #1f2635;
      background: #0b0f16;
    }}
    .sprite-lab .thumb {{
      display: flex;
      flex-direction: column;
      align-items: center;
      gap: 4px;
      padding: 6px;
      border-radius: 10px;
      border: 1px solid transparent;
      background: rgba(18,24,36,0.8);
      cursor: pointer;
    }}
    .sprite-lab .thumb.active {{
      border-color: rgba(123,231,217,0.8);
      box-shadow: 0 0 12px rgba(123,231,217,0.2);
    }}
    .sprite-lab .thumb canvas {{
      width: 48px;
      height: 48px;
      image-rendering: pixelated;
    }}
    .sprite-lab .phase-list {{
      display: grid;
      gap: 6px;
      padding: 8px;
      border-radius: 10px;
      border: 1px solid #1f2635;
      background: #0b0f16;
      font-size: 0.78rem;
      color: #cfd6e5;
    }}
    .sprite-lab .phase-row {{
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
      padding: 6px 8px;
      border-radius: 8px;
      background: #121824;
      border: 1px solid transparent;
      cursor: grab;
    }}
    .sprite-lab .phase-row.dragging {{
      opacity: 0.6;
      cursor: grabbing;
    }}
    .sprite-lab .phase-row.drop-target {{
      border-color: rgba(123,231,217,0.8);
      box-shadow: 0 0 10px rgba(123,231,217,0.2);
    }}
    .sprite-lab .phase-row button {{
      width: auto;
      padding: 4px 8px;
    }}
    .sprite-lab .phase-row .meta {{
      display: flex;
      flex-direction: column;
      gap: 2px;
    }}
    .sprite-lab .player-row {{
      display: grid;
      grid-template-columns: minmax(220px, 320px) 1fr;
      gap: 12px;
      align-items: stretch;
    }}
    .sprite-lab .player-panel {{
      padding: 10px;
      border-radius: 12px;
      border: 1px solid #1f2635;
      background: #0b0f16;
    }}
    .sprite-lab .help {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      width: 22px;
      height: 22px;
      border-radius: 999px;
      border: 1px solid #4b5b72;
      background: #1a2433;
      color: #e7ecf3;
      font-weight: 700;
      cursor: pointer;
      position: relative;
      box-shadow: 0 0 0 1px rgba(123,231,217,0.2);
    }}
    .sprite-lab .help .help-text {{
      position: absolute;
      left: 0;
      top: 28px;
      width: 320px;
      padding: 12px 14px;
      border-radius: 12px;
      border: 1px solid #2f3b52;
      background: #0f1520;
      color: #e7ecf3;
      font-size: 0.8rem;
      line-height: 1.5;
      opacity: 0;
      pointer-events: none;
      transform: translateY(6px);
      transition: opacity 0.2s ease, transform 0.2s ease;
      z-index: 5;
      box-shadow: 0 12px 24px rgba(0,0,0,0.35);
    }}
    .sprite-lab .help:hover .help-text {{
      opacity: 1;
      transform: translateY(0);
      pointer-events: auto;
    }}
    .sprite-lab .help:focus-within .help-text {{
      opacity: 1;
      transform: translateY(0);
      pointer-events: auto;
    }}
    .sprite-lab.mobile {{
      gap: 10px;
    }}
    .sprite-lab.mobile .player-row {{
      grid-template-columns: 1fr;
    }}
    .sprite-lab.mobile .row {{
      grid-template-columns: 1fr;
    }}
    .sprite-lab.mobile .help .help-text {{
      width: 90vw;
      max-width: 360px;
    }}
    .sprite-lab.mobile .seq-controls button {{
      width: 100%;
    }}
    @media (max-width: 900px) {{
      .sprite-lab {{
        gap: 10px;
      }}
      .sprite-lab .player-row {{
        grid-template-columns: 1fr;
      }}
      .sprite-lab .row {{
        grid-template-columns: 1fr;
      }}
      .sprite-lab .help .help-text {{
        width: 90vw;
        max-width: 360px;
      }}
      .sprite-lab .seq-controls button {{
        width: 100%;
      }}
    }}
    .sprite-lab .btn-accent {{
      background: #1f2a3a;
      border-color: #3a465c;
      color: #f4f7ff;
      font-weight: 700;
    }}
    </style>
    <div class="sprite-lab">
      <div class="player-row">
        <div class="player-panel">
          <label>Preview</label>
          <div class="canvas-wrap" style="margin-top:6px;">
            <canvas id="spriteLabCanvas" width="256" height="256"></canvas>
          </div>
          <div class="status" id="spriteCurrentFrame">Frame --</div>
          <div class="pos-row">
            <span class="pos-pill">Position</span>
            <span id="spritePosX">x: 0</span>
            <span id="spritePosY">y: 0</span>
            <button id="spritePosReset" style="margin-left:auto;">Reset</button>
          </div>
          <div class="pos-row">
            <span class="pos-pill">Direction</span>
            <span id="spriteDirLabel">Dir: Default</span>
            <button id="spriteDirReset" style="margin-left:auto;">Reset</button>
          </div>
          <div class="status">Drag to rotate. Shift + drag to move.</div>
        </div>
        <div class="player-panel">
          <label>Status</label>
          <div class="status" id="spriteStatus">Loading manifest...</div>
          <div class="status" id="spritePlaybackStatus"></div>
          <div class="status" id="spriteSheetInfo"></div>
      <div class="status" id="spriteModeHint">Mode: Strip</div>
      <button id="spriteReload" class="btn-accent" style="margin-top:6px;">Reload manifest</button>
      <label style="display:flex;align-items:center;gap:6px;margin-top:6px;font-size:0.8rem;">
        <input id="spriteAutoRefresh" type="checkbox" />
        Auto-refresh manifest
      </label>
      <div class="status" id="spriteTabLog"></div>
    </div>
      </div>
      <div class="status">
        <span class="help">?
          <span class="help-text">
            Choose a mode, press Play. Strip plays raw frames; Phases plays your saved phase timing (pause/fade); Sequence plays the queued actions from Sequence Builder. Use FPS to adjust speed.
          </span>
        </span>
      </div>
      <div class="status">
        Mobile layout
        <input id="spriteMobileToggle" type="checkbox" style="margin-left:6px;" />
      </div>
      <div class="row">
        <div>
          <label>Character</label>
          <select id="spriteCharSelect"></select>
        </div>
        <div>
          <label>Action</label>
          <select id="spriteActionSelect"></select>
        </div>
        <div>
          <label>FPS</label>
          <input id="spriteFps" type="range" min="1" max="20" value="8" />
          <div class="status" id="spriteFpsValue">8 fps</div>
        </div>
        <div>
          <label>Playback</label>
          <button id="spritePlayToggle">Play</button>
        </div>
      </div>
      <div class="row">
        <div>
          <label>Action Filter</label>
          <input id="spriteActionFilter" class="input" type="text" placeholder="Search name or tags" />
          <div class="status" id="spriteActionCount"></div>
        </div>
        <div>
          <label>Clone From</label>
          <select id="spriteCloneSelect"></select>
        </div>
        <div>
          <label>Clone</label>
          <button id="spriteCloneAction">Clone Labels</button>
        </div>
        <div>
          <label>Confidence</label>
          <input id="spriteConfidence" type="range" min="0" max="2" value="0" />
          <div class="status" id="spriteConfidenceValue">Draft</div>
        </div>
      </div>
      <div class="row">
        <div>
          <label>Frame</label>
          <input id="spriteFrame" type="range" min="0" max="0" value="0" />
          <div class="status" id="spriteFrameValue">Frame 0</div>
        </div>
        <div>
          <label>Mode</label>
          <select id="spritePlayMode">
            <option value="strip">Strip</option>
            <option value="phases">Phases</option>
            <option value="sequence">Sequence</option>
          </select>
        </div>
      </div>
      <div class="grid-2">
        <div>
          <label>Action Name</label>
          <input id="spriteActionName" class="input" type="text" placeholder="e.g., Run Attack" />
        </div>
        <div>
          <label>Tags</label>
          <input id="spriteActionTags" class="input" type="text" placeholder="e.g., combat, fast, windy" />
        </div>
      </div>
      <div>
        <label>Description</label>
        <textarea id="spriteActionDesc" class="input" placeholder="Notes about timing, intent, mood..."></textarea>
      </div>
      <div class="row">
        <div>
          <label>Phase Name</label>
          <input id="spritePhaseName" class="input" type="text" placeholder="wind-up, impact, recover" />
        </div>
        <div>
          <label>Start Frame</label>
          <input id="spritePhaseStart" class="input" type="number" min="0" value="0" />
        </div>
        <div>
          <label>End Frame</label>
          <input id="spritePhaseEnd" class="input" type="number" min="0" value="0" />
        </div>
        <div>
          <label>Pause (sec)</label>
          <input id="spritePhasePause" class="input" type="number" min="0" step="0.1" value="0" />
        </div>
        <div>
          <label>Fade (sec)</label>
          <input id="spritePhaseFade" class="input" type="number" min="0" step="0.1" value="0" />
        </div>
        <div>
          <label>Phase Controls</label>
          <button id="spritePhaseAdd">Add Phase</button>
        </div>
      </div>
      <div>
        <div class="section-title">
          <span>Phases</span>
          <span class="chip">Step 2</span>
        </div>
        <div class="drag-hint">Drag to reorder phases.</div>
        <div class="status" id="spritePhaseCount">0 phases</div>
        <div class="phase-list" id="spritePhaseList">No phases yet.</div>
        <div class="status" id="spritePhaseSummary"></div>
      </div>
      <div style="margin-top:12px;">
        <div class="section-title">
          <span>Sequence Builder</span>
          <span class="chip">Step 3</span>
        </div>
        <div class="drag-hint">Drag to reorder the action queue.</div>
        <div class="seq-controls" style="display:flex; gap:8px; flex-wrap:wrap; align-items:center;">
          <button id="spriteSeqAdd" class="btn-accent" style="flex:1; min-width:140px;">Add Action to Sequence</button>
          <button id="spriteSeqPlay" style="flex:1; min-width:140px;">Play Sequence</button>
          <button id="spriteSeqClear" style="flex:1; min-width:120px;">Clear Sequence</button>
        </div>
        <div class="status">Queue actions from the current Character/Action with your phases, then play in Sequence mode.</div>
        <div class="log" id="spriteSeqLog">No sequence yet.</div>
      </div>
      <div class="row">
        <div>
          <button id="spriteSaveAction">Save Label</button>
        </div>
        <div>
          <button id="spriteDownloadLabels">Download JSON</button>
        </div>
        <div>
          <button id="spriteCopyPhases" class="btn-accent">Copy Phases JSON</button>
        </div>
        <div class="toggle">
          <input id="spriteApplyAllFrames" type="checkbox" />
          <label for="spriteApplyAllFrames">Apply to all frames</label>
        </div>
        <div>
          <button id="spriteCopyLabels" class="icon-button" title="Copy JSON">
            <svg viewBox="0 0 24 24" aria-hidden="true">
              <path d="M16 1H6a2 2 0 0 0-2 2v12h2V3h10V1zm3 4H10a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h9a2 2 0 0 0 2-2V7a2 2 0 0 0-2-2zm0 16H10V7h9v14z"/>
            </svg>
            Copy
          </button>
        </div>
        <div class="status" id="spriteSaveStatus"></div>
      </div>
      <div>
        <label>Saved Phases</label>
        <div class="log" id="spriteSavedPhases">No saved phases yet.</div>
      </div>
      <div>
        <label>Frame Thumbnails (click to jump)</label>
        <div class="thumbs" id="spriteThumbs"></div>
      </div>
      <div>
        <label>Saved Labels</label>
        <div class="log" id="spriteLabelLog">No labels saved yet.</div>
      </div>
    </div>
    <script>{sprite_js}</script>
    <script>
      window.__spriteManifest = {sprite_manifest};
      window.__spriteImages = {json.dumps(sprite_images)};
    </script>
    <script>
      (function() {{
        const statusEl = document.getElementById("spriteStatus");
        const playbackStatusEl = document.getElementById("spritePlaybackStatus");
        const infoEl = document.getElementById("spriteSheetInfo");
        const currentFrameEl = document.getElementById("spriteCurrentFrame");
        const charSelect = document.getElementById("spriteCharSelect");
        const actionSelect = document.getElementById("spriteActionSelect");
        const actionFilter = document.getElementById("spriteActionFilter");
        const actionCount = document.getElementById("spriteActionCount");
        const cloneSelect = document.getElementById("spriteCloneSelect");
        const cloneBtn = document.getElementById("spriteCloneAction");
        const confidenceRange = document.getElementById("spriteConfidence");
        const confidenceValue = document.getElementById("spriteConfidenceValue");
        const fpsRange = document.getElementById("spriteFps");
        const fpsValue = document.getElementById("spriteFpsValue");
        const frameRange = document.getElementById("spriteFrame");
        const frameValue = document.getElementById("spriteFrameValue");
        const playToggle = document.getElementById("spritePlayToggle");
        const playModeSelect = document.getElementById("spritePlayMode");
        const mobileToggle = document.getElementById("spriteMobileToggle");
        const reloadBtn = document.getElementById("spriteReload");
        const autoRefreshToggle = document.getElementById("spriteAutoRefresh");
        const rootEl = document.querySelector(".sprite-lab");
        const canvas = document.getElementById("spriteLabCanvas");
        const ctx = canvas.getContext("2d");
        const posXEl = document.getElementById("spritePosX");
        const posYEl = document.getElementById("spritePosY");
        const posResetBtn = document.getElementById("spritePosReset");
        const dirLabelEl = document.getElementById("spriteDirLabel");
        const dirResetBtn = document.getElementById("spriteDirReset");
        const nameInput = document.getElementById("spriteActionName");
        const tagsInput = document.getElementById("spriteActionTags");
        const descInput = document.getElementById("spriteActionDesc");
        const phaseNameInput = document.getElementById("spritePhaseName");
        const phaseStartInput = document.getElementById("spritePhaseStart");
        const phaseEndInput = document.getElementById("spritePhaseEnd");
        const phasePauseInput = document.getElementById("spritePhasePause");
        const phaseFadeInput = document.getElementById("spritePhaseFade");
        const phaseAddBtn = document.getElementById("spritePhaseAdd");
        const phaseListEl = document.getElementById("spritePhaseList");
        const phaseCountEl = document.getElementById("spritePhaseCount");
        const phaseSummaryEl = document.getElementById("spritePhaseSummary");
        const seqAddBtn = document.getElementById("spriteSeqAdd");
        const seqPlayBtn = document.getElementById("spriteSeqPlay");
        const seqClearBtn = document.getElementById("spriteSeqClear");
        const seqLogEl = document.getElementById("spriteSeqLog");
        const saveBtn = document.getElementById("spriteSaveAction");
        const downloadBtn = document.getElementById("spriteDownloadLabels");
        const copyPhasesBtn = document.getElementById("spriteCopyPhases");
        const copyBtn = document.getElementById("spriteCopyLabels");
        const applyAllFrames = document.getElementById("spriteApplyAllFrames");
        const saveStatus = document.getElementById("spriteSaveStatus");
        const savedPhasesEl = document.getElementById("spriteSavedPhases");
        const labelLog = document.getElementById("spriteLabelLog");
        const statusLines = [];
        const STATE_KEY = "spriteLabState";
        const POSITION_KEY = "spriteLabPositions";
        const modeHintEl = document.getElementById("spriteModeHint");
        const thumbsEl = document.getElementById("spriteThumbs");
        const tabLogEl = document.getElementById("spriteTabLog");

        let manifest = null;
        let imagesBySheet = {{}};
        let playing = false;
        let playMode = "strip";
        let sheet = null;
        let frameIdx = 0;
        let fps = 8;
        let last = performance.now();
        let phases = [];
        let phaseState = null;
        let sequence = [];
        let sequenceState = null;
        let dragPhaseIndex = null;
        let dragSeqIndex = null;
        let spriteOffset = {{ x: 0, y: 0 }};
        let dragPosition = null;
        let directionIndex = null;
        let currentDirection = "Default";
        let flipX = false;
        let directionLock = false;

        const DIR_TOKENS = [
          {{ key: "Right", angle: 0, aliases: ["right", "r", "e"] }},
          {{ key: "UpRight", angle: 45, aliases: ["upright", "up_right", "ur", "ne"] }},
          {{ key: "Up", angle: 90, aliases: ["up", "u", "n"] }},
          {{ key: "UpLeft", angle: 135, aliases: ["upleft", "up_left", "ul", "nw"] }},
          {{ key: "Left", angle: 180, aliases: ["left", "l", "w"] }},
          {{ key: "DownLeft", angle: -135, aliases: ["downleft", "down_left", "dl", "sw"] }},
          {{ key: "Down", angle: -90, aliases: ["down", "d", "s"] }},
          {{ key: "DownRight", angle: -45, aliases: ["downright", "down_right", "dr", "se"] }},
        ];

        function normalizeToken(text) {{
          return String(text || "").toLowerCase().replace(/[^a-z0-9]/g, "");
        }}

        function parseActionPart(actionPart) {{
          const normalized = String(actionPart || "").replace(/[-\\s]+/g, "_");
          const parts = normalized.split("_").filter(Boolean);
          const joined = parts.join("_");
          const suffixes = [];
          if (parts.length) {{
            suffixes.push({{ value: parts[parts.length - 1], count: 1 }});
          }}
          if (parts.length >= 2) {{
            suffixes.push({{ value: parts.slice(-2).join("_"), count: 2 }});
          }}
          for (const suffix of suffixes) {{
            const candidate = normalizeToken(suffix.value);
            const token = DIR_TOKENS.find((dir) => {{
              const aliases = [dir.key, ...dir.aliases].map(normalizeToken);
              return aliases.includes(candidate);
            }});
            if (token) {{
              const baseParts = parts.slice(0, parts.length - suffix.count);
              return {{
                base: baseParts.join("_") || joined,
                direction: token.key,
              }};
            }}
          }}
          return {{ base: joined, direction: null }};
        }}

        function buildDirectionIndex() {{
          const index = {{}};
          manifest.sheets.forEach((s) => {{
            if (!s.name) return;
            const parts = s.name.split("/");
            if (parts.length < 2) return;
            const charName = parts[0];
            const actionPart = parts.slice(1).join("/");
            const meta = parseActionPart(actionPart);
            const key = charName + "/" + meta.base;
            if (!index[key]) {{
              index[key] = {{ defaultName: null, variants: {{}} }};
            }}
            if (meta.direction) {{
              index[key].variants[meta.direction] = s.name;
            }} else {{
              index[key].defaultName = s.name;
            }}
          }});
          return index;
        }}

        function getActionMeta(fullName) {{
          if (!fullName) return null;
          const parts = fullName.split("/");
          if (parts.length < 2) return null;
          const charName = parts[0];
          const actionPart = parts.slice(1).join("/");
          const meta = parseActionPart(actionPart);
          return {{ key: charName + "/" + meta.base, base: meta.base, direction: meta.direction, char: charName }};
        }}

        function updateDirectionDisplay() {{
          if (!dirLabelEl) return;
          const meta = getActionMeta(sheet ? sheet.name : "");
          const label = currentDirection && currentDirection !== "Default"
            ? currentDirection
            : (meta && meta.direction ? meta.direction : "Default");
          dirLabelEl.textContent = "Dir: " + label;
        }}

        function setDirection(targetDir) {{
          const meta = getActionMeta(sheet ? sheet.name : "");
          if (!meta || !directionIndex) return;
          const entry = directionIndex[meta.key];
          if (!entry) {{
            logDebug("No direction variants for " + meta.base);
            return;
          }}
          const targetName = entry.variants[targetDir] || null;
          const shouldFlip = targetDir === "Left" || targetDir === "UpLeft" || targetDir === "DownLeft";
          currentDirection = targetDir;
          if (targetName) {{
            if (sheet && sheet.name === targetName) {{
              flipX = false;
              updateDirectionDisplay();
              persistActionState();
              return;
            }}
            flipX = false;
            actionSelect.value = targetName;
            setSheetByName(targetName);
            drawFrame(frameIdx);
            updateDirectionDisplay();
            persistActionState();
            saveState();
            return;
          }}
          flipX = shouldFlip;
          drawFrame(frameIdx);
          updateDirectionDisplay();
          persistActionState();
          saveState();
        }}

        function directionFromAngle(angle) {{
          let best = DIR_TOKENS[0];
          let bestDelta = 999;
          DIR_TOKENS.forEach((dir) => {{
            const delta = Math.abs((((angle - dir.angle) + 540) % 360) - 180);
            if (delta < bestDelta) {{
              bestDelta = delta;
              best = dir;
            }}
          }});
          return best.key;
        }}

        function loadPositions() {{
          try {{
            const raw = localStorage.getItem(POSITION_KEY);
            if (raw) return JSON.parse(raw);
          }} catch (e) {{
            logDebug("Position load failed: " + e);
          }}
          return {{}};
        }}

        function savePositions(map) {{
          try {{
            localStorage.setItem(POSITION_KEY, JSON.stringify(map));
          }} catch (e) {{
            logDebug("Position save failed: " + e);
          }}
        }}

        function getPositionKey() {{
          const meta = getActionMeta(sheet ? sheet.name : "");
          return meta ? meta.key : (sheet ? sheet.name : null);
        }}

        function applyActionStateFromStore() {{
          const key = getPositionKey();
          if (!key) return null;
          const map = loadPositions();
          const entry = map[key];
          spriteOffset = {{
            x: entry && Number.isFinite(entry.x) ? entry.x : 0,
            y: entry && Number.isFinite(entry.y) ? entry.y : 0,
          }};
          currentDirection = entry && entry.direction ? entry.direction : "Default";
          flipX = Boolean(entry && entry.flip);
          updatePositionDisplay();
          updateDirectionDisplay();
          if (entry && entry.direction && entry.direction !== "Default") {{
            return entry.direction;
          }}
          return null;
        }}

        function persistActionState() {{
          const key = getPositionKey();
          if (!key) return;
          const map = loadPositions();
          map[key] = {{
            x: spriteOffset.x,
            y: spriteOffset.y,
            direction: currentDirection,
            flip: flipX,
          }};
          savePositions(map);
        }}

        function updatePositionDisplay() {{
          if (posXEl) posXEl.textContent = "x: " + Math.round(spriteOffset.x);
          if (posYEl) posYEl.textContent = "y: " + Math.round(spriteOffset.y);
        }}

        function moveItem(list, fromIndex, toIndex) {{
          if (!Array.isArray(list)) return;
          if (fromIndex === null || fromIndex === undefined) return;
          if (toIndex === null || toIndex === undefined) return;
          if (fromIndex === toIndex) return;
          if (fromIndex < 0 || fromIndex >= list.length) return;
          const [item] = list.splice(fromIndex, 1);
          const target = Math.min(Math.max(toIndex, 0), list.length);
          list.splice(target, 0, item);
        }}

        function clearDropTargets(container) {{
          if (!container) return;
          container.querySelectorAll(".drop-target").forEach((el) => {{
            el.classList.remove("drop-target");
          }});
        }}

        function renderStatus() {{
          if (!statusEl) return;
          statusEl.textContent = statusLines.slice(0, 6).join("\\n");
        }}

        function setStatus(text) {{
          statusLines.unshift(text);
          renderStatus();
        }}

        function logDebug(msg) {{
          const stamp = new Date().toLocaleTimeString();
          statusLines.unshift("[" + stamp + "] " + msg);
          renderStatus();
        }}

        function refreshTabLog() {{
          if (!tabLogEl) return;
          try {{
            const raw = localStorage.getItem("sprite:tab_log") || "";
            tabLogEl.textContent = raw;
          }} catch (e) {{}}
        }}

        function setPlaybackStatus(text) {{
          if (!playbackStatusEl) return;
          playbackStatusEl.textContent = text;
        }}

        function updateModeHint() {{
          if (!modeHintEl) return;
          const label = playMode === "sequence" ? "Sequence" : playMode === "phases" ? "Phases" : "Strip";
          modeHintEl.textContent = "Mode: " + label;
        }}

        function applyMobileLayout() {{
          if (!rootEl || !mobileToggle) return;
          const enable = Boolean(mobileToggle.checked);
          if (enable) {{
            rootEl.classList.add("mobile");
          }} else {{
            rootEl.classList.remove("mobile");
          }}
        }}

        function getConfidenceLabel(value) {{
          const labels = ["Draft", "Reviewed", "Final"];
          return labels[value] || labels[0];
        }}

        function updateConfidenceLabel() {{
          confidenceValue.textContent = getConfidenceLabel(Number(confidenceRange.value));
        }}

        function getLabelKey() {{
          if (!sheet) return null;
          return sheet.name;
        }}

        function loadLabels() {{
          try {{
            const raw = localStorage.getItem("spriteLabels");
            if (raw) return JSON.parse(raw);
          }} catch (e) {{
            logDebug("localStorage read failed: " + e);
          }}
          if (!window.__spriteLabelCache) {{
            window.__spriteLabelCache = {{}};
          }}
          return window.__spriteLabelCache;
        }}

        function saveLabels(data) {{
          window.__spriteLabelCache = data;
          try {{
            localStorage.setItem("spriteLabels", JSON.stringify(data, null, 2));
          }} catch (e) {{
            logDebug("localStorage write failed: " + e);
          }}
        }}

        function refreshLabelLog() {{
          const labels = loadLabels();
          const keys = Object.keys(labels);
          if (!keys.length) {{
            labelLog.textContent = "No labels saved yet.";
            return;
          }}
          const lines = keys.sort().map((key) => {{
            const entry = labels[key];
            const conf = entry && Number.isFinite(entry.confidence) ? getConfidenceLabel(entry.confidence) : "Draft";
            return key + " -> " + (entry.name || "Untitled") + " [" + conf + "]";
          }});
          labelLog.textContent = lines.join("\\n");
        }}

        function renderSavedPhases() {{
          if (!savedPhasesEl) return;
          const labels = loadLabels();
          const rows = [];
          Object.entries(labels).forEach(([key, entry]) => {{
            if (!entry || !Array.isArray(entry.phases) || !entry.phases.length) return;
            entry.phases.forEach((phase) => {{
              rows.push({{
                action: key,
                phase: phase.name || "Unnamed",
                start: phase.start,
                end: phase.end,
                pause: phase.pause_ms ? (phase.pause_ms / 1000).toFixed(1) + "s" : "0s",
                fade: phase.fade_ms ? (phase.fade_ms / 1000).toFixed(1) + "s" : "0s",
                confidence: Number.isFinite(entry.confidence) ? getConfidenceLabel(entry.confidence) : "Draft",
                updated: entry.updated_at || "",
              }});
            }});
          }});
          if (!rows.length) {{
            savedPhasesEl.textContent = "No saved phases yet.";
            return;
          }}
          const header = "Action | Phase | Start | End | Pause | Fade | Confidence | Updated";
          const body = rows.map((row) =>
            row.action + " | " + row.phase + " | " + row.start + " | " + row.end + " | " +
            row.pause + " | " + row.fade + " | " + row.confidence + " | " + row.updated
          );
          savedPhasesEl.textContent = [header].concat(body).join("\\n");
        }}

        function loadLabelFields() {{
          const labels = loadLabels();
          const key = getLabelKey();
          const entry = key ? labels[key] : null;
          nameInput.value = entry?.name || "";
          tagsInput.value = entry?.tags || "";
          descInput.value = entry?.description || "";
          applyAllFrames.checked = Boolean(entry?.apply_all_frames);
          confidenceRange.value = String(entry?.confidence ?? 0);
          updateConfidenceLabel();
          phases = Array.isArray(entry?.phases) ? entry.phases.slice() : [];
          renderPhases();
        }}

        function listCharacters() {{
          if (!manifest || !manifest.sheets) return [];
          const names = new Set();
          manifest.sheets.forEach((s) => {{
            if (!s.name) return;
            const parts = s.name.split("/");
            if (parts.length >= 2) {{
              names.add(parts[0]);
            }}
          }});
          return Array.from(names).sort();
        }}

        function listActions(charName) {{
          if (!manifest || !manifest.sheets || !charName) return [];
          const labels = loadLabels();
          const term = (actionFilter.value || "").trim().toLowerCase();
          return manifest.sheets.filter((s) => {{
            if (!s.name || !s.name.startsWith(charName + "/")) return false;
            if (!(s.type === "strip" || s.type === "single" || s.type === "atlas")) return false;
            if (!term) return true;
            const label = labels[s.name] || {{}};
            const hay = [
              s.name,
              label.name || "",
              label.tags || "",
              label.description || "",
            ].join(" ").toLowerCase();
            return hay.includes(term);
          }});
        }}

        function populateCharacters() {{
          const chars = listCharacters();
          charSelect.innerHTML = "";
          if (!chars.length) {{
            const opt = document.createElement("option");
            opt.value = "";
            opt.textContent = "No characters found";
            charSelect.appendChild(opt);
            return;
          }}
          chars.forEach((name) => {{
            const opt = document.createElement("option");
            opt.value = name;
            opt.textContent = name;
            charSelect.appendChild(opt);
          }});
          charSelect.value = chars[0];
        }}

        function populateActions() {{
          const actions = listActions(charSelect.value);
          actionSelect.innerHTML = "";
          actions.forEach((s) => {{
            const label = s.name.split("/")[1];
            const opt = document.createElement("option");
            opt.value = s.name;
            opt.textContent = label;
            actionSelect.appendChild(opt);
          }});
          if (actions.length) {{
            actionSelect.value = actions[0].name;
          }}
          actionCount.textContent = actions.length ? (actions.length + " action(s)") : "No actions found";
          populateCloneOptions(actions);
        }}

        function applyManifest(data) {{
          manifest = data;
          directionIndex = buildDirectionIndex();
          populateCharacters();
          populateActions();
          if (charSelect.value && actionSelect.value) {{
            setSheetByName(actionSelect.value);
          }}
          if (!manifest || !manifest.sheets || !manifest.sheets.length) {{
            setStatus("Manifest loaded: no sheets found.");
          }} else {{
            setStatus("Manifest loaded: " + manifest.sheets.length + " sheets");
          }}
        }}

        function reloadManifest() {{
          setStatus("Reloading manifest...");
          window.SpriteSheetPlayer.loadSpriteManifest()
            .then((data) => {{
              applyManifest(data);
              if (window.__spriteImages && Object.keys(window.__spriteImages).length) {{
                return loadInlineImages(window.__spriteImages);
              }}
              return window.SpriteSheetPlayer.loadSheetImages(manifest);
            }})
            .then((result) => {{
              imagesBySheet = result.imagesBySheet || {{}};
              renderThumbnails();
              refreshLabelLog();
            }})
            .catch((err) => {{
              console.error(err);
              setStatus("Reload failed.");
            }});
        }}

        function populateCloneOptions(actions) {{
          const labels = loadLabels();
          cloneSelect.innerHTML = "";
          const emptyOpt = document.createElement("option");
          emptyOpt.value = "";
          emptyOpt.textContent = "Select source action";
          cloneSelect.appendChild(emptyOpt);
          actions.forEach((s) => {{
            const opt = document.createElement("option");
            const label = labels[s.name]?.name;
            opt.value = s.name;
            opt.textContent = label ? (s.name.split("/")[1] + " (" + label + ")") : s.name.split("/")[1];
            cloneSelect.appendChild(opt);
          }});
        }}

        function renderPhases() {{
          phaseListEl.innerHTML = "";
          if (!phases.length) {{
            phaseListEl.textContent = "No phases yet.";
            if (phaseCountEl) {{
              phaseCountEl.textContent = "0 phases";
            }}
            if (phaseSummaryEl) {{
              phaseSummaryEl.textContent = "";
            }}
            logDebug("Render phases: none");
            saveState();
            return;
          }}
          if (phaseCountEl) {{
            phaseCountEl.textContent = phases.length + " phase(s)";
          }}
          if (phaseSummaryEl) {{
            const summary = phases.map((phase) => phase.name + " " + phase.start + "-" + phase.end).join(" | ");
            phaseSummaryEl.textContent = summary;
          }}
          logDebug("Render phases: " + phases.length);
          phases.forEach((phase, idx) => {{
            const row = document.createElement("div");
            row.className = "phase-row";
            row.draggable = true;
            row.dataset.index = String(idx);
            const text = document.createElement("div");
            text.className = "meta";
            const line1 = document.createElement("div");
            line1.textContent = phase.name + " (frames " + phase.start + "-" + phase.end + ")";
            const pause = phase.pause_ms ? (phase.pause_ms / 1000).toFixed(1) : "0.0";
            const fade = phase.fade_ms ? (phase.fade_ms / 1000).toFixed(1) : "0.0";
            const line2 = document.createElement("div");
            line2.textContent = "pause " + pause + "s, fade " + fade + "s";
            text.appendChild(line1);
            text.appendChild(line2);
            const removeBtn = document.createElement("button");
            removeBtn.textContent = "Remove";
            removeBtn.addEventListener("click", () => {{
              phases.splice(idx, 1);
              renderPhases();
            }});
            row.appendChild(text);
            row.appendChild(removeBtn);
            phaseListEl.appendChild(row);
          }});
          saveState();
        }}

        function renderSequence() {{
          if (!seqLogEl) return;
          seqLogEl.innerHTML = "";
          if (!sequence.length) {{
            seqLogEl.textContent = "No sequence yet.";
            saveState();
            return;
          }}
          const list = document.createElement("div");
          sequence.forEach((item, idx) => {{
            const row = document.createElement("div");
            row.className = "phase-row";
            row.draggable = true;
            row.dataset.index = String(idx);
            const text = document.createElement("div");
            text.className = "meta";
            text.textContent = (idx + 1) + ". " + item.action + " (" + (item.phases ? item.phases.length : 0) + " phases)";
            const removeBtn = document.createElement("button");
            removeBtn.textContent = "Remove";
            removeBtn.addEventListener("click", () => {{
              sequence.splice(idx, 1);
              renderSequence();
            }});
            row.appendChild(text);
            row.appendChild(removeBtn);
            list.appendChild(row);
          }});
          seqLogEl.appendChild(list);
          saveState();
        }}

        function saveState() {{
          try {{
            const payload = {{
              char: charSelect.value,
              action: actionSelect.value,
              filter: actionFilter.value,
              fps: fps,
              mode: playMode,
              confidence: Number(confidenceRange.value),
              name: nameInput.value,
              tags: tagsInput.value,
              desc: descInput.value,
              frame: frameIdx,
              apply_all_frames: applyAllFrames.checked,
              mobile: mobileToggle ? mobileToggle.checked : false,
              direction: currentDirection,
              flip: flipX,
              phaseDraft: {{
                name: phaseNameInput ? phaseNameInput.value : "",
                start: phaseStartInput ? phaseStartInput.value : "",
                end: phaseEndInput ? phaseEndInput.value : "",
                pause: phasePauseInput ? phasePauseInput.value : "",
                fade: phaseFadeInput ? phaseFadeInput.value : "",
              }},
              phases: phases,
              sequence: sequence,
            }};
            localStorage.setItem(STATE_KEY, JSON.stringify(payload));
          }} catch (e) {{
            logDebug("State save failed: " + e);
          }}
        }}

        function restoreState() {{
          try {{
            const raw = localStorage.getItem(STATE_KEY);
            if (!raw) return false;
            const state = JSON.parse(raw);
            if (state.filter !== undefined) actionFilter.value = state.filter;
            if (state.fps !== undefined) {{
              fps = state.fps;
              fpsRange.value = String(fps);
              fpsValue.textContent = fps + " fps";
            }}
            if (state.mode) {{
              playMode = state.mode;
              playModeSelect.value = playMode;
              updateModeHint();
            }}
            if (state.char) {{
              charSelect.value = state.char;
            }}
            populateActions();
            if (state.action) {{
              actionSelect.value = state.action;
            }}
            setSheetByName(actionSelect.value);
            if (state.confidence !== undefined) {{
              confidenceRange.value = String(state.confidence);
              updateConfidenceLabel();
            }}
            if (state.name !== undefined) nameInput.value = state.name;
            if (state.tags !== undefined) tagsInput.value = state.tags;
            if (state.desc !== undefined) descInput.value = state.desc;
            if (state.apply_all_frames !== undefined) {{
              applyAllFrames.checked = Boolean(state.apply_all_frames);
            }}
            if (state.direction !== undefined) {{
              currentDirection = state.direction || "Default";
            }}
            if (state.flip !== undefined) {{
              flipX = Boolean(state.flip);
            }}
            if (state.phaseDraft) {{
              if (phaseNameInput) phaseNameInput.value = state.phaseDraft.name || "";
              if (phaseStartInput) phaseStartInput.value = state.phaseDraft.start || "";
              if (phaseEndInput) phaseEndInput.value = state.phaseDraft.end || "";
              if (phasePauseInput) phasePauseInput.value = state.phaseDraft.pause || "";
              if (phaseFadeInput) phaseFadeInput.value = state.phaseDraft.fade || "";
            }}
            if (Array.isArray(state.phases)) phases = state.phases;
            if (Array.isArray(state.sequence)) sequence = state.sequence;
            if (state.frame !== undefined && sheet) {{
              const maxFrame = Math.max(0, (sheet.frames || 1) - 1);
              frameIdx = Math.min(Math.max(Number(state.frame) || 0, 0), maxFrame);
              frameRange.value = String(frameIdx);
              frameValue.textContent = "Frame " + frameIdx;
              drawFrame(frameIdx);
            }}
            if (state.mobile !== undefined && mobileToggle) {{
              mobileToggle.checked = Boolean(state.mobile);
              applyMobileLayout();
            }}
            renderPhases();
            renderSequence();
            return true;
          }} catch (e) {{
            logDebug("State restore failed: " + e);
          }}
          return false;
        }}

        function renderThumbnails() {{
          thumbsEl.innerHTML = "";
          if (!sheet) return;
          const count = sheet.frames || 1;
          const size = 48;
          const scale = size / (manifest.frame_size || 128);
          for (let i = 0; i < count; i += 1) {{
            const item = document.createElement("div");
            item.className = "thumb";
            item.dataset.index = String(i);
            const canvasEl = document.createElement("canvas");
            canvasEl.width = size;
            canvasEl.height = size;
            const c = canvasEl.getContext("2d");
            const frameName = sheet.name + "/" + i;
            window.SpriteSheetPlayer.drawFrameByName(
              c,
              imagesBySheet,
              manifest,
              frameName,
              0,
              0,
              scale,
              1
            );
            item.appendChild(canvasEl);
            item.addEventListener("click", () => {{
              frameIdx = i;
              frameRange.value = String(frameIdx);
              frameValue.textContent = "Frame " + frameIdx;
              drawFrame(frameIdx);
            }});
            thumbsEl.appendChild(item);
          }}
          setActiveThumb(frameIdx);
        }}

        function setActiveThumb(index) {{
          const nodes = thumbsEl.querySelectorAll(".thumb");
          nodes.forEach((node) => {{
            const idx = Number(node.dataset.index);
            if (idx === index) {{
              node.classList.add("active");
            }} else {{
              node.classList.remove("active");
            }}
          }});
        }}

        function setSheetByName(name) {{
          sheet = manifest.sheets.find((s) => s.name === name) || null;
          frameIdx = 0;
          phaseState = null;
          sequenceState = null;
          const maxFrame = Math.max(0, (sheet && sheet.frames ? sheet.frames : 1) - 1);
          frameRange.max = String(maxFrame);
          frameRange.value = "0";
          frameValue.textContent = "Frame 0";
          if (sheet) {{
            infoEl.textContent = "Sheet: " + sheet.name + " | Frames: " + (sheet.frames || 1);
            phaseStartInput.max = String(maxFrame);
            phaseEndInput.max = String(maxFrame);
            phaseEndInput.value = String(maxFrame);
            loadLabelFields();
            const storedDir = applyActionStateFromStore();
            if (storedDir && directionIndex && !directionLock) {{
              const meta = getActionMeta(sheet.name);
              const entry = meta ? directionIndex[meta.key] : null;
              const targetName = entry && entry.variants ? entry.variants[storedDir] : null;
              if (targetName && targetName !== sheet.name) {{
                directionLock = true;
                setSheetByName(targetName);
                directionLock = false;
                return;
              }}
            }}
            if (Object.keys(imagesBySheet).length) {{
              renderThumbnails();
            }}
          }} else {{
            infoEl.textContent = "";
          }}
        }}

        function drawFrame(idx, alpha = 1) {{
          ctx.clearRect(0, 0, canvas.width, canvas.height);
          if (!sheet) return;
          const frameName = sheet.name + "/" + idx;
          const size = (manifest.frame_size || 128);
          const dx = (canvas.width - size) / 2 + spriteOffset.x;
          const dy = (canvas.height - size) / 2 + spriteOffset.y;
          let ok = false;
          if (flipX) {{
            ctx.save();
            ctx.translate(canvas.width, 0);
            ctx.scale(-1, 1);
            const mirroredDx = canvas.width - dx - size;
            ok = window.SpriteSheetPlayer.drawFrameByName(
              ctx,
              imagesBySheet,
              manifest,
              frameName,
              mirroredDx,
              dy,
              1,
              alpha
            );
            ctx.restore();
          }} else {{
            ok = window.SpriteSheetPlayer.drawFrameByName(
              ctx,
              imagesBySheet,
              manifest,
              frameName,
              dx,
              dy,
              1,
              alpha
            );
          }}
          if (!ok) {{
            setPlaybackStatus("Missing image for " + sheet.name);
          }} else {{
            setPlaybackStatus("Showing " + frameName);
          }}
          setActiveThumb(idx);
          if (currentFrameEl) {{
            currentFrameEl.textContent = "Frame " + idx + " / " + (sheet.frames || 1);
          }}
          updatePositionDisplay();
        }}

        function tick(now) {{
          const dt = (now - last) / 1000;
          last = now;
          if (playing && playMode === "sequence" && sequenceState && sequence.length) {{
            const current = sequence[sequenceState.actionIndex];
            if (!current || !current.phases || !current.phases.length) {{
              sequenceState = null;
            }} else {{
              const phase = current.phases[sequenceState.phaseIndex];
              if (!phase) {{
                sequenceState = null;
              }} else {{
                if (!sequenceState.started) {{
                  sequenceState.started = true;
                  frameIdx = phase.start;
                  frameRange.value = String(frameIdx);
                  frameValue.textContent = "Frame " + frameIdx;
                  drawFrame(frameIdx, 1);
                }} else if (sequenceState.holding) {{
                  const pauseMs = phase.pause_ms || 0;
                  const fadeMs = phase.fade_ms || 0;
                  const totalHold = pauseMs + fadeMs;
                  sequenceState.holdElapsed += dt * 1000;
                  let alpha = 1;
                  if (fadeMs > 0 && sequenceState.holdElapsed > pauseMs) {{
                    const fadeProgress = Math.min(1, (sequenceState.holdElapsed - pauseMs) / fadeMs);
                    alpha = Math.max(0, 1 - fadeProgress);
                  }}
                  drawFrame(sequenceState.frame, alpha);
                  if (sequenceState.holdElapsed >= totalHold) {{
                    sequenceState.phaseIndex += 1;
                    const nextPhase = current.phases[sequenceState.phaseIndex];
                    if (!nextPhase) {{
                      sequenceState.actionIndex += 1;
                      sequenceState.phaseIndex = 0;
                      sequenceState.started = false;
                      sequenceState.holding = false;
                      sequenceState.holdElapsed = 0;
                      if (sequenceState.actionIndex >= sequence.length) {{
                        sequenceState = null;
                      }}
                    }} else {{
                      sequenceState.frame = nextPhase.start;
                      sequenceState.timer = 0;
                      sequenceState.holding = false;
                      sequenceState.holdElapsed = 0;
                      frameIdx = nextPhase.start;
                      frameRange.value = String(frameIdx);
                      frameValue.textContent = "Frame " + frameIdx;
                      drawFrame(frameIdx, 1);
                    }}
                  }}
                }} else {{
                  const step = fps > 0 ? 1 / fps : 0.2;
                  sequenceState.timer += dt;
                  if (sequenceState.timer >= step) {{
                    sequenceState.timer = 0;
                    if (sequenceState.frame < phase.end) {{
                      sequenceState.frame += 1;
                      frameIdx = sequenceState.frame;
                      frameRange.value = String(frameIdx);
                      frameValue.textContent = "Frame " + frameIdx;
                      drawFrame(frameIdx, 1);
                    }} else {{
                      sequenceState.holding = true;
                      sequenceState.holdElapsed = 0;
                    }}
                  }}
                }}
              }}
            }}
          }} else if (playing && playMode === "phases" && sheet && phases.length) {{
            const phase = phases[phaseState?.index ?? 0];
            if (!phase) {{
              phaseState = null;
            }} else {{
              if (!phaseState) {{
                phaseState = {{
                  index: 0,
                  frame: phase.start,
                  timer: 0,
                  holding: false,
                  holdElapsed: 0,
                }};
                frameIdx = phase.start;
                frameRange.value = String(frameIdx);
                frameValue.textContent = "Frame " + frameIdx;
                drawFrame(frameIdx, 1);
              }} else if (phaseState.holding) {{
                const pauseMs = phase.pause_ms || 0;
                const fadeMs = phase.fade_ms || 0;
                const totalHold = pauseMs + fadeMs;
                phaseState.holdElapsed += dt * 1000;
                let alpha = 1;
                if (fadeMs > 0 && phaseState.holdElapsed > pauseMs) {{
                  const fadeProgress = Math.min(1, (phaseState.holdElapsed - pauseMs) / fadeMs);
                  alpha = Math.max(0, 1 - fadeProgress);
                }}
                drawFrame(phaseState.frame, alpha);
                if (phaseState.holdElapsed >= totalHold) {{
                  const nextIndex = (phaseState.index + 1) % phases.length;
                  const nextPhase = phases[nextIndex];
                  phaseState = {{
                    index: nextIndex,
                    frame: nextPhase.start,
                    timer: 0,
                    holding: false,
                    holdElapsed: 0,
                  }};
                  frameIdx = nextPhase.start;
                  frameRange.value = String(frameIdx);
                  frameValue.textContent = "Frame " + frameIdx;
                  drawFrame(frameIdx, 1);
                }}
              }} else {{
                const step = fps > 0 ? 1 / fps : 0.2;
                phaseState.timer += dt;
                if (phaseState.timer >= step) {{
                  phaseState.timer = 0;
                  if (phaseState.frame < phase.end) {{
                    phaseState.frame += 1;
                    frameIdx = phaseState.frame;
                    frameRange.value = String(frameIdx);
                    frameValue.textContent = "Frame " + frameIdx;
                    drawFrame(frameIdx, 1);
                  }} else {{
                    phaseState.holding = true;
                    phaseState.holdElapsed = 0;
                  }}
                }}
              }}
            }}
          }} else if (playing && playMode === "strip" && sheet) {{
            const step = fps > 0 ? 1 / fps : 0.2;
            if (!window.__spriteFrameTimer) window.__spriteFrameTimer = 0;
            window.__spriteFrameTimer += dt;
            if (window.__spriteFrameTimer >= step) {{
              window.__spriteFrameTimer = 0;
              frameIdx = (frameIdx + 1) % Math.max(1, sheet.frames || 1);
              frameRange.value = String(frameIdx);
              frameValue.textContent = "Frame " + frameIdx;
              drawFrame(frameIdx);
            }}
          }}
          requestAnimationFrame(tick);
        }}

        function hookEvents() {{
          logDebug("Binding events.");
          fpsRange.addEventListener("input", () => {{
            fps = Number(fpsRange.value);
            fpsValue.textContent = fps + " fps";
            saveState();
          }});
          confidenceRange.addEventListener("input", () => {{
            updateConfidenceLabel();
            saveState();
          }});
          if (mobileToggle) {{
            mobileToggle.addEventListener("change", () => {{
              applyMobileLayout();
              saveState();
            }});
          }}
          playModeSelect.addEventListener("change", () => {{
            playMode = playModeSelect.value;
            phaseState = null;
            sequenceState = null;
            updateModeHint();
            logDebug("Mode set: " + playMode);
            saveState();
          }});
          if (reloadBtn) {{
            reloadBtn.addEventListener("click", () => {{
              reloadManifest();
            }});
          }}
          if (autoRefreshToggle) {{
            autoRefreshToggle.addEventListener("change", () => {{
              if (autoRefreshToggle.checked) {{
                if (!window.__spriteAutoRefresh) {{
                  window.__spriteAutoRefresh = setInterval(() => {{
                    reloadManifest();
                  }}, 8000);
                }}
              }} else if (window.__spriteAutoRefresh) {{
                clearInterval(window.__spriteAutoRefresh);
                window.__spriteAutoRefresh = null;
              }}
            }});
          }}
          frameRange.addEventListener("input", () => {{
            frameIdx = Number(frameRange.value);
            frameValue.textContent = "Frame " + frameIdx;
            drawFrame(frameIdx);
            saveState();
          }});
          if (canvas) {{
          canvas.addEventListener("pointerdown", (event) => {{
            if (!sheet) return;
            event.preventDefault();
            canvas.setPointerCapture(event.pointerId);
            dragPosition = {{
              startX: event.clientX,
              startY: event.clientY,
              originX: spriteOffset.x,
              originY: spriteOffset.y,
                mode: event.shiftKey ? "position" : "direction",
            }};
          }});
            canvas.addEventListener("pointermove", (event) => {{
              if (!dragPosition) return;
              event.preventDefault();
              if (dragPosition.mode === "direction") {{
                const rect = canvas.getBoundingClientRect();
                const cx = rect.left + rect.width / 2;
                const cy = rect.top + rect.height / 2;
                const dx = event.clientX - cx;
                const dy = event.clientY - cy;
                const mag = Math.hypot(dx, dy);
                if (mag > 6) {{
                  const angle = Math.atan2(-dy, dx) * (180 / Math.PI);
                  const targetDir = directionFromAngle(angle);
                  if (targetDir !== currentDirection) {{
                    setDirection(targetDir);
                  }}
                }}
              }} else {{
                const dx = event.clientX - dragPosition.startX;
                const dy = event.clientY - dragPosition.startY;
                spriteOffset = {{
                  x: dragPosition.originX + dx,
                  y: dragPosition.originY + dy,
                }};
                drawFrame(frameIdx);
              }}
            }});
            const endDrag = (event) => {{
              if (!dragPosition) return;
              const mode = dragPosition.mode;
              dragPosition = null;
              try {{
                canvas.releasePointerCapture(event.pointerId);
              }} catch (e) {{
                // Ignore release errors for non-captured pointers.
              }}
              if (mode === "position") {{
                persistActionState();
                saveState();
              }}
            }};
            canvas.addEventListener("pointerup", endDrag);
            canvas.addEventListener("pointercancel", endDrag);
          }}
          if (posResetBtn) {{
            posResetBtn.addEventListener("click", () => {{
              spriteOffset = {{ x: 0, y: 0 }};
              persistActionState();
              drawFrame(frameIdx);
              saveState();
            }});
          }}
          if (dirResetBtn) {{
            dirResetBtn.addEventListener("click", () => {{
              const meta = getActionMeta(sheet ? sheet.name : "");
              if (!meta || !directionIndex) return;
              const entry = directionIndex[meta.key];
              const targetName = entry && entry.defaultName ? entry.defaultName : (sheet ? sheet.name : null);
              if (!targetName) return;
              currentDirection = "Default";
              flipX = false;
              persistActionState();
              actionSelect.value = targetName;
              setSheetByName(targetName);
              drawFrame(frameIdx);
              updateDirectionDisplay();
              saveState();
            }});
          }}
          playToggle.addEventListener("click", () => {{
            playing = !playing;
            playToggle.textContent = playing ? "Pause" : "Play";
            if (playing) {{
              if (playMode === "phases") {{
                phaseState = null;
              }}
              if (playMode === "sequence") {{
                sequenceState = {{
                  actionIndex: 0,
                  phaseIndex: 0,
                  frame: sequence[0] && sequence[0].phases && sequence[0].phases.length ? sequence[0].phases[0].start : 0,
                  timer: 0,
                  holding: false,
                  holdElapsed: 0,
                  started: false,
                }};
              }}
            }}
            if (!playing) {{
              drawFrame(frameIdx);
            }}
          }});
          seqAddBtn.addEventListener("click", () => {{
            if (!sheet) {{
              saveStatus.textContent = "Select an action first.";
              setTimeout(() => (saveStatus.textContent = ""), 1500);
              return;
            }}
            const actionName = sheet.name;
            const phasesCopy = phases.slice().map((p) => ({{
              name: p.name,
              start: p.start,
              end: p.end,
              pause_ms: p.pause_ms || 0,
              fade_ms: p.fade_ms || 0,
            }}));
            sequence.push({{
              action: actionName,
              phases: phasesCopy,
            }});
            renderSequence();
            logDebug("Added to sequence: " + actionName);
          }});
          seqClearBtn.addEventListener("click", () => {{
            sequence = [];
            sequenceState = null;
            renderSequence();
            logDebug("Sequence cleared.");
          }});
          seqPlayBtn.addEventListener("click", () => {{
            if (!sequence.length) {{
              saveStatus.textContent = "Sequence is empty.";
              setTimeout(() => (saveStatus.textContent = ""), 1500);
              return;
            }}
            playMode = "sequence";
            playModeSelect.value = "sequence";
            updateModeHint();
            playing = true;
            playToggle.textContent = "Pause";
            sequenceState = {{
              actionIndex: 0,
              phaseIndex: 0,
              frame: sequence[0].phases && sequence[0].phases.length ? sequence[0].phases[0].start : 0,
              timer: 0,
              holding: false,
              holdElapsed: 0,
              started: false,
            }};
            logDebug("Sequence playback started.");
          }});
          charSelect.addEventListener("change", () => {{
            populateActions();
            setSheetByName(actionSelect.value);
            drawFrame(0);
            saveState();
          }});
          actionFilter.addEventListener("input", () => {{
            populateActions();
            setSheetByName(actionSelect.value);
            drawFrame(0);
            saveState();
          }});
          actionSelect.addEventListener("change", () => {{
            setSheetByName(actionSelect.value);
            drawFrame(0);
            saveState();
          }});
          nameInput.addEventListener("input", saveState);
          tagsInput.addEventListener("input", saveState);
          descInput.addEventListener("input", saveState);
          if (applyAllFrames) applyAllFrames.addEventListener("change", saveState);
          if (phaseNameInput) phaseNameInput.addEventListener("input", saveState);
          if (phaseStartInput) phaseStartInput.addEventListener("input", saveState);
          if (phaseEndInput) phaseEndInput.addEventListener("input", saveState);
          if (phasePauseInput) phasePauseInput.addEventListener("input", saveState);
          if (phaseFadeInput) phaseFadeInput.addEventListener("input", saveState);
          cloneBtn.addEventListener("click", () => {{
            const sourceKey = cloneSelect.value;
            if (!sourceKey) {{
              saveStatus.textContent = "Select a source action.";
              setTimeout(() => (saveStatus.textContent = ""), 1500);
              return;
            }}
            const labels = loadLabels();
            const source = labels[sourceKey];
            if (!source) {{
              saveStatus.textContent = "No saved label for that action.";
              setTimeout(() => (saveStatus.textContent = ""), 1500);
              return;
            }}
            nameInput.value = source.name || "";
            tagsInput.value = source.tags || "";
            descInput.value = source.description || "";
            confidenceRange.value = String(source.confidence ?? 0);
            updateConfidenceLabel();
            phases = Array.isArray(source.phases) ? source.phases.slice() : [];
            renderPhases();
            saveStatus.textContent = "Cloned (not saved yet).";
            setTimeout(() => (saveStatus.textContent = ""), 1500);
            saveState();
          }});
          phaseAddBtn.addEventListener("click", () => {{
            try {{
              logDebug("Add phase click.");
              const name = (phaseNameInput && phaseNameInput.value || "").trim();
              if (!name) {{
                saveStatus.textContent = "Phase needs a name.";
                setTimeout(() => (saveStatus.textContent = ""), 1500);
                logDebug("Phase add blocked: missing name.");
                return;
              }}
              const start = Math.max(0, Number(phaseStartInput && phaseStartInput.value || 0));
              const maxFrame = sheet && sheet.frames ? sheet.frames - 1 : start;
              const end = Math.min(Math.max(start, Number(phaseEndInput && phaseEndInput.value || start)), maxFrame);
              const pauseValue = phasePauseInput ? phasePauseInput.value : 0;
              const fadeValue = phaseFadeInput ? phaseFadeInput.value : 0;
              const pauseMs = Math.max(0, Number(pauseValue || 0)) * 1000;
              const fadeMs = Math.max(0, Number(fadeValue || 0)) * 1000;
              phases.push({{ name, start, end, pause_ms: pauseMs, fade_ms: fadeMs }});
              logDebug("Phases length after add: " + phases.length);
              renderPhases();
              if (phaseNameInput) {{
                phaseNameInput.value = "";
              }}
              saveStatus.textContent = "Phase added.";
              setTimeout(() => (saveStatus.textContent = ""), 1500);
              logDebug("Phase added: " + name + " (" + start + "-" + end + ")");
              saveState();
            }} catch (err) {{
              console.error(err);
              saveStatus.textContent = "Phase add failed.";
              setTimeout(() => (saveStatus.textContent = ""), 1500);
              logDebug("Phase add error: " + err);
            }}
          }});
          saveBtn.addEventListener("click", () => {{
            const key = getLabelKey();
            if (!key) {{
              logDebug("Save blocked: no sheet selected.");
              return;
            }}
            const labels = loadLabels();
            const entry = {{
              name: nameInput.value.trim(),
              tags: tagsInput.value.trim(),
              description: descInput.value.trim(),
              frames: sheet ? sheet.frames : null,
              apply_all_frames: applyAllFrames.checked,
              confidence: Number(confidenceRange.value),
              phases: phases.slice(),
              updated_at: new Date().toISOString(),
            }};
            labels[key] = entry;
            if (applyAllFrames.checked && sheet && sheet.frames) {{
              for (let i = 0; i < sheet.frames; i += 1) {{
                labels[key + "/" + i] = entry;
              }}
            }}
            saveLabels(labels);
            saveStatus.textContent = "Saved.";
            setTimeout(() => (saveStatus.textContent = ""), 1500);
            refreshLabelLog();
            renderSavedPhases();
            logDebug("Saved label for " + key + " (phases: " + phases.length + ")");
            saveState();
          }});
          if (copyPhasesBtn) {{
            copyPhasesBtn.addEventListener("click", async () => {{
              if (!sheet) {{
                saveStatus.textContent = "Select an action first.";
                setTimeout(() => (saveStatus.textContent = ""), 1500);
                return;
              }}
              const payload = {{
                action: sheet.name,
                phases: phases.slice(),
                confidence: Number(confidenceRange.value),
                updated_at: new Date().toISOString(),
              }};
              const text = JSON.stringify(payload, null, 2);
              try {{
                await navigator.clipboard.writeText(text);
                saveStatus.textContent = "Copied phases JSON.";
                setTimeout(() => (saveStatus.textContent = ""), 1500);
              }} catch (e) {{
                saveStatus.textContent = "Copy failed.";
                setTimeout(() => (saveStatus.textContent = ""), 1500);
              }}
            }});
          }}
          downloadBtn.addEventListener("click", () => {{
            const labels = loadLabels();
            const payload = JSON.stringify(labels, null, 2);
            const blob = new Blob([payload], {{ type: "application/json" }});
            const url = URL.createObjectURL(blob);
            const link = document.createElement("a");
            link.href = url;
            link.download = "sprite_labels.json";
            document.body.appendChild(link);
            link.click();
            document.body.removeChild(link);
            URL.revokeObjectURL(url);
            saveStatus.textContent = "Downloaded.";
            setTimeout(() => (saveStatus.textContent = ""), 1500);
          }});
          copyBtn.addEventListener("click", async () => {{
            const labels = loadLabels();
            const payload = JSON.stringify(labels, null, 2);
            try {{
              await navigator.clipboard.writeText(payload);
              saveStatus.textContent = "Copied JSON.";
              setTimeout(() => (saveStatus.textContent = ""), 1500);
            }} catch (e) {{
              saveStatus.textContent = "Copy failed.";
              setTimeout(() => (saveStatus.textContent = ""), 1500);
            }}
          }});
          if (phaseListEl) {{
            phaseListEl.addEventListener("dragstart", (event) => {{
              const row = event.target.closest(".phase-row");
              if (!row) return;
              dragPhaseIndex = Number(row.dataset.index);
              row.classList.add("dragging");
              event.dataTransfer.effectAllowed = "move";
            }});
            phaseListEl.addEventListener("dragover", (event) => {{
              event.preventDefault();
              const row = event.target.closest(".phase-row");
              if (!row) return;
              clearDropTargets(phaseListEl);
              row.classList.add("drop-target");
            }});
            phaseListEl.addEventListener("dragleave", (event) => {{
              const row = event.target.closest(".phase-row");
              if (!row) return;
              row.classList.remove("drop-target");
            }});
            phaseListEl.addEventListener("drop", (event) => {{
              event.preventDefault();
              const row = event.target.closest(".phase-row");
              const targetIndex = row ? Number(row.dataset.index) : phases.length - 1;
              moveItem(phases, dragPhaseIndex, targetIndex);
              dragPhaseIndex = null;
              clearDropTargets(phaseListEl);
              renderPhases();
            }});
            phaseListEl.addEventListener("dragend", (event) => {{
              const row = event.target.closest(".phase-row");
              if (row) row.classList.remove("dragging");
              clearDropTargets(phaseListEl);
              dragPhaseIndex = null;
            }});
          }}
          if (seqLogEl) {{
            seqLogEl.addEventListener("dragstart", (event) => {{
              const row = event.target.closest(".phase-row");
              if (!row) return;
              dragSeqIndex = Number(row.dataset.index);
              row.classList.add("dragging");
              event.dataTransfer.effectAllowed = "move";
            }});
            seqLogEl.addEventListener("dragover", (event) => {{
              event.preventDefault();
              const row = event.target.closest(".phase-row");
              if (!row) return;
              clearDropTargets(seqLogEl);
              row.classList.add("drop-target");
            }});
            seqLogEl.addEventListener("dragleave", (event) => {{
              const row = event.target.closest(".phase-row");
              if (!row) return;
              row.classList.remove("drop-target");
            }});
            seqLogEl.addEventListener("drop", (event) => {{
              event.preventDefault();
              const row = event.target.closest(".phase-row");
              const targetIndex = row ? Number(row.dataset.index) : sequence.length - 1;
              moveItem(sequence, dragSeqIndex, targetIndex);
              dragSeqIndex = null;
              clearDropTargets(seqLogEl);
              renderSequence();
            }});
            seqLogEl.addEventListener("dragend", (event) => {{
              const row = event.target.closest(".phase-row");
              if (row) row.classList.remove("dragging");
              clearDropTargets(seqLogEl);
              dragSeqIndex = null;
            }});
          }}
        }}

        if (!window.SpriteSheetPlayer || !window.SpriteSheetPlayer.loadSpriteManifest) {{
          setStatus("Sprite helpers not loaded.");
          return;
        }}

        const manifestPromise = window.__spriteManifest && window.__spriteManifest.sheets
          ? Promise.resolve(window.__spriteManifest)
          : window.SpriteSheetPlayer.loadSpriteManifest();

        function loadInlineImages(map) {{
          return new Promise((resolve) => {{
            const imagesBySheet = {{}};
            const errors = [];
            const entries = Object.entries(map || {{}});
            if (!entries.length) {{
              resolve({{ imagesBySheet, errors }});
              return;
            }}
            let remaining = entries.length;
            entries.forEach(([name, uri]) => {{
              const img = new Image();
              img.onload = () => {{
                imagesBySheet[name] = img;
                remaining -= 1;
                if (!remaining) resolve({{ imagesBySheet, errors }});
              }};
              img.onerror = () => {{
                errors.push({{ sheet: name, error: "Failed to load inline image" }});
                remaining -= 1;
                if (!remaining) resolve({{ imagesBySheet, errors }});
              }};
              img.src = uri;
            }});
          }});
        }}

        manifestPromise
          .then((data) => {{
            applyManifest(data);
            if (window.__spriteImages && Object.keys(window.__spriteImages).length) {{
              return loadInlineImages(window.__spriteImages);
            }}
            return window.SpriteSheetPlayer.loadSheetImages(manifest);
          }})
          .then((result) => {{
            imagesBySheet = result.imagesBySheet || {{}};
            if (result.errors && result.errors.length) {{
              console.warn("Sprite load errors", result.errors);
            }}
            hookEvents();
            const restored = restoreState();
            if (!restored) {{
              drawFrame(0);
            }}
            renderThumbnails();
            refreshLabelLog();
            renderSavedPhases();
            updateConfidenceLabel();
            updateModeHint();
            refreshTabLog();
            setInterval(refreshTabLog, 2000);
            if (mobileToggle) {{
              if (!restored) {{
                mobileToggle.checked = window.innerWidth < 900;
                applyMobileLayout();
              }}
            }}
            logDebug("Sprite Lab ready.");
            setStatus("Sprite Lab ready.");
            requestAnimationFrame(tick);
          }})
          .catch((err) => {{
            console.error(err);
            setStatus("Failed to load sprite manifest.");
            logDebug("Init failed: " + err);
          }});
      }})();
    </script>
    """
    components.html(sprite_lab_html, height=1600)
