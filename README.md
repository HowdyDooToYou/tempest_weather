# Tempest Weather & Air Quality Dashboard

Streamlit dashboard for monitoring Tempest weather and AirLink air-quality data with flexible chart ordering and metric selection.

## Features
- Overview with selectable metrics (accordion) — temperature preselected by default.
- Trends tab with user-driven chart order (drag/deselect via multiselect).
- Raw data views for Tempest, AirLink, and hub events.
- Sidebar controls for window size, date range, theme, and location overrides.
- [paused] Legacy sprite/"Sprite Lab" assets removed; focused on weather/air data only.

## Supported devices
- Tempest Station (weather observations)
- Tempest Hub (connectivity/heartbeat)
- AirLink (PM/AQI; treated as outdoor)

## Requirements
- Python 3.10+ (tested on 3.12)
- Dependencies: `streamlit`, `pandas`, `altair`, `requests` (install with `pip install streamlit pandas altair requests`)
- Tempest/AirLink data available in `data/tempest.db` (and related tables)

## Quick start
1) Create a virtual environment (recommended):
   ```bash
   python -m venv .venv
   .\.venv\Scripts\activate
   ```
2) Install deps:
   ```bash
   pip install streamlit pandas altair requests
   ```
3) Run the app:
   ```bash
   streamlit run dashboard.py
   ```

## Configuration
- API token (optional, for auto-location): set `TEMPEST_API_TOKEN` in your environment.
- Device IDs and hub targets live near the top of `dashboard.py` (`TEMPEST_STATION_ID`, `TEMPEST_HUB_ID`, `PING_TARGETS`).
- Data path: `data/tempest.db` (adjust in `DB_PATH` if needed).

## Usage notes
- In **Overview**, toggle metrics via the accordion; charts for selected metrics render below.
- In **Trends**, reorder/hide charts with the “Choose chart order” multiselect; order is remembered per session.
- Raw tabs provide table views for quick inspection.

## Development
- Run `python -m py_compile dashboard.py` for a quick syntax check.
- No external build needed; app is Streamlit-only.
