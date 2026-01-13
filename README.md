# Tempest Weather and Air Quality Dashboard

Streamlit dashboard for Tempest weather and AirLink air-quality data with live gauges, palettes, and background alerts.

## Features
- Overview, Trends, Comparisons, and Raw tabs for weather and AQI.
- Live gauges with highlights and local time.
- Theme palettes plus a Custom token picker.
- Connection and ingest health panel with collector status.
- Freeze alerts via email/SMS (UI or background worker).

## Supported devices
- Tempest Station (weather observations)
- Tempest Hub (connectivity/heartbeat)
- AirLink (PM/AQI; treated as outdoor)

## Requirements
- Python 3.10+ (tested on 3.12)
- Dependencies: `streamlit`, `pandas`, `altair`, `requests`
- Tempest/AirLink data available in `data/tempest.db` (and related tables)

## Quick start
1) Create a virtual environment:
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
4) Optional: create `.env` from `.env.example` and run the convenience script:
   ```powershell
   copy .env.example .env
   .\scripts\run_streamlit.ps1
   ```
   The script loads `.env` into the current shell and does not overwrite existing env vars unless `-OverrideEnv` is used.
   For services, skip `.env` and set environment variables via NSSM or System Environment.

## Configuration
- `TEMPEST_API_TOKEN`: optional, for auto-location.
- `TEMPEST_DB_PATH`: optional, defaults to `data/tempest.db`.
- `LOCAL_TZ`: optional, defaults to `America/New_York`.

### Alerts
- SMTP credentials (environment only):
  - `SMTP_USERNAME`, `SMTP_PASSWORD`, `ALERT_EMAIL_FROM`
  - Optional: `SMTP_HOST`, `SMTP_PORT`, `SMTP_USE_TLS`, `SMTP_USE_SSL`
- Recipients:
  - Email: `ALERT_EMAIL_TO` (env) or save via Controls -> Alerts.
  - SMS: `VERIZON_SMS_TO` (env) or save via Controls -> Alerts.
- Thresholds:
  - `FREEZE_WARNING_F` (default 32)
  - `DEEP_FREEZE_F` (default 18)
  - `FREEZE_RESET_F` (default 34)
- Worker settings:
  - `ALERTS_WORKER_ENABLED=1` to stop UI sends when the worker is active.
  - `ALERT_WORKER_INTERVAL_SECONDS` (default 60)
  - Use the worker for continuous alerts when no UI session is open.

## Alerts and privacy
- SMTP credentials are never stored in the database or code.
- Recipient overrides saved in the UI are stored in `data/tempest.db` (gitignored).
- Saved recipients override env values; clear them in Controls -> Alerts to revert.

## Run as a Windows service (NSSM)
1) Install NSSM (for example: `choco install nssm`).
2) Configure environment variables for services (Windows System Environment or NSSM Environment tab).
   The installer reads from the current shell and writes them into `AppEnvironmentExtra`.
3) Run:
   ```powershell
   .\scripts\install_services.ps1
   ```
4) Remove services:
   ```powershell
   .\scripts\uninstall_services.ps1
   ```

Services installed by the script:
- `TempestWeatherUI` (Streamlit dashboard on port 8501)
- `TempestWeatherAlerts` (background alerts worker)
The install script sets `ALERTS_WORKER_ENABLED=1` for the UI service to avoid duplicate alerts.

Service env checklist (NSSM/System):
- Required: `SMTP_USERNAME`, `SMTP_PASSWORD`, `ALERT_EMAIL_FROM`
- Recipients: `ALERT_EMAIL_TO` and `VERIZON_SMS_TO` (optional if saved via Controls -> Alerts)
- Transport: `SMTP_HOST`, `SMTP_PORT`, `SMTP_USE_TLS`, `SMTP_USE_SSL` (set one true)
- Alerts: `FREEZE_WARNING_F`, `DEEP_FREEZE_F`, `FREEZE_RESET_F` (optional)
- Optional: `LOCAL_TZ`, `TEMPEST_DB_PATH`, `TEMPEST_API_TOKEN`
- Alerts worker: `ALERT_WORKER_INTERVAL_SECONDS` (service only)
- UI service: `ALERTS_WORKER_ENABLED=1` (set by installer)

## Usage notes
- In Overview, toggle metrics via the accordion; charts for selected metrics render below.
- In Trends, reorder or hide charts with the chart order multiselect.
- Raw tabs provide table views for quick inspection.

## Development
- Run `python -m py_compile dashboard.py src/alerting.py src/alerts_worker.py` for a quick syntax check.
- No external build needed; app is Streamlit-only.
