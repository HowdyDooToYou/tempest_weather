# Configuration Reference

This document provides a complete reference for all environment variables and configuration options in Tempest Weather.

## Quick Start

1. Copy `.env.example` to `.env`
2. Set required variables for your use case
3. Run `.\scripts\run_streamlit.ps1` (loads `.env` automatically)

For Windows services, configure environment variables via NSSM or System Environment instead of `.env`.

---

## Required Variables

### WeatherFlow API

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `TEMPEST_API_TOKEN` | **Yes** | - | WeatherFlow API token for WebSocket and REST APIs |
| `TEMPEST_STATION_ID` | No | `475329` | Your Tempest station ID (find in WeatherFlow app) |

**How to get your API token:**
1. Log in to [tempestwx.com](https://tempestwx.com)
2. Go to Settings → Data Authorizations
3. Create a new token with "Personal Use" scope

### AirLink (Optional)

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `DAVIS_AIRLINK_HOST` | For AirLink | - | IP address or hostname of your AirLink device (e.g., `192.168.1.19`) |
| `AIRLINK_POLL_SEC` | No | `15` | Polling interval in seconds |
| `AIRLINK_HTTP_TIMEOUT` | No | `8` | HTTP request timeout in seconds |
| `AIRLINK_RETRY_SEC` | No | `5` | Retry delay after failed requests |

---

## Database Configuration

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `TEMPEST_DB_PATH` | No | `data/tempest.db` | Path to SQLite database file |

The database is created automatically on first run. Uses WAL mode for concurrent access.

---

## Timezone & Locale

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `LOCAL_TZ` | No | `America/New_York` | IANA timezone name for display and scheduling |

**Examples:** `America/Los_Angeles`, `Europe/London`, `Asia/Tokyo`

---

## Dashboard Settings

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `CONTROL_REFRESH_SECONDS` | No | `120` | Sidebar auto-refresh interval |
| `AUTO_REFRESH_SECONDS` | No | `120` | Fallback for `CONTROL_REFRESH_SECONDS` |
| `FORECAST_REFRESH_MINUTES` | No | `30` | How often to refresh Tempest forecast |
| `FORECAST_UNITS` | No | `imperial` | Units for forecast (`imperial` or `metric`) |

---

## Alerting Configuration

### SMTP Email

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `SMTP_USERNAME` | For email | - | SMTP username (usually your email address) |
| `SMTP_PASSWORD` | For email | - | SMTP password or app password |
| `SMTP_HOST` | No | `smtp.gmail.com` | SMTP server hostname |
| `SMTP_PORT` | No | `587` | SMTP server port |
| `SMTP_USE_TLS` | No | `true` | Enable STARTTLS |
| `SMTP_USE_SSL` | No | `false` | Enable SSL/TLS (mutually exclusive with TLS) |
| `ALERT_EMAIL_FROM` | For email | - | Sender email address |
| `ALERT_EMAIL_TO` | No | - | Default recipient email address |

**Gmail Setup:**
1. Enable 2-factor authentication on your Google account
2. Generate an App Password at [myaccount.google.com/apppasswords](https://myaccount.google.com/apppasswords)
3. Use the app password as `SMTP_PASSWORD`

### Windows Credential Manager (Alternative)

Instead of storing SMTP credentials in environment variables, you can use Windows Credential Manager:

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `SMTP_CRED_TARGET` | No | `TempestWeatherSMTP` | Credential Manager target name |

**Setup:**
```powershell
.\scripts\set_smtp_credential.ps1 -Username "you@gmail.com"
# Enter your app password when prompted
```

When `SMTP_CRED_TARGET` is set and credentials exist, `SMTP_USERNAME` and `SMTP_PASSWORD` are loaded automatically.

### Verizon SMS

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `VERIZON_SMS_TO` | For SMS | - | Verizon phone number (10 digits, no dashes) |

**Note:** SMS delivery uses Verizon's email-to-SMS gateway (`{number}@vtext.com`). This is carrier-specific and only works for Verizon numbers.

### Freeze Alert Thresholds

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `FREEZE_WARNING_F` | No | `32` | Temperature (°F) to trigger freeze warning |
| `DEEP_FREEZE_F` | No | `18` | Temperature (°F) to trigger deep freeze alert |
| `FREEZE_RESET_F` | No | `34` | Temperature (°F) to reset alert state |

### Alert Worker

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `ALERTS_WORKER_ENABLED` | No | `0` | Set to `1` to disable UI alerts (when worker is active) |
| `ALERT_WORKER_INTERVAL_SECONDS` | No | `60` | Worker polling interval |

---

## NWS Integration

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `NWS_USER_AGENT` | **Yes** for NWS | - | User-Agent string (required by api.weather.gov) |
| `NWS_ALERTS_ENABLED` | No | `1` | Enable NWS active alerts |
| `NWS_HWO_NOTIFY` | No | `0` | Enable Hazardous Weather Outlook notifications |
| `NWS_ZONE` | No | Auto-detected | Override NWS zone (e.g., `GAZ041`) |

**User-Agent Format:**
```
TempestWeather/1.0 (contact: you@example.com)
```

The NWS API requires a valid User-Agent with contact information.

---

## Daily Brief (OpenAI)

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `OPENAI_API_KEY` | For briefs | - | OpenAI API key |
| `DAILY_BRIEF_MODEL` | No | `gpt-4o-mini` | OpenAI model to use |
| `DAILY_BRIEF_INTERVAL_MINUTES` | No | `180` | Generation interval (3 hours) |
| `DAILY_BRIEF_LAT` | No | Auto-detected | Latitude for historical context |
| `DAILY_BRIEF_LON` | No | Auto-detected | Longitude for historical context |

**Location Resolution Order:**
1. UI location override (if enabled)
2. `DAILY_BRIEF_LAT` / `DAILY_BRIEF_LON` environment variables
3. Database location override
4. Tempest station location (via API)

---

## Daily Email

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `DAILY_EMAIL_TO` | For email | - | Recipient email address |
| `DAILY_EMAIL_HOUR` | No | `7` | Hour to send (24-hour format, local time) |
| `DAILY_EMAIL_MINUTE` | No | `0` | Minute to send |
| `DAILY_EMAIL_LAT` | No | Auto-detected | Latitude for forecast |
| `DAILY_EMAIL_LON` | No | Auto-detected | Longitude for forecast |

---

## AQI Smoke Event Suppression

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `AQI_SMOKE_CLEAR_HOURS` | No | `6` | Hours of good AQI before auto-clearing smoke event |
| `AQI_SMOKE_CLEAR_MAX` | No | `50` | Maximum AQI to consider "good" |
| `AQI_SMOKE_CLEAR_MIN_COUNT` | No | `6` | Minimum readings required to auto-clear |

When a smoke event is active:
- AQI mentions are suppressed in daily briefs
- Both observed and smoke-adjusted averages are shown in UI
- Event persists until manually cleared or auto-clears when air improves

---

## Collector Watchdog

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `WATCHDOG_STALE_SECONDS` | No | `600` | General stale threshold |
| `WATCHDOG_TEMPEST_HEARTBEAT_SEC` | No | `300` | Tempest collector heartbeat threshold |
| `WATCHDOG_AIRLINK_HEARTBEAT_SEC` | No | `180` | AirLink collector heartbeat threshold |
| `WATCHDOG_TEMPEST_DATA_SEC` | No | `900` | Tempest data staleness threshold |
| `WATCHDOG_AIRLINK_DATA_SEC` | No | `300` | AirLink data staleness threshold |

---

## Complete `.env.example`

```bash
# =============================================================================
# TEMPEST WEATHER CONFIGURATION
# =============================================================================

# -----------------------------------------------------------------------------
# WeatherFlow API (Required)
# -----------------------------------------------------------------------------
TEMPEST_API_TOKEN=
TEMPEST_API_KEY=
TEMPEST_STATION_ID=475329

# -----------------------------------------------------------------------------
# AirLink (Optional - for air quality monitoring)
# -----------------------------------------------------------------------------
# DAVIS_AIRLINK_HOST=192.168.1.19
# AIRLINK_POLL_SEC=15

# -----------------------------------------------------------------------------
# Database & Timezone
# -----------------------------------------------------------------------------
TEMPEST_DB_PATH=data/tempest.db
LOCAL_TZ=America/New_York

# -----------------------------------------------------------------------------
# SMTP Email Configuration
# -----------------------------------------------------------------------------
SMTP_USERNAME=
SMTP_PASSWORD=
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USE_TLS=true
SMTP_USE_SSL=false
ALERT_EMAIL_FROM=
ALERT_EMAIL_TO=

# Alternative: Windows Credential Manager
# SMTP_CRED_TARGET=TempestWeatherSMTP

# -----------------------------------------------------------------------------
# SMS Alerts (Verizon only)
# -----------------------------------------------------------------------------
VERIZON_SMS_TO=

# -----------------------------------------------------------------------------
# Freeze Alert Thresholds
# -----------------------------------------------------------------------------
FREEZE_WARNING_F=32
DEEP_FREEZE_F=18
FREEZE_RESET_F=34

# -----------------------------------------------------------------------------
# Alert Worker
# -----------------------------------------------------------------------------
ALERT_WORKER_INTERVAL_SECONDS=60
ALERTS_WORKER_ENABLED=0

# -----------------------------------------------------------------------------
# NWS Integration
# -----------------------------------------------------------------------------
NWS_USER_AGENT=TempestWeather/1.0 (contact: you@example.com)
NWS_ALERTS_ENABLED=1
NWS_HWO_NOTIFY=0
NWS_ZONE=

# -----------------------------------------------------------------------------
# Daily Brief (OpenAI)
# -----------------------------------------------------------------------------
OPENAI_API_KEY=
DAILY_BRIEF_MODEL=gpt-4o-mini
DAILY_BRIEF_INTERVAL_MINUTES=180
DAILY_BRIEF_LAT=
DAILY_BRIEF_LON=

# -----------------------------------------------------------------------------
# Daily Email
# -----------------------------------------------------------------------------
DAILY_EMAIL_TO=
DAILY_EMAIL_HOUR=7
DAILY_EMAIL_MINUTE=0
DAILY_EMAIL_LAT=
DAILY_EMAIL_LON=

# -----------------------------------------------------------------------------
# AQI Smoke Event
# -----------------------------------------------------------------------------
AQI_SMOKE_CLEAR_HOURS=6
AQI_SMOKE_CLEAR_MAX=50
AQI_SMOKE_CLEAR_MIN_COUNT=6

# -----------------------------------------------------------------------------
# Dashboard Settings
# -----------------------------------------------------------------------------
# CONTROL_REFRESH_SECONDS=120
# FORECAST_REFRESH_MINUTES=30
# FORECAST_UNITS=imperial
```

---

## Service Configuration

When running as Windows services via NSSM, environment variables should be configured in one of these ways:

### Option 1: System Environment Variables

Set variables in Windows System Properties → Environment Variables. These are inherited by all services.

### Option 2: NSSM AppEnvironmentExtra

The install script (`scripts/install_services.ps1`) automatically copies environment variables from the current shell to each service's `AppEnvironmentExtra`.

### Option 3: Per-Service Configuration

Use NSSM GUI to configure individual services:
```powershell
nssm edit TempestWeatherAlerts
# Navigate to Environment tab
```

### Checking Service Environment

```powershell
.\scripts\services.ps1 env
# Shows which variables are configured for each service
```

---

## Configuration Precedence

For most settings, the precedence order is:

1. **Environment variable** (highest priority)
2. **Database setting** (via UI Controls)
3. **Default value** (lowest priority)

For location settings specifically:
1. UI location override (if `override_location_enabled` is true)
2. Environment variable (`DAILY_BRIEF_LAT`/`DAILY_BRIEF_LON`)
3. Database location override
4. Tempest station location (via API)
