# Alerting System

This document describes the alerting capabilities in Tempest Weather, including freeze alerts, NWS integration, and delivery channels.

## Overview

Tempest Weather provides multi-channel alerting for:

| Alert Type | Source | Delivery |
|------------|--------|----------|
| Freeze Warning | Local temperature | Email, SMS |
| Deep Freeze | Local temperature | Email, SMS |
| NWS Active Alerts | api.weather.gov | Email, SMS |
| Hazardous Weather Outlook | api.weather.gov | Email, SMS |

Alerts can be triggered from:
- **Dashboard UI**: Manual or automatic during page load
- **Alerts Worker**: Background service running every 60 seconds

---

## Freeze Alerts

### How It Works

The freeze alert system monitors your Tempest station's temperature and sends notifications when thresholds are crossed.
It also records when a freeze begins so the dashboard can display how long the condition has been active.

```
obs_st (temperature) ──> alerting.py ──> Email/SMS
                              │
                              └── alert_state (state machine)
```

### Thresholds

| Alert | Default | Variable | Description |
|-------|---------|----------|-------------|
| Freeze Warning | 32°F | `FREEZE_WARNING_F` | First freeze notification |
| Deep Freeze | 18°F | `DEEP_FREEZE_F` | Severe cold notification |
| Reset | 34°F | `FREEZE_RESET_F` | Temperature to reset alert state |

### State Machine

The alert system uses a state machine to prevent duplicate notifications:

```
                    ┌─────────────────────────────────────┐
                    │                                     │
                    ▼                                     │
┌─────────┐    ┌─────────────┐    ┌─────────────┐    ┌───┴───┐
│  IDLE   │───>│FREEZE_WARNED│───>│ DEEP_FREEZE │───>│ RESET │
└─────────┘    └─────────────┘    └─────────────┘    └───────┘
     │              │                   │                │
     │              │                   │                │
     └──────────────┴───────────────────┴────────────────┘
                         (temp > RESET)
```

**States:**
- `IDLE`: No active alerts, monitoring for freeze
- `FREEZE_WARNED`: Freeze warning sent, monitoring for deep freeze or reset
- `DEEP_FREEZE`: Deep freeze alert sent, monitoring for reset
- `RESET`: Temperature rose above reset threshold, return to IDLE

### Alert State Storage

Alert state is persisted in the `alert_state` table:

```sql
CREATE TABLE alert_state (
    key TEXT PRIMARY KEY,
    value TEXT
);

-- Keys used:
-- freeze_sent: "1" or "0"
-- deep_freeze_sent: "1" or "0"
-- freeze_started_at: Unix timestamp
-- deep_freeze_started_at: Unix timestamp
```

### Message Format

```
Subject: Freeze Warning - Tempest 31.5 F

Freeze Warning

Current temperature: 31.5°F
Time: Jan 19, 2024 6:30 AM EST

Protect sensitive plants and exposed pipes.
```

---

## NWS Integration

### Active Alerts

The system fetches active weather alerts from the National Weather Service API.

**API Endpoint:**
```
GET https://api.weather.gov/alerts/active?zone={ZONE_ID}
```

**Alert Types Monitored:**
- Winter Storm Warning/Watch
- Freeze Warning/Watch
- Wind Advisory
- Tornado Warning/Watch
- Severe Thunderstorm Warning
- Flash Flood Warning
- And all other NWS alert types

### Zone Resolution

Zones are resolved automatically from your station's coordinates:

1. Query `https://api.weather.gov/points/{lat},{lon}`
2. Extract `forecastZone` and `county` from response
3. Query alerts for both zones
4. Deduplicate by alert ID

**Manual Override:**
```bash
NWS_ZONE=GAZ041  # Override with specific zone
```

### Hazardous Weather Outlook (HWO)

The HWO provides a 7-day outlook for potential hazardous weather.

**API Endpoint:**
```
GET https://api.weather.gov/products/types/HWO/locations/{CWA}
GET https://api.weather.gov/products/{PRODUCT_ID}
```

**Fallback:**
If the API fails, the system falls back to:
```
GET https://forecast.weather.gov/showsigwx.php?...
```

### HWO Sections Parsed

| Section | Label | Description |
|---------|-------|-------------|
| `.DAY ONE` | Today | Today's hazards |
| `.DAYS TWO THROUGH SEVEN` | Next 7 days | Extended outlook |
| `.SPOTTER INFORMATION` | Spotter info | Storm spotter activation |

### Deduplication

NWS alerts are deduplicated to prevent repeat notifications:

```sql
-- Alert deduplication
CREATE TABLE nws_alert_log (
    alert_id TEXT PRIMARY KEY,
    sent_at INTEGER NOT NULL
);

-- HWO deduplication
CREATE TABLE nws_hwo_log (
    product_id TEXT PRIMARY KEY,
    sent_at INTEGER NOT NULL
);
```

### Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `NWS_USER_AGENT` | Required | User-Agent for API requests |
| `NWS_ALERTS_ENABLED` | `1` | Enable active alert fetching |
| `NWS_HWO_NOTIFY` | `0` | Enable HWO notifications |
| `NWS_ZONE` | Auto | Override zone ID |

**User-Agent Requirement:**

The NWS API requires a valid User-Agent header:
```
NWS_USER_AGENT=TempestWeather/1.0 (contact: you@example.com)
```

---

## Delivery Channels

### Email (SMTP)

Email delivery uses SMTP with support for Gmail and other providers.

**Configuration:**
```bash
SMTP_USERNAME=you@gmail.com
SMTP_PASSWORD=your-app-password
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USE_TLS=true
ALERT_EMAIL_FROM=you@gmail.com
ALERT_EMAIL_TO=recipient@example.com
```

**Gmail Setup:**
1. Enable 2-factor authentication
2. Generate an App Password at [myaccount.google.com/apppasswords](https://myaccount.google.com/apppasswords)
3. Use the 16-character app password as `SMTP_PASSWORD`

**Windows Credential Manager:**

For enhanced security, store credentials in Windows Credential Manager:

```powershell
.\scripts\set_smtp_credential.ps1 -Username "you@gmail.com"
# Enter app password when prompted
```

Then set:
```bash
SMTP_CRED_TARGET=TempestWeatherSMTP
```

### SMS (Verizon)

SMS delivery uses Verizon's email-to-SMS gateway.

**How It Works:**
```
Email ──> {number}@vtext.com ──> Verizon ──> SMS
```

**Configuration:**
```bash
VERIZON_SMS_TO=5551234567  # 10-digit number, no dashes
```

**Limitations:**
- Verizon numbers only
- 160 character limit per message
- May have carrier delays
- Requires working SMTP configuration

**Other Carriers:**

To support other carriers, you would need to modify `src/alerting.py`:

| Carrier | Gateway |
|---------|---------|
| AT&T | `{number}@txt.att.net` |
| T-Mobile | `{number}@tmomail.net` |
| Sprint | `{number}@messaging.sprintpcs.com` |

---

## Alerts Worker

### Purpose

The alerts worker (`src/alerts_worker.py`) runs as a background service, checking for alert conditions every 60 seconds.

### Why Use the Worker?

| Scenario | UI Alerts | Worker Alerts |
|----------|-----------|---------------|
| Dashboard open | ✅ Works | ✅ Works |
| Dashboard closed | ❌ No alerts | ✅ Works |
| Multiple users | ⚠️ Duplicates | ✅ Single source |

### Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `ALERTS_WORKER_ENABLED` | `0` | Set to `1` to disable UI alerts |
| `ALERT_WORKER_INTERVAL_SECONDS` | `60` | Check interval |

When `ALERTS_WORKER_ENABLED=1`, the UI will not send alerts, deferring to the worker.

### Running the Worker

**As a Windows Service:**
```powershell
.\scripts\install_services.ps1
# Installs TempestWeatherAlerts service
```

**Manual Execution:**
```bash
python -m src.alerts_worker
```

**Single Run (for testing):**
```bash
python -m src.alerts_worker --once
```

### Worker Flow

```
┌─────────────────────────────────────────────────────────────┐
│                     Every 60 seconds                        │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  1. Load latest temperature from obs_st                     │
│  2. Check freeze thresholds                                 │
│  3. Send freeze alerts if needed                            │
│  4. Update alert_state                                      │
│                                                             │
│  5. If NWS_ALERTS_ENABLED:                                  │
│     - Fetch active alerts                                   │
│     - Filter out already-sent alerts                        │
│     - Send new alerts                                       │
│     - Record in nws_alert_log                               │
│                                                             │
│  6. If NWS_HWO_NOTIFY:                                      │
│     - Fetch HWO                                             │
│     - Check if already sent                                 │
│     - Send if new                                           │
│     - Record in nws_hwo_log                                 │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

---

## Daily Email

### Purpose

The daily email worker sends a morning summary at a configured time (default 7:00 AM).

### Content

The daily email includes:

1. **Current Conditions**
   - Temperature, wind, pressure, humidity
   - PM2.5 (if AirLink available)

2. **Daily Brief (AI)**
   - Headline
   - Bullet points
   - Tomorrow outlook

3. **48-Hour Forecast**
   - Temperature range
   - Wind and precipitation
   - Hourly snapshots

4. **NWS Outlooks & Alerts**
   - Active alerts summary
   - HWO summary

### Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `DAILY_EMAIL_TO` | Required | Recipient email |
| `DAILY_EMAIL_HOUR` | `7` | Hour to send (24h format) |
| `DAILY_EMAIL_MINUTE` | `0` | Minute to send |
| `DAILY_EMAIL_LAT` | Auto | Latitude for forecast |
| `DAILY_EMAIL_LON` | Auto | Longitude for forecast |

Note: Set `DAILY_EMAIL_TO` via an environment variable, not in code. If it is empty, the daily email falls back to `ALERT_EMAIL_TO`. Leave both unset to disable daily email delivery.

### Deduplication

The worker tracks sent emails to prevent duplicates:

```sql
CREATE TABLE daily_email_log (
    date TEXT PRIMARY KEY,
    sent_at TEXT,
    status TEXT,
    error TEXT
);
```

### Running the Worker

**As a Windows Service:**
```powershell
.\scripts\install_services.ps1
# Installs TempestWeatherDailyEmail service
```

**Manual Execution:**
```bash
python -m src.daily_email_worker
```

---

## UI Alert Controls

### Accessing Controls

1. Open the dashboard
2. Expand the sidebar
3. Click "Controls" → "Alerts"

### Available Settings

| Setting | Description |
|---------|-------------|
| Email recipient | Override `ALERT_EMAIL_TO` |
| SMS recipient | Override `VERIZON_SMS_TO` |
| Test email | Send a test alert |
| Test SMS | Send a test alert |
| Clear saved | Revert to environment variables |

### Recipient Override

Recipients saved via the UI are stored in the database and take precedence over environment variables:

```sql
-- In alert_config table
key: alert_email_to
value: override@example.com
```

To revert to environment variables, use "Clear saved recipients" in the UI.

---

## Troubleshooting

### Email Not Sending

1. **Check SMTP credentials:**
   ```bash
   python -c "from src.alerting import send_email; print(send_email('Test', 'Body', return_error=True))"
   ```

2. **Verify Gmail app password:**
   - Must be 16 characters
   - Generated from Google Account settings
   - 2FA must be enabled

3. **Check firewall:**
   - Port 587 (TLS) or 465 (SSL) must be open
   - Some networks block SMTP

4. **Review logs:**
   - `logs/alerts_worker.log`
   - `logs/alerts_service_error.log`

### SMS Not Sending

1. **Verify phone number format:**
   - 10 digits only
   - No dashes, spaces, or country code

2. **Check carrier:**
   - Only Verizon is supported by default
   - Other carriers need code modification

3. **Test email first:**
   - SMS uses SMTP, so email must work

### NWS Alerts Not Working

1. **Check User-Agent:**
   ```bash
   NWS_USER_AGENT=TempestWeather/1.0 (contact: you@example.com)
   ```

2. **Verify location:**
   - Check `DAILY_BRIEF_LAT` and `DAILY_BRIEF_LON`
   - Or ensure Tempest API token is set for auto-detection

3. **Test API manually:**
   ```bash
   curl -H "User-Agent: TempestWeather/1.0" "https://api.weather.gov/alerts/active?point=33.7,-84.4"
   ```

### Duplicate Alerts

1. **Check worker vs UI:**
   - Set `ALERTS_WORKER_ENABLED=1` if using worker
   - This disables UI alerts

2. **Check deduplication tables:**
   ```sql
   SELECT * FROM nws_alert_log ORDER BY sent_at DESC LIMIT 10;
   SELECT * FROM alert_state;
   ```

3. **Clear alert state (if needed):**
   ```sql
   DELETE FROM alert_state;
   DELETE FROM nws_alert_log;
   ```

---

## Alert Message Examples

### Freeze Warning Email

```
Subject: Freeze Warning - Tempest 31.5 F

Freeze Warning

Current temperature: 31.5°F
Time: Jan 19, 2024 6:30 AM EST

Protect sensitive plants and exposed pipes.
```

### NWS Alert Email

```
Subject: NWS Alerts (2)

NWS Alerts:
- Winter Storm Warning (Extreme) until Jan 20 6:00 PM.
- Wind Advisory (Moderate) until Jan 19 10:00 PM.
```

### Daily Email

```
Subject: Tempest Morning Brief

Tempest Daily Brief - Jan 19 2024 7:00 AM

Current conditions
- Temp: 28.5 F
- Wind: 5.2 mph
- Pressure: 30.12 inHg
- Humidity: 65%
- PM2.5: 12

Daily brief (AI)
- A crisp winter morning with temperatures hovering near freezing
  - Overnight lows dipped to 26°F around 4 AM
  - Winds remained calm at 3-5 mph from the northwest
  - On this day: Average highs are 52°F with lows around 32°F
  - Tomorrow: Warming trend begins with highs near 45°F

48-hour outlook
- Next 48h: 28.5F to 48.2F, max wind 15.3 mph, precip 0.00 in.
  - Next hours:
    - 8 AM: 29F, wind 5 mph
    - 9 AM: 32F, wind 6 mph
    - 10 AM: 36F, wind 8 mph

NWS Outlooks & Alerts
- Outlook: Today: No hazardous weather expected. Next 7 days: A warming trend...
```
