# Tempest Weather and Air Quality Dashboard

A personal weather station dashboard built with Streamlit that aggregates real-time data from WeatherFlow Tempest devices and Davis AirLink air quality sensors.

![Example screen shot of Home page](docs/images/home.png)

## Features

- **Real-time Monitoring**: Live gauges for temperature, humidity, wind, pressure, UV index
- **Air Quality Tracking**: PM2.5/PM10/AQI with smoke event suppression toggle
- **Forecast Integration**: Tempest better_forecast API with Open-Meteo fallback
- **Radar & Overlays**: Past/forecast radar loops with NOAA nowCOAST layers
- **AI Daily Briefs**: GPT-4o-mini generated weather summaries with historical context
- **Multi-channel Alerts**: Email + Verizon SMS for freeze warnings and NWS severe weather (with duration tracking)
- **Theme System**: Dark/light modes with custom CSS token picker
- **Windows Service Support**: NSSM-based service installation for headless operation

### Dashboard Pages

| Page | Description |
|------|-------------|
| **Home** | Daily brief, forecast chart, 7-day outlook, radar maps, NWS alerts |
| **Trends** | Reorderable time-series charts with metric selection |
| **Compare** | Today vs yesterday, week vs week, year vs year overlays |
| **Data** | Raw tables, health status, diagnostics |

## Supported Devices

| Device | Data Collected |
|--------|----------------|
| **Tempest Station** | Temperature, humidity, wind, pressure, UV, solar radiation, rain, lightning |
| **Tempest Hub** | Connectivity heartbeat |
| **Davis AirLink** | PM1, PM2.5, PM10, AQI (outdoor) |

## Quick Start

### Prerequisites

- Python 3.10+ (tested on 3.12)
- Windows 10/11 (for NSSM services)
- WeatherFlow Tempest station with API token
- Optional: Davis AirLink sensor, OpenAI API key

### Installation

1. **Clone and setup virtual environment:**
   ```bash
   git clone https://github.com/yourusername/tempest_weather.git
   cd tempest_weather
   python -m venv .venv
   .\.venv\Scripts\activate
   ```

2. **Install dependencies:**
   ```bash
   pip install streamlit streamlit-autorefresh pandas altair requests websocket-client
   pip install openai  # Optional: for AI daily briefs
   pip install meteostat  # Optional: for historical context
   ```

3. **Configure environment:**
   ```powershell
   copy .env.example .env
   # Edit .env with your API tokens and settings
   ```

4. **Run the dashboard:**
   ```bash
   streamlit run dashboard.py
   ```

   Or use the convenience script:
   ```powershell
   .\scripts\run_streamlit.ps1
   ```

## Configuration

### Essential Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `TEMPEST_API_TOKEN` | Yes | WeatherFlow API token |
| `LOCAL_TZ` | No | Timezone (default: `America/New_York`) |
| `TEMPEST_DB_PATH` | No | Database path (default: `data/tempest.db`) |

### Optional Features

| Feature | Variables |
|---------|-----------|
| **AirLink** | `DAVIS_AIRLINK_HOST` |
| **AI Briefs** | `OPENAI_API_KEY`, `DAILY_BRIEF_MODEL` |
| **Email Alerts** | `SMTP_USERNAME`, `SMTP_PASSWORD`, `ALERT_EMAIL_TO` |
| **SMS Alerts** | `VERIZON_SMS_TO` |
| **NWS Alerts** | `NWS_USER_AGENT`, `NWS_ALERTS_ENABLED` |

📖 **Full configuration reference:** [docs/CONFIGURATION.md](docs/CONFIGURATION.md)

## Architecture

```
┌─────────────────┐     ┌─────────────────┐
│ WeatherFlow WS  │     │  Davis AirLink  │
└────────┬────────┘     └────────┬────────┘
         │                       │
         ▼                       ▼
┌─────────────────────────────────────────┐
│           Data Collectors               │
│  (collector.py, airlink_collector.py)   │
└────────────────┬────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────┐
│         SQLite Database                 │
│         (data/tempest.db)               │
└────────────────┬────────────────────────┘
                 │
    ┌────────────┼────────────┐
    ▼            ▼            ▼
┌────────┐  ┌────────┐  ┌────────┐
│  UI    │  │ Alerts │  │ Daily  │
│Service │  │ Worker │  │ Brief  │
└────────┘  └────────┘  └────────┘
```

📖 **Full architecture documentation:** [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)

## Windows Services (NSSM)

### Install Services

1. Install NSSM:
   ```powershell
   choco install nssm
   ```

2. Set environment variables and install:
   ```powershell
   $env:TEMPEST_API_TOKEN = "your-token"
   $env:SMTP_USERNAME = "you@gmail.com"
   # ... other variables
   .\scripts\install_services.ps1
   ```

### Manage Services

```powershell
.\scripts\services.ps1 status              # Check all services
.\scripts\services.ps1 start               # Start all services
.\scripts\services.ps1 restart -Target ui  # Restart specific service
.\scripts\services.ps1 logs -Target alerts # View logs
.\scripts\services.ps1 env                 # Check environment config
```

### Services Installed

| Service | Purpose |
|---------|---------|
| `TempestWeatherUI` | Streamlit dashboard (port 8501) |
| `TempestWeatherAlerts` | Freeze + NWS alert monitoring |
| `TempestWeatherDailyBrief` | AI weather digest (every 3 hours) |
| `TempestWeatherDailyEmail` | Morning email summary (7am) |

## Alerting

### Freeze Alerts

Automatic notifications when temperature drops below thresholds:

| Alert | Default Threshold |
|-------|-------------------|
| Freeze Warning | 32°F |
| Deep Freeze | 18°F |
| Reset | 34°F |

The Home page banner displays how long a freeze condition has been active based on saved alert state.

### NWS Integration

- Active weather alerts from api.weather.gov
- Hazardous Weather Outlook (HWO)
- Automatic zone detection from coordinates

### Delivery Channels

- **Email**: SMTP (Gmail supported, with app passwords)
- **SMS**: Verizon email-to-SMS gateway

📖 **Full alerting documentation:** [docs/ALERTING.md](docs/ALERTING.md)

## Documentation

| Document | Description |
|----------|-------------|
| [ARCHITECTURE.md](docs/ARCHITECTURE.md) | System design, data flow, database schema |
| [CONFIGURATION.md](docs/CONFIGURATION.md) | Complete environment variable reference |
| [COLLECTORS.md](docs/COLLECTORS.md) | Data collector setup and operation |
| [ALERTING.md](docs/ALERTING.md) | Alert system configuration |
| [TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md) | Common issues and solutions |

## Troubleshooting

### Quick Diagnostics

```powershell
# Check system health
python -m src.collector_watchdog

# Check service status
.\scripts\services.ps1 status

# View recent logs
.\scripts\services.ps1 logs -LogLines 50
```

### Common Issues

| Issue | Solution |
|-------|----------|
| No data appearing | Check `TEMPEST_API_TOKEN` is set |
| Emails not sending | Verify Gmail app password (not regular password) |
| Service won't start | Check logs: `.\scripts\services.ps1 logs -Target ui` |
| Database locked | Stop all services, check for zombie processes |

📖 **Full troubleshooting guide:** [docs/TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md)

## Development

### Syntax Check

```bash
python -m py_compile dashboard.py src/alerting.py src/alerts_worker.py
```

### Run Tests

```bash
python -m pytest tests/
```

### Project Structure

```
tempest_weather/
├── dashboard.py           # Main Streamlit entry point
├── src/
│   ├── collector.py       # Tempest WebSocket collector
│   ├── airlink_collector.py # AirLink HTTP collector
│   ├── alerting.py        # Alert delivery (email/SMS)
│   ├── alerts_worker.py   # Background alert service
│   ├── daily_brief_worker.py # AI brief generator
│   ├── daily_email_worker.py # Morning email service
│   ├── nws_alerts.py      # NWS API integration
│   ├── forecast.py        # Forecast parsing
│   └── pages/             # Dashboard pages
├── scripts/               # PowerShell utilities
├── docs/                  # Documentation
├── data/                  # SQLite database (gitignored)
└── logs/                  # Service logs (gitignored)
```

## Privacy & Security

- **API tokens**: Stored in environment variables only
- **SMTP credentials**: Support Windows Credential Manager
- **Database**: Local SQLite, gitignored by default
- **No telemetry**: All data stays on your machine

## License

MIT License - See LICENSE file for details.

## Acknowledgments

- [WeatherFlow](https://weatherflow.com/) for the Tempest API
- [Davis Instruments](https://www.davisinstruments.com/) for AirLink
- [National Weather Service](https://www.weather.gov/) for alert data
- [Iowa Environmental Mesonet](https://mesonet.agron.iastate.edu/) for radar services
- [NOAA nowCOAST](https://nowcoast.noaa.gov/) for weather overlays
- [Open-Meteo](https://open-meteo.com/) for forecast fallback
- [OpenAI](https://openai.com/) for daily brief generation
