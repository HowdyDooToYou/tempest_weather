# Data Collectors

This document describes the data collection components that ingest weather and air quality data into the Tempest Weather system.

## Overview

Tempest Weather uses two primary collectors:

| Collector | File | Protocol | Data Source |
|-----------|------|----------|-------------|
| Tempest Collector | `src/collector.py` | WebSocket | WeatherFlow cloud |
| AirLink Collector | `src/airlink_collector.py` | HTTP | Local AirLink device |

Both collectors write to the same SQLite database and maintain heartbeat records for health monitoring.

---

## Tempest Collector

### Purpose

The Tempest Collector maintains a persistent WebSocket connection to WeatherFlow's cloud service, receiving real-time observations from your Tempest weather station.

### How It Works

```
WeatherFlow Cloud ──WebSocket──> collector.py ──> SQLite
                                     │
                                     ├── raw_events (lossless)
                                     ├── obs_st (parsed)
                                     └── collector_heartbeat
```

1. **Connection**: Connects to `wss://ws.weatherflow.com/swd/data?token={TOKEN}`
2. **Subscription**: Sends `listen_start` for configured device IDs
3. **Reception**: Receives JSON messages for observations and heartbeats
4. **Storage**: Stores raw JSON losslessly, parses `obs_st` messages to structured table
5. **Heartbeat**: Updates `collector_heartbeat` table every 30 seconds

### Message Types

| Type | Description | Stored In |
|------|-------------|-----------|
| `obs_st` | Tempest station observations | `raw_events` + `obs_st` |
| `hub_status` | Hub connectivity status | `raw_events` only |
| `device_status` | Device battery/signal | `raw_events` only |
| `evt_precip` | Rain start event | `raw_events` only |
| `evt_strike` | Lightning strike | `raw_events` only |

### Observation Fields (`obs_st`)

Each `obs_st` message contains an array of 18 values:

| Index | Field | Unit | Description |
|-------|-------|------|-------------|
| 0 | `obs_epoch` | seconds | Unix timestamp |
| 1 | `wind_lull` | m/s | Minimum wind speed |
| 2 | `wind_avg` | m/s | Average wind speed |
| 3 | `wind_gust` | m/s | Maximum wind speed |
| 4 | `wind_dir` | degrees | Wind direction |
| 5 | `wind_interval` | seconds | Sampling interval |
| 6 | `station_pressure` | mb | Station pressure |
| 7 | `air_temperature` | °C | Air temperature |
| 8 | `relative_humidity` | % | Relative humidity |
| 9 | `illuminance` | lux | Light level |
| 10 | `uv` | index | UV index |
| 11 | `solar_radiation` | W/m² | Solar radiation |
| 12 | `rain_accumulated` | mm | Rain since last report |
| 13 | `precip_type` | enum | 0=none, 1=rain, 2=hail |
| 14 | `lightning_avg_dist` | km | Average lightning distance |
| 15 | `lightning_strike_count` | count | Strikes since last report |
| 16 | `battery` | volts | Battery voltage |
| 17 | `report_interval` | minutes | Reporting interval |

### Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `TEMPEST_API_TOKEN` | Required | WeatherFlow API token |
| `TEMPEST_DB_PATH` | `data/tempest.db` | Database path |

Device IDs are configured in the source code:
```python
DEVICE_IDS = [475329, 475327]  # Station, Hub
```

### Reconnection Strategy

The collector uses exponential backoff for reconnection:

- **Base delay**: 5 seconds
- **Maximum delay**: 300 seconds (5 minutes)
- **Backoff multiplier**: 2x after each failure
- **Reset**: Delay resets to base after successful connection

### Lossless Storage

All WebSocket messages are stored losslessly:

1. **Raw text**: Original message stored in `payload_text`
2. **Parsed JSON**: Normalized JSON in `payload_json`
3. **Hash**: SHA-256 fingerprint in `payload_hash` for deduplication
4. **Metadata**: `received_at_epoch`, `device_id`, `message_type`

Even if JSON parsing fails, the raw text is preserved.

### Running the Collector

**Manual execution:**
```bash
python -m src.collector
```

**As a Windows service:**
```powershell
.\scripts\services.ps1 start -Target ui
# The collector runs as part of the main application
```

**Standalone service (if needed):**
```powershell
nssm install TempestCollector python.exe "-m src.collector"
nssm set TempestCollector AppDirectory "C:\path\to\tempest_weather"
```

---

## AirLink Collector

### Purpose

The AirLink Collector polls a Davis AirLink air quality sensor on your local network, retrieving PM1, PM2.5, PM10, and AQI readings.

### How It Works

```
AirLink Device ──HTTP GET──> airlink_collector.py ──> SQLite
                                      │
                                      ├── airlink_raw_all (lossless)
                                      ├── airlink_current_obs (parsed)
                                      └── collector_heartbeat
```

1. **Polling**: HTTP GET to `http://{HOST}/v1/current_conditions`
2. **Parsing**: Extracts PM and AQI values from JSON response
3. **Storage**: Stores raw JSON and parsed observations
4. **Heartbeat**: Updates `collector_heartbeat` table

### API Endpoint

The AirLink exposes a local REST API:

```
GET http://{DAVIS_AIRLINK_HOST}/v1/current_conditions
```

**Response structure:**
```json
{
  "data": {
    "did": "001D0A100000",
    "ts": 1705632000,
    "conditions": [
      {
        "lsid": 123456,
        "data_structure_type": 6,
        "temp": 72.5,
        "hum": 45.2,
        "pm_1": 5.2,
        "pm_2p5": 8.7,
        "pm_10": 12.3,
        "pm_2p5_last_1_hour": 9.1,
        "pm_2p5_last_3_hours": 8.5,
        "pm_2p5_last_24_hours": 7.8,
        "aqi_val": 35,
        "aqi_pm_2p5_val": 35,
        "aqi_pm_10_val": 12
      }
    ]
  }
}
```

### Observation Fields

| Field | Unit | Description |
|-------|------|-------------|
| `pm_1` | µg/m³ | PM1.0 concentration |
| `pm_2p5` | µg/m³ | PM2.5 concentration |
| `pm_10` | µg/m³ | PM10 concentration |
| `pm_2p5_last_1_hour` | µg/m³ | 1-hour average PM2.5 |
| `pm_2p5_last_3_hours` | µg/m³ | 3-hour average PM2.5 |
| `pm_2p5_last_24_hours` | µg/m³ | 24-hour average PM2.5 |
| `aqi_val` | index | Overall AQI |
| `aqi_pm_2p5_val` | index | PM2.5 AQI |
| `aqi_pm_10_val` | index | PM10 AQI |
| `temp` | °F | Internal temperature |
| `hum` | % | Internal humidity |

### Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `DAVIS_AIRLINK_HOST` | Required | IP address or hostname |
| `AIRLINK_POLL_SEC` | `15` | Polling interval (seconds) |
| `AIRLINK_HTTP_TIMEOUT` | `8` | HTTP timeout (seconds) |
| `AIRLINK_RETRY_SEC` | `5` | Retry delay after failure |
| `TEMPEST_DB_PATH` | `data/tempest.db` | Database path |

### Finding Your AirLink IP

1. Check your router's DHCP client list
2. Use the WeatherLink app to find the device
3. Scan your network: `nmap -sn 192.168.1.0/24`

### Running the Collector

**Manual execution:**
```bash
python -m src.airlink_collector
```

**As a Windows service:**
```powershell
nssm install TempestAirLink python.exe "-m src.airlink_collector"
nssm set TempestAirLink AppDirectory "C:\path\to\tempest_weather"
nssm set TempestAirLink AppEnvironmentExtra "DAVIS_AIRLINK_HOST=192.168.1.19"
```

---

## Collector Watchdog

### Purpose

The Collector Watchdog monitors the health of both collectors by checking heartbeat timestamps and data freshness.

### How It Works

```
SQLite ──> collector_watchdog.py ──> stdout/log
                │
                ├── Check collector_heartbeat timestamps
                └── Check latest data timestamps
```

### Health Checks

| Check | Table | Column | Default Threshold |
|-------|-------|--------|-------------------|
| Tempest heartbeat | `collector_heartbeat` | `last_ok_epoch` | 300s (5 min) |
| AirLink heartbeat | `collector_heartbeat` | `last_ok_epoch` | 180s (3 min) |
| Tempest data | `obs_st` | `obs_epoch` | 900s (15 min) |
| AirLink data | `airlink_current_obs` | `ts` | 300s (5 min) |

### Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `WATCHDOG_TEMPEST_HEARTBEAT_SEC` | `300` | Tempest heartbeat threshold |
| `WATCHDOG_AIRLINK_HEARTBEAT_SEC` | `180` | AirLink heartbeat threshold |
| `WATCHDOG_TEMPEST_DATA_SEC` | `900` | Tempest data threshold |
| `WATCHDOG_AIRLINK_DATA_SEC` | `300` | AirLink data threshold |

### Running the Watchdog

**Manual execution:**
```bash
python -m src.collector_watchdog
```

**Exit codes:**
- `0`: All checks passed
- `1`: Database or table missing
- `2`: One or more checks failed (stale data)

**Example output:**
```
2024-01-19 10:30:00 | OK: Tempest Collector: ok (45s ago) | AirLink Collector: ok (12s ago) | AirLink Data: ok (12s ago) | Tempest Data: ok (45s ago)
```

### Scheduled Monitoring

You can run the watchdog on a schedule using Task Scheduler:

```powershell
# Create a scheduled task to run every 5 minutes
$action = New-ScheduledTaskAction -Execute "python.exe" -Argument "-m src.collector_watchdog" -WorkingDirectory "C:\path\to\tempest_weather"
$trigger = New-ScheduledTaskTrigger -RepetitionInterval (New-TimeSpan -Minutes 5) -Once -At (Get-Date)
Register-ScheduledTask -TaskName "TempestWatchdog" -Action $action -Trigger $trigger
```

---

## Heartbeat System

Both collectors maintain heartbeat records in the `collector_heartbeat` table:

```sql
CREATE TABLE collector_heartbeat (
    name TEXT PRIMARY KEY,
    last_ok_epoch INTEGER,
    last_error_epoch INTEGER,
    last_ok_message TEXT,
    last_error TEXT
);
```

### Heartbeat Names

| Name | Collector |
|------|-----------|
| `tempest_collector` | Tempest WebSocket collector |
| `airlink_collector` | AirLink HTTP collector |

### Heartbeat Messages

| Message | Meaning |
|---------|---------|
| `startup ok` | Collector just started |
| `ws connected` | WebSocket connection established |
| `ingesting` | Actively receiving data |
| `connected idle` | Connected but no recent messages |

### Querying Heartbeats

```sql
-- Check all collector statuses
SELECT 
    name,
    datetime(last_ok_epoch, 'unixepoch', 'localtime') as last_ok,
    last_ok_message,
    datetime(last_error_epoch, 'unixepoch', 'localtime') as last_error,
    last_error
FROM collector_heartbeat;
```

---

## Troubleshooting

### Tempest Collector Issues

**No data being collected:**
1. Verify `TEMPEST_API_TOKEN` is set correctly
2. Check WebSocket connectivity: `python -m src.tempest_ws_test`
3. Review logs: `logs/collector.log`

**Frequent disconnections:**
1. Check internet stability
2. Verify token hasn't expired
3. Look for rate limiting (too many connections)

**Missing observations:**
1. Check device IDs match your station
2. Verify station is online in WeatherFlow app
3. Check `raw_events` table for incoming messages

### AirLink Collector Issues

**Connection refused:**
1. Verify `DAVIS_AIRLINK_HOST` is correct
2. Ping the device: `ping 192.168.1.19`
3. Check device is powered on and connected to network

**Timeout errors:**
1. Increase `AIRLINK_HTTP_TIMEOUT`
2. Check for network congestion
3. Verify device isn't overloaded

**Empty responses:**
1. Check AirLink firmware is up to date
2. Verify API endpoint: `curl http://{HOST}/v1/current_conditions`
3. Review device logs in WeatherLink app

### Database Issues

**Database locked:**
1. Ensure only one collector instance is running
2. Check for zombie processes
3. Verify WAL mode is enabled

**Table missing:**
1. Run collector once to create schema
2. Check `TEMPEST_DB_PATH` is correct
3. Verify write permissions on data directory

---

## Data Retention

By default, all data is retained indefinitely. For long-term deployments, consider:

### Pruning Old Raw Events

```sql
-- Delete raw events older than 30 days
DELETE FROM raw_events 
WHERE received_at_epoch < strftime('%s', 'now', '-30 days');

-- Vacuum to reclaim space
VACUUM;
```

### Archiving Strategy

1. Export old data to CSV/Parquet
2. Delete from SQLite
3. Store archives externally

```python
import pandas as pd
import sqlite3

conn = sqlite3.connect('data/tempest.db')
df = pd.read_sql_query("""
    SELECT * FROM obs_st 
    WHERE obs_epoch < strftime('%s', 'now', '-90 days')
""", conn)
df.to_parquet(f'archive/obs_st_{date}.parquet')
```
