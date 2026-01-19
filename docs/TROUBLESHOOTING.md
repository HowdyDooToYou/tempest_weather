# Troubleshooting Guide

This guide covers common issues and their solutions for Tempest Weather.

## Quick Diagnostics

### Check System Status

```powershell
# Check all services
.\scripts\services.ps1 status

# Check environment configuration
.\scripts\services.ps1 env

# View recent logs
.\scripts\services.ps1 logs -LogLines 50
```

### Run Health Check

```bash
python -m src.collector_watchdog
```

**Exit codes:**
- `0`: All systems healthy
- `1`: Database or configuration error
- `2`: Stale data detected

---

## Installation Issues

### Python Not Found

**Symptom:** `python.exe not found` or `'python' is not recognized`

**Solutions:**
1. Install Python 3.10+ from [python.org](https://python.org)
2. Check "Add Python to PATH" during installation
3. Or specify full path:
   ```powershell
   .\scripts\install_services.ps1 -PythonExe "C:\Python312\python.exe"
   ```

### NSSM Not Found

**Symptom:** `nssm.exe not found`

**Solutions:**
1. Install via Chocolatey:
   ```powershell
   choco install nssm
   ```
2. Or download from [nssm.cc](https://nssm.cc/download)
3. Or specify path:
   ```powershell
   .\scripts\install_services.ps1 -NssmPath "C:\tools\nssm.exe"
   ```

### Virtual Environment Issues

**Symptom:** `ModuleNotFoundError` for streamlit, pandas, etc.

**Solutions:**
1. Create and activate venv:
   ```bash
   python -m venv .venv
   .\.venv\Scripts\activate
   pip install streamlit streamlit-autorefresh pandas altair requests
   ```
2. Ensure services use venv Python:
   ```powershell
   .\scripts\install_services.ps1 -PythonExe ".\.venv\Scripts\python.exe"
   ```

---

## Data Collection Issues

### No Tempest Data

**Symptom:** Dashboard shows "No data available" or empty charts

**Diagnostic Steps:**

1. **Check API token:**
   ```bash
   echo %TEMPEST_API_TOKEN%
   # Should show your token
   ```

2. **Test WebSocket connection:**
   ```bash
   python -m src.tempest_ws_test
   ```

3. **Check collector heartbeat:**
   ```sql
   SELECT * FROM collector_heartbeat WHERE name = 'tempest_collector';
   ```

4. **Review collector logs:**
   ```
   logs/collector.log
   ```

**Common Causes:**
- Invalid or expired API token
- Network/firewall blocking WebSocket
- Wrong device IDs in collector.py

### No AirLink Data

**Symptom:** AQI section empty or shows "--"

**Diagnostic Steps:**

1. **Verify host is set:**
   ```bash
   echo %DAVIS_AIRLINK_HOST%
   ```

2. **Ping the device:**
   ```bash
   ping 192.168.1.19
   ```

3. **Test API endpoint:**
   ```bash
   curl http://192.168.1.19/v1/current_conditions
   ```

4. **Check collector heartbeat:**
   ```sql
   SELECT * FROM collector_heartbeat WHERE name = 'airlink_collector';
   ```

**Common Causes:**
- Wrong IP address
- Device offline or rebooting
- Network segmentation (device on different VLAN)

### Stale Data

**Symptom:** Data stops updating, timestamps are old

**Diagnostic Steps:**

1. **Run watchdog:**
   ```bash
   python -m src.collector_watchdog
   ```

2. **Check latest data:**
   ```sql
   SELECT datetime(obs_epoch, 'unixepoch', 'localtime'), air_temperature 
   FROM obs_st ORDER BY obs_epoch DESC LIMIT 5;
   ```

3. **Check for errors:**
   ```sql
   SELECT * FROM collector_heartbeat;
   ```

**Common Causes:**
- Collector process crashed
- Database locked
- Network interruption

---

## Dashboard Issues

### Dashboard Won't Start

**Symptom:** `streamlit run dashboard.py` fails

**Diagnostic Steps:**

1. **Check syntax:**
   ```bash
   python -m py_compile dashboard.py
   ```

2. **Check imports:**
   ```bash
   python -c "import streamlit; import pandas; import altair"
   ```

3. **Check port availability:**
   ```powershell
   netstat -an | findstr 8501
   ```

**Common Causes:**
- Missing dependencies
- Port 8501 already in use
- Syntax error in code

### Charts Not Rendering

**Symptom:** Charts show "No data" or are blank

**Diagnostic Steps:**

1. **Check data exists:**
   ```sql
   SELECT COUNT(*) FROM obs_st WHERE obs_epoch > strftime('%s', 'now', '-24 hours');
   ```

2. **Check timezone:**
   ```bash
   echo %LOCAL_TZ%
   # Should be valid IANA timezone like America/New_York
   ```

3. **Check browser console:**
   - Press F12 in browser
   - Look for JavaScript errors

**Common Causes:**
- No data in selected time range
- Invalid timezone setting
- Browser caching issues (try Ctrl+Shift+R)

### Auto-Refresh Not Working

**Symptom:** Dashboard doesn't update automatically

**Solutions:**
1. Check `CONTROL_REFRESH_SECONDS` is set
2. Verify `streamlit-autorefresh` is installed
3. Check browser isn't blocking scripts

---

## Alerting Issues

### Emails Not Sending

**Symptom:** No email received, no errors shown

**Diagnostic Steps:**

1. **Test email directly:**
   ```python
   from src.alerting import send_email
   success, error = send_email("Test", "Body", return_error=True)
   print(f"Success: {success}, Error: {error}")
   ```

2. **Check SMTP settings:**
   ```bash
   echo %SMTP_USERNAME%
   echo %SMTP_HOST%
   echo %SMTP_PORT%
   ```

3. **Verify Gmail app password:**
   - Must be 16 characters
   - Generated from Google Account
   - 2FA must be enabled

**Common Causes:**
- Wrong app password (not regular password)
- 2FA not enabled on Gmail
- Firewall blocking port 587
- `ALERT_EMAIL_FROM` not set

### SMS Not Sending

**Symptom:** No SMS received

**Diagnostic Steps:**

1. **Verify phone format:**
   ```bash
   echo %VERIZON_SMS_TO%
   # Should be 10 digits only: 5551234567
   ```

2. **Test email first:**
   - SMS uses SMTP, so email must work

3. **Check carrier:**
   - Only Verizon is supported by default

**Common Causes:**
- Wrong phone format (no dashes or country code)
- Non-Verizon number
- SMTP not configured

### Duplicate Alerts

**Symptom:** Same alert received multiple times

**Diagnostic Steps:**

1. **Check worker vs UI:**
   ```bash
   echo %ALERTS_WORKER_ENABLED%
   # Should be 1 if using worker
   ```

2. **Check alert state:**
   ```sql
   SELECT * FROM alert_state;
   SELECT * FROM nws_alert_log ORDER BY sent_at DESC LIMIT 10;
   ```

**Solutions:**
- Set `ALERTS_WORKER_ENABLED=1` when using worker
- Clear alert state if corrupted:
  ```sql
  DELETE FROM alert_state;
  ```

### NWS Alerts Not Working

**Symptom:** No NWS alerts even during active weather

**Diagnostic Steps:**

1. **Check User-Agent:**
   ```bash
   echo %NWS_USER_AGENT%
   # Must be set with contact info
   ```

2. **Test API:**
   ```bash
   curl -H "User-Agent: TempestWeather/1.0" "https://api.weather.gov/alerts/active?point=33.7,-84.4"
   ```

3. **Check location:**
   ```bash
   echo %DAILY_BRIEF_LAT%
   echo %DAILY_BRIEF_LON%
   ```

**Common Causes:**
- Missing `NWS_USER_AGENT`
- Location not configured
- `NWS_ALERTS_ENABLED=0`

---

## Service Issues

### Service Won't Start

**Symptom:** NSSM service fails to start

**Diagnostic Steps:**

1. **Check service status:**
   ```powershell
   .\scripts\services.ps1 status
   ```

2. **Check error logs:**
   ```powershell
   .\scripts\services.ps1 logs -Target ui
   ```

3. **Check NSSM configuration:**
   ```powershell
   nssm edit TempestWeatherUI
   ```

4. **Try manual start:**
   ```powershell
   nssm start TempestWeatherUI
   ```

**Common Causes:**
- Wrong Python path
- Missing environment variables
- Database locked by another process

### Service Crashes Repeatedly

**Symptom:** Service starts then stops

**Diagnostic Steps:**

1. **Check error log:**
   ```
   logs/ui_service_error.log
   logs/alerts_service_error.log
   ```

2. **Run manually to see errors:**
   ```bash
   python -m streamlit run dashboard.py
   python -m src.alerts_worker --once
   ```

3. **Check Windows Event Viewer:**
   - Application logs
   - System logs

**Common Causes:**
- Unhandled exception in code
- Database corruption
- Out of memory

### Environment Variables Not Loading

**Symptom:** Service doesn't see environment variables

**Diagnostic Steps:**

1. **Check service environment:**
   ```powershell
   .\scripts\services.ps1 env
   ```

2. **Verify NSSM configuration:**
   ```powershell
   nssm get TempestWeatherUI AppEnvironmentExtra
   ```

**Solutions:**
- Re-run installer with variables set:
  ```powershell
  $env:TEMPEST_API_TOKEN = "your-token"
  .\scripts\install_services.ps1
  ```
- Or edit via NSSM GUI:
  ```powershell
  nssm edit TempestWeatherUI
  # Go to Environment tab
  ```

---

## Database Issues

### Database Locked

**Symptom:** `database is locked` errors

**Diagnostic Steps:**

1. **Find processes using database:**
   ```powershell
   Get-Process | Where-Object { $_.Modules.FileName -like "*tempest.db*" }
   ```

2. **Check for zombie processes:**
   ```powershell
   Get-Process python | Stop-Process -Force
   ```

**Solutions:**
- Stop all services before manual operations
- Ensure WAL mode is enabled:
  ```sql
  PRAGMA journal_mode;
  -- Should return "wal"
  ```

### Database Corruption

**Symptom:** `database disk image is malformed`

**Solutions:**

1. **Try integrity check:**
   ```sql
   PRAGMA integrity_check;
   ```

2. **Attempt recovery:**
   ```bash
   sqlite3 data/tempest.db ".recover" | sqlite3 data/tempest_recovered.db
   ```

3. **Restore from backup:**
   - If you have backups, restore the latest
   - Otherwise, delete and let collectors rebuild

### Missing Tables

**Symptom:** `no such table` errors

**Solutions:**

1. **Run collector once:**
   ```bash
   python -m src.collector
   # Ctrl+C after a few seconds
   ```

2. **Or create manually:**
   ```bash
   python -c "from src.collector import db_connect; db_connect()"
   ```

---

## Performance Issues

### High CPU Usage

**Symptom:** Python process using excessive CPU

**Diagnostic Steps:**

1. **Check which process:**
   ```powershell
   Get-Process python | Select-Object Id, CPU, WorkingSet
   ```

2. **Profile the code:**
   ```bash
   python -m cProfile -s cumtime dashboard.py
   ```

**Common Causes:**
- Tight polling loop
- Large dataset processing
- Memory leak causing GC pressure

### High Memory Usage

**Symptom:** Memory grows over time

**Solutions:**
1. Restart services periodically
2. Reduce data retention:
   ```sql
   DELETE FROM raw_events WHERE received_at_epoch < strftime('%s', 'now', '-7 days');
   VACUUM;
   ```

### Slow Dashboard

**Symptom:** Pages take long to load

**Solutions:**
1. Reduce time range in queries
2. Add database indexes:
   ```sql
   CREATE INDEX IF NOT EXISTS idx_obs_st_epoch ON obs_st(obs_epoch);
   ```
3. Increase `CONTROL_REFRESH_SECONDS`

---

## Log Locations

| Component | Log File |
|-----------|----------|
| Tempest Collector | `logs/collector.log` |
| AirLink Collector | `logs/airlink_collector.log` |
| Alerts Worker | `logs/alerts_worker.log` |
| Collector Watchdog | `logs/collector_watchdog.log` |
| UI Service (stdout) | `logs/ui_service.log` |
| UI Service (stderr) | `logs/ui_service_error.log` |
| Alerts Service (stderr) | `logs/alerts_service_error.log` |
| Daily Brief Service | `logs/daily_brief_service.log` |
| Daily Email Service | `logs/daily_email_service.log` |

### Viewing Logs

```powershell
# View last 100 lines of UI log
Get-Content logs/ui_service.log -Tail 100

# Follow log in real-time
Get-Content logs/collector.log -Wait

# Search for errors
Select-String -Path logs/*.log -Pattern "ERROR|WARN"
```

---

## Getting Help

### Information to Gather

When reporting issues, include:

1. **System info:**
   ```powershell
   python --version
   pip list | findstr streamlit
   ```

2. **Service status:**
   ```powershell
   .\scripts\services.ps1 status
   .\scripts\services.ps1 env
   ```

3. **Recent logs:**
   ```powershell
   .\scripts\services.ps1 logs -LogLines 50
   ```

4. **Database state:**
   ```sql
   SELECT name, last_ok_epoch, last_error FROM collector_heartbeat;
   SELECT COUNT(*) FROM obs_st;
   ```

### Common Commands Reference

```powershell
# Service management
.\scripts\services.ps1 status
.\scripts\services.ps1 start
.\scripts\services.ps1 stop
.\scripts\services.ps1 restart
.\scripts\services.ps1 logs

# Manual testing
python -m src.collector_watchdog
python -m src.alerts_worker --once
python -m streamlit run dashboard.py

# Database inspection
sqlite3 data/tempest.db ".tables"
sqlite3 data/tempest.db "SELECT * FROM collector_heartbeat"
```
