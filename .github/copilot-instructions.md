<!-- Tempest Weather — Copilot instructions for AI coding agents -->

# Summary

Be concise and project-specific: this repo is a Streamlit dashboard + background collectors and workers that ingest WeatherFlow Tempest and Davis AirLink data into a local SQLite DB (`data/tempest.db`). Key goals for code changes are safety of data ingestion, backwards-compatible DB migrations, and predictable behavior when run as Windows services (NSSM).

# Big-picture architecture (quick)

- Data collectors: `src/collector.py` (WebSocket Tempest), `src/airlink_collector.py` (AirLink). They write a lossless raw capture table (`raw_events`) and structured tables like `obs_st`.
- Persistence: local SQLite at `data/tempest.db`. Code consistently sets PRAGMA `journal_mode=WAL`, `synchronous=NORMAL`, and `busy_timeout` for concurrency.
- Workers: `src/alerts_worker.py`, `src/daily_brief_worker.py`, `src/daily_email_worker.py` — read DB, compute alerts/briefs, call `src/alerting.py` and helper APIs.
- UI: `dashboard.py` (Streamlit) and `src/pages/*`. The UI reads DB tables (often discovered with `resolve_table(...)`) and uses `src/ui/*` for styling and components.
- Service management: PowerShell scripts in `scripts/` install/manage NSSM services (see `scripts/install_services.ps1`, `scripts/services.ps1`).

# Project-specific conventions and patterns

- Environment variables are the primary config; many modules read `TEMPEST_DB_PATH`, `TEMPEST_API_TOKEN`, etc. Boolean env flags accept `1,true,yes,on` (see `src/alerting.py::_env_flag`).
- Database migrations are idempotent and non-destructive. Collectors call `migrate(conn)` and add columns/indexes if missing — follow that pattern for schema changes (preserve old columns, add non-breaking indices).
- Lossless ingestion: collectors always store raw text (`payload_text`) and a hash (`payload_hash`) alongside any parsed JSON; never drop malformed messages.
- Commit batching: collectors use `COMMIT_EVERY_N_MESSAGES` and `COMMIT_EVERY_SECONDS`. When adding high-volume writes preserve batching/backpressure logic.
- Time values: many modules use UNIX epoch integers; prefer UTC for storage and convert to local TZ at display time (see `src/forecast.py` and `dashboard.py`).
- Windows secrets: SMTP credentials may come from environment or Windows Credential Manager (see `_read_windows_credential` in `src/alerting.py`). Default credential target: `TempestWeatherSMTP`.

# How to run / developer workflows

- Run dashboard locally:

  ```bash
  streamlit run dashboard.py
  ```

  or on Windows use `.\scripts\run_streamlit.ps1`.

- Run collectors/workers manually for debugging:

  ```bash
  python -m src.collector      # Tempest WS collector
  python -m src.airlink_collector
  python -m src.alerts_worker  # single-run: add `--once`
  ```

- Tests:

  ```bash
  python -m pytest tests/
  ```

- Lint / quick syntax check used in docs:

  ```bash
  python -m py_compile dashboard.py src/alerting.py src/alerts_worker.py
  ```

- NSSM services (install / manage): use `scripts/install_services.ps1` and `scripts/services.ps1`.

# Implementation guidance for common changes

- Adding a new collector: mirror `src/collector.py`'s lossless pattern — always write `raw_events` (text + hash), then optionally populate structured tables. Call `migrate(conn)` or write idempotent ALTER statements.
- Adding a DB column/table: add an idempotent `CREATE TABLE IF NOT EXISTS` or `ALTER TABLE ... ADD COLUMN` guarded by existence checks (see `column_exists()` / `migrate()` in `src/collector.py`).
- Adding alerts/delivery: use `src/alerting.py` helpers: `get_email_config()`, `send_email()`, `send_verizon_sms()`, `load_alert_state()`/`save_alert_state()` and `load_alert_config()`/`save_alert_config()`.
- Accessing config values at runtime: use `src/config_store.py` (connect, get_bool/get_float/set_bool/set_float) to store UI-editable settings.

# Useful code examples (copyable patterns)

- Lossless insert (from `src/collector.py`):

  ```py
  insert_raw_lossless(conn, received_at, device_id, msg_type, payload_text, payload_json_str)
  ```

- DB connection template (use `PRAGMA` settings):

  ```py
  conn = sqlite3.connect(DB_PATH)
  conn.execute("PRAGMA journal_mode=WAL;")
  ```

- Resolve recipients for alerts (use config overrides):

  ```py
  email, sms = resolve_alert_recipients(str(db_path), overrides=None)
  ```

# Files to inspect when making changes

- UI and UX: `dashboard.py`, `src/pages/*`, `src/ui/*`
- Collectors: `src/collector.py`, `src/airlink_collector.py`, `src/collector_watchdog.py`
- Persistence/config: `src/config_store.py`, `data/` (actual DB)
- Alerting & delivery: `src/alerting.py`, `src/alerts_worker.py`
- Forecast parsing: `src/forecast.py` (returns pandas DataFrames)
- Service automation: `scripts/*.ps1`

# Testing and safety notes

- DB migrations must be additive and idempotent; avoid destructive ALTERs. Back up `data/tempest.db` before mass schema changes.
- When touching high-volume ingestion paths, preserve batching/commit behavior and WAL pragmas to avoid locking.
- Avoid shipping secrets; tests and CI should not rely on real API tokens. Use environment mocks in tests.

# What not to change without discussion

- Global DB PRAGMA choices (WAL/synchronous) — these were selected for concurrent writer/readers and Windows services.
- Raw-event schema semantics (payload_text + payload_hash) — used for forensic replay and dedupe.

# If you need more

Open `docs/ARCHITECTURE.md` and `docs/COLLECTORS.md` for deeper context. After edits, run unit tests and exercise the Streamlit UI locally; when changing collectors run them with logging enabled and verify `raw_events` entries appear.

---
_If anything above is unclear or you want examples for a specific change (e.g., add a new alert type or collector), tell me which area and I'll expand with a focused snippet._
