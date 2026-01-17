import os
import time
import json
import hashlib
import sqlite3
import traceback
from pathlib import Path
from datetime import datetime

import requests

ROOT = Path(__file__).resolve().parents[1]
DB_PATH = Path(os.getenv("TEMPEST_DB_PATH", str(ROOT / "data" / "tempest.db")))
if not DB_PATH.is_absolute():
    DB_PATH = ROOT / DB_PATH
LOG_PATH = ROOT / "logs" / "airlink_collector.log"

HOST = os.environ.get("DAVIS_AIRLINK_HOST", "").rstrip("/")
URL = f"{HOST}/v1/current_conditions"

POLL_SEC = int(os.getenv("AIRLINK_POLL_SEC", "15"))
HTTP_TIMEOUT = int(os.getenv("AIRLINK_HTTP_TIMEOUT", "8"))
RETRY_SEC = int(os.getenv("AIRLINK_RETRY_SEC", "5"))

AIRLINK_OBS_TABLE = "airlink_current_obs"
AIRLINK_RAW_TABLE = "airlink_raw_all"
HEARTBEAT_TABLE = "collector_heartbeat"
HEARTBEAT_NAME = "airlink_collector"


def log(msg: str):
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"{ts} | {msg}"
    print(line, flush=True)
    with open(LOG_PATH, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def sha256(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def to_int(x):
    try:
        return None if x is None else int(float(x))
    except Exception:
        return None


def to_float(x):
    try:
        return None if x is None else float(x)
    except Exception:
        return None


def db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=15)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA busy_timeout=5000;")
    return conn


def table_exists(conn, name: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (name,),
    ).fetchone()
    return row is not None


def table_columns(conn, name: str) -> set:
    rows = conn.execute(f"PRAGMA table_info({name})").fetchall()
    return {row[1] for row in rows}


def migrate_legacy_airlink_obs(conn: sqlite3.Connection) -> None:
    if not table_exists(conn, "airlink_obs"):
        return
    cols = table_columns(conn, "airlink_obs")
    if {"did", "ts"}.issubset(cols):
        if not table_exists(conn, AIRLINK_OBS_TABLE):
            conn.execute(f"ALTER TABLE airlink_obs RENAME TO {AIRLINK_OBS_TABLE};")

def backfill_airlink_raw_all(conn: sqlite3.Connection) -> None:
    if not table_exists(conn, "airlink_raw") or not table_exists(conn, AIRLINK_RAW_TABLE):
        return
    count = conn.execute(f"SELECT COUNT(1) FROM {AIRLINK_RAW_TABLE}").fetchone()
    if count and count[0]:
        return

    # Handle legacy schema where payload_hash might be missing
    cols = table_columns(conn, "airlink_raw")
    hash_col = "payload_hash" if "payload_hash" in cols else "'legacy_no_hash'"

    conn.execute(
        f"""
        INSERT INTO {AIRLINK_RAW_TABLE} (
          received_at_epoch, host, did, ts, lsid, payload_json, payload_hash
        )
        SELECT received_at_epoch, host, did, ts, lsid, payload_json, {hash_col}
        FROM airlink_raw
        """
    )


def ensure_schema() -> None:
    with db() as conn:
        migrate_legacy_airlink_obs(conn)
        conn.executescript(
            f"""
CREATE TABLE IF NOT EXISTS airlink_raw (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  received_at_epoch INTEGER NOT NULL,
  host TEXT NOT NULL,
  did TEXT,
  ts INTEGER,
  lsid INTEGER,
  payload_json TEXT NOT NULL,
  payload_hash TEXT NOT NULL UNIQUE
);

CREATE INDEX IF NOT EXISTS idx_airlink_raw_received
  ON airlink_raw(received_at_epoch);

CREATE INDEX IF NOT EXISTS idx_airlink_raw_ts
  ON airlink_raw(ts);

CREATE TABLE IF NOT EXISTS {AIRLINK_RAW_TABLE} (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  received_at_epoch INTEGER NOT NULL,
  host TEXT NOT NULL,
  did TEXT,
  ts INTEGER,
  lsid INTEGER,
  payload_json TEXT NOT NULL,
  payload_hash TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_airlink_raw_all_received
  ON {AIRLINK_RAW_TABLE}(received_at_epoch);

CREATE INDEX IF NOT EXISTS idx_airlink_raw_all_ts
  ON {AIRLINK_RAW_TABLE}(ts);

CREATE TABLE IF NOT EXISTS {AIRLINK_OBS_TABLE} (
  did TEXT NOT NULL,
  ts INTEGER NOT NULL,

  lsid INTEGER,
  data_structure_type INTEGER,
  last_report_time INTEGER,

  temp_f REAL,
  hum REAL,
  dew_point_f REAL,
  wet_bulb_f REAL,
  heat_index_f REAL,

  pm_1 REAL,
  pm_2p5 REAL,
  pm_10 REAL,

  pm_1_last REAL,
  pm_2p5_last REAL,
  pm_10_last REAL,

  pm_1_last_1_hour REAL,
  pm_2p5_last_1_hour REAL,
  pm_10_last_1_hour REAL,

  pm_1_last_3_hours REAL,
  pm_2p5_last_3_hours REAL,
  pm_10_last_3_hours REAL,

  pm_1_last_24_hours REAL,
  pm_2p5_last_24_hours REAL,
  pm_10_last_24_hours REAL,

  pm_1_nowcast REAL,
  pm_2p5_nowcast REAL,
  pm_10_nowcast REAL,

  pct_pm_data_nowcast REAL,
  pct_pm_data_last_1_hour REAL,
  pct_pm_data_last_3_hours REAL,
  pct_pm_data_last_24_hours REAL,

  PRIMARY KEY (did, ts)
);

CREATE INDEX IF NOT EXISTS idx_airlink_current_obs_ts
  ON {AIRLINK_OBS_TABLE}(ts);

CREATE TABLE IF NOT EXISTS {HEARTBEAT_TABLE} (
  name TEXT PRIMARY KEY,
  last_ok_epoch INTEGER,
  last_error_epoch INTEGER,
  last_ok_message TEXT,
  last_error TEXT
);

CREATE INDEX IF NOT EXISTS idx_collector_heartbeat_last_ok
  ON {HEARTBEAT_TABLE}(last_ok_epoch);
"""
        )
        backfill_airlink_raw_all(conn)
        conn.commit()


def heartbeat_ok(conn: sqlite3.Connection, epoch: int, message: str) -> None:
    conn.execute(
        f"""
        INSERT INTO {HEARTBEAT_TABLE} (name, last_ok_epoch, last_ok_message)
        VALUES (?, ?, ?)
        ON CONFLICT(name) DO UPDATE SET
          last_ok_epoch=excluded.last_ok_epoch,
          last_ok_message=excluded.last_ok_message
        """,
        (HEARTBEAT_NAME, epoch, message),
    )


def heartbeat_error(conn: sqlite3.Connection, epoch: int, message: str) -> None:
    conn.execute(
        f"""
        INSERT INTO {HEARTBEAT_TABLE} (name, last_error_epoch, last_error)
        VALUES (?, ?, ?)
        ON CONFLICT(name) DO UPDATE SET
          last_error_epoch=excluded.last_error_epoch,
          last_error=excluded.last_error
        """,
        (HEARTBEAT_NAME, epoch, message),
    )

def run():
    global HOST, URL
    if not HOST:
        # Fallback to common IP if not set
        HOST = "http://192.168.1.1"
        URL = f"{HOST}/v1/current_conditions"
        log(f"WARNING: DAVIS_AIRLINK_HOST not set. Defaulting to {HOST}")

    try:
        ensure_schema()
    except Exception as e:
        log(f"CRITICAL: Schema init failed: {e}")
        traceback.print_exc()
        try:
            with db() as conn:
                heartbeat_error(conn, int(time.time()), f"Startup schema error: {e}")
                conn.commit()
        except Exception:
            pass
        return

    log(f"DB ready at: {DB_PATH}")
    log(f"Polling AirLink URL={URL} every {POLL_SEC}s")

    session = requests.Session()
    with db() as conn:
        heartbeat_ok(conn, int(time.time()), "startup ok")
        conn.commit()

    while True:
        try:
            r = session.get(URL, timeout=HTTP_TIMEOUT, headers={"Accept": "application/json"})
            r.raise_for_status()
            payload = r.json()

            received_at = int(time.time())
            payload_text = json.dumps(payload, separators=(",", ":"), sort_keys=True)
            payload_hash = sha256(payload_text)

            data = payload.get("data", {})
            did = data.get("did")
            ts = to_int(data.get("ts")) or received_at

            conds = data.get("conditions") or []
            c0 = conds[0] if conds else {}

            with db() as conn:
                # raw payload (append-only)
                conn.execute(
                    f"""
                    INSERT INTO {AIRLINK_RAW_TABLE}
                      (received_at_epoch, host, did, ts, lsid, payload_json, payload_hash)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        received_at,
                        HOST,
                        did,
                        ts,
                        to_int(c0.get("lsid")),
                        payload_text,
                        payload_hash,
                    ),
                )

                # structured observation
                conn.execute(
                    f"""
                    INSERT OR REPLACE INTO {AIRLINK_OBS_TABLE} (
                      did, ts,
                      lsid, data_structure_type, last_report_time,
                      temp_f, hum, dew_point_f, wet_bulb_f, heat_index_f,
                      pm_1, pm_2p5, pm_10,
                      pm_1_last, pm_2p5_last, pm_10_last,
                      pm_1_last_1_hour, pm_2p5_last_1_hour, pm_10_last_1_hour,
                      pm_1_last_3_hours, pm_2p5_last_3_hours, pm_10_last_3_hours,
                      pm_1_last_24_hours, pm_2p5_last_24_hours, pm_10_last_24_hours,
                      pm_1_nowcast, pm_2p5_nowcast, pm_10_nowcast,
                      pct_pm_data_nowcast, pct_pm_data_last_1_hour,
                      pct_pm_data_last_3_hours, pct_pm_data_last_24_hours
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        did,
                        ts,
                        to_int(c0.get("lsid")),
                        to_int(c0.get("data_structure_type")),
                        to_int(c0.get("last_report_time")),
                        to_float(c0.get("temp")),
                        to_float(c0.get("hum")),
                        to_float(c0.get("dew_point")),
                        to_float(c0.get("wet_bulb")),
                        to_float(c0.get("heat_index")),
                        to_float(c0.get("pm_1")),
                        to_float(c0.get("pm_2p5")),
                        to_float(c0.get("pm_10")),
                        to_float(c0.get("pm_1_last")),
                        to_float(c0.get("pm_2p5_last")),
                        to_float(c0.get("pm_10_last")),
                        to_float(c0.get("pm_1_last_1_hour")),
                        to_float(c0.get("pm_2p5_last_1_hour")),
                        to_float(c0.get("pm_10_last_1_hour")),
                        to_float(c0.get("pm_1_last_3_hours")),
                        to_float(c0.get("pm_2p5_last_3_hours")),
                        to_float(c0.get("pm_10_last_3_hours")),
                        to_float(c0.get("pm_1_last_24_hours")),
                        to_float(c0.get("pm_2p5_last_24_hours")),
                        to_float(c0.get("pm_10_last_24_hours")),
                        to_float(c0.get("pm_1_nowcast")),
                        to_float(c0.get("pm_2p5_nowcast")),
                        to_float(c0.get("pm_10_nowcast")),
                        to_float(c0.get("pct_pm_data_nowcast")),
                        to_float(c0.get("pct_pm_data_last_1_hour")),
                        to_float(c0.get("pct_pm_data_last_3_hours")),
                        to_float(c0.get("pct_pm_data_last_24_hours")),
                    ),
                )
                heartbeat_ok(conn, received_at, f"poll ok did={did}")
                conn.commit()

            log(
                f"Stored {AIRLINK_OBS_TABLE} did={did} ts={ts} "
                f"pm2.5={c0.get('pm_2p5')} temp_f={c0.get('temp')} hum={c0.get('hum')}"
            )

            time.sleep(POLL_SEC)

        except Exception as e:
            log(f"ERROR: {repr(e)}")
            traceback.print_exc()
            try:
                with db() as conn:
                    heartbeat_error(conn, int(time.time()), repr(e))
                    conn.commit()
            except Exception:
                pass
            time.sleep(RETRY_SEC)


if __name__ == "__main__":
    run()
