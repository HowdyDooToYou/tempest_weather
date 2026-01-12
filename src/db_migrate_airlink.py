import os
import sqlite3
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DB_PATH = Path(os.getenv("TEMPEST_DB_PATH", str(ROOT / "data" / "tempest.db")))
if not DB_PATH.is_absolute():
    DB_PATH = ROOT / DB_PATH

DDL = """
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

CREATE TABLE IF NOT EXISTS airlink_raw_all (
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
  ON airlink_raw_all(received_at_epoch);

CREATE INDEX IF NOT EXISTS idx_airlink_raw_all_ts
  ON airlink_raw_all(ts);

CREATE TABLE IF NOT EXISTS airlink_current_obs (
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
  ON airlink_current_obs(ts);
"""

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
        if not table_exists(conn, "airlink_current_obs"):
            conn.execute("ALTER TABLE airlink_obs RENAME TO airlink_current_obs;")
        return

def backfill_airlink_raw_all(conn: sqlite3.Connection) -> None:
    if not table_exists(conn, "airlink_raw") or not table_exists(conn, "airlink_raw_all"):
        return
    count = conn.execute("SELECT COUNT(1) FROM airlink_raw_all").fetchone()
    if count and count[0]:
        return
    conn.execute(
        """
        INSERT INTO airlink_raw_all (
          received_at_epoch, host, did, ts, lsid, payload_json, payload_hash
        )
        SELECT received_at_epoch, host, did, ts, lsid, payload_json, payload_hash
        FROM airlink_raw
        """
    )

def main():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        migrate_legacy_airlink_obs(conn)
        conn.executescript(DDL)
        backfill_airlink_raw_all(conn)
        conn.commit()
    print(f"OK: migrated DB at {DB_PATH}")

if __name__ == "__main__":
    main()
