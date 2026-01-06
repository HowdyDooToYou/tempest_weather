import os
import sqlite3
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "data" / "tempest.db"

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

CREATE TABLE IF NOT EXISTS airlink_obs (
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

CREATE INDEX IF NOT EXISTS idx_airlink_obs_ts
  ON airlink_obs(ts);
"""

def main():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        conn.executescript(DDL)
        conn.commit()
    print(f"OK: migrated DB at {DB_PATH}")

if __name__ == "__main__":
    main()
