import sqlite3
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = PROJECT_ROOT / "data" / "tempest.db"

def main():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")

        conn.execute("""
        CREATE TABLE IF NOT EXISTS davis_raw_events (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          received_at_epoch INTEGER NOT NULL,
          source TEXT NOT NULL,
          device_ip TEXT,
          device_did TEXT,
          lsid INTEGER,
          message_type TEXT NOT NULL,
          payload_text TEXT NOT NULL,
          payload_hash TEXT NOT NULL UNIQUE
        );
        """)

        conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_davis_raw_received
          ON davis_raw_events(received_at_epoch);
        """)

        conn.execute("""
        CREATE TABLE IF NOT EXISTS airlink_obs (
          obs_epoch INTEGER NOT NULL,
          device_ip TEXT NOT NULL,
          device_did TEXT,
          lsid INTEGER,

          temp_f REAL,
          humidity_pct REAL,
          heat_index_f REAL,
          dew_point_f REAL,
          wet_bulb_f REAL,

          pm_1 REAL,
          pm_2p5 REAL,
          pm_10 REAL,
          pm_2p5_last_1_hour REAL,
          pm_2p5_last_3_hours REAL,
          pm_2p5_last_24_hours REAL,

          PRIMARY KEY (obs_epoch, device_ip)
        );
        """)

        conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_airlink_obs_epoch
          ON airlink_obs(obs_epoch);
        """)

        conn.commit()

    print(f"OK: migrated DB at {DB_PATH}")

if __name__ == "__main__":
    main()
