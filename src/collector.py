import json
import os
import sqlite3
import time
import traceback
import hashlib
from pathlib import Path

import websocket
from websocket._exceptions import WebSocketTimeoutException

# =====================
# Configuration
# =====================
# Subscribe to BOTH: Tempest station (ST) + Hub (HB)
DEVICE_IDS = [475329, 475327]

SOCKET_TIMEOUT_SEC = 30

# Reconnect backoff (exponential)
RECONNECT_BASE_SEC = 5
RECONNECT_MAX_SEC = 300  # 5 minutes cap

# Commit batching
COMMIT_EVERY_N_MESSAGES = 25
COMMIT_EVERY_SECONDS = 5

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = PROJECT_ROOT / "data" / "tempest.db"
LOG_PATH = PROJECT_ROOT / "logs" / "collector.log"

TOKEN = os.getenv("TEMPEST_API_TOKEN")
if not TOKEN:
    raise RuntimeError("TEMPEST_API_TOKEN is not set")

WS_URL = f"wss://ws.weatherflow.com/swd/data?token={TOKEN}"

# =====================
# Logging
# =====================
def log(msg: str) -> None:
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    line = f"{ts} | {msg}"
    print(line)
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with LOG_PATH.open("a", encoding="utf-8") as f:
        f.write(line + "\n")

# =====================
# Database schema + migrations
# =====================
BASE_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS raw_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  received_at_epoch INTEGER NOT NULL,
  device_id INTEGER,
  message_type TEXT,
  payload_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_raw_events_received_at
  ON raw_events(received_at_epoch);

CREATE INDEX IF NOT EXISTS idx_raw_events_device_type
  ON raw_events(device_id, message_type);

CREATE TABLE IF NOT EXISTS obs_st (
  obs_epoch INTEGER NOT NULL,
  device_id INTEGER NOT NULL,

  wind_lull REAL,
  wind_avg REAL,
  wind_gust REAL,
  wind_dir INTEGER,

  wind_interval INTEGER,
  station_pressure REAL,
  air_temperature REAL,
  relative_humidity INTEGER,
  illuminance REAL,
  uv REAL,
  solar_radiation REAL,
  rain_accumulated REAL,
  precip_type INTEGER,
  lightning_avg_dist REAL,
  lightning_strike_count INTEGER,
  battery REAL,
  report_interval INTEGER,

  obs_raw_json TEXT,

  PRIMARY KEY (obs_epoch, device_id)
);

CREATE INDEX IF NOT EXISTS idx_obs_st_epoch
  ON obs_st(obs_epoch);
"""

def db_connect() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)

    # Durability + crash safety
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")

    # Base schema (for first run)
    conn.executescript(BASE_SCHEMA_SQL)

    # Automatic migrations (bulletproof raw log)
    migrate(conn)

    return conn

def column_exists(conn: sqlite3.Connection, table: str, column: str) -> bool:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return any(r[1] == column for r in rows)

def migrate(conn: sqlite3.Connection) -> None:
    # Step 1: raw text preservation
    if not column_exists(conn, "raw_events", "payload_text"):
        conn.execute("ALTER TABLE raw_events ADD COLUMN payload_text TEXT;")
        log("Migration: added raw_events.payload_text")

    # Step 2: payload hash (idempotency / dedupe)
    if not column_exists(conn, "raw_events", "payload_hash"):
        conn.execute("ALTER TABLE raw_events ADD COLUMN payload_hash TEXT;")
        log("Migration: added raw_events.payload_hash")

    # Make payload_json nullable in practice:
    # Old schema had NOT NULL; we keep writing it when available.
    # For malformed messages we store payload_text + hash and set payload_json to '{}'
    # to satisfy old constraint. (No destructive migration.)

    # Allow duplicate payloads with different epochs; keep a non-unique index for lookup.
    conn.execute("DROP INDEX IF EXISTS idx_raw_events_payload_hash;")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_raw_events_payload_hash "
        "ON raw_events(payload_hash);"
    )
    conn.commit()

# =====================
# Inserts
# =====================
def payload_fingerprint(payload_text: str) -> str:
    return hashlib.sha256(payload_text.encode("utf-8", errors="replace")).hexdigest()

def insert_raw_lossless(
    conn: sqlite3.Connection,
    received_at: int,
    device_id,
    msg_type: str,
    payload_text: str,
    payload_json: str
) -> None:
    phash = payload_fingerprint(payload_text)

    conn.execute(
        """
        INSERT INTO raw_events(
          received_at_epoch, device_id, message_type, payload_json, payload_text, payload_hash
        ) VALUES (?,?,?,?,?,?)
        """,
        (received_at, device_id, msg_type, payload_json, payload_text, phash)
    )

def insert_obs_st(conn: sqlite3.Connection, device_id: int, obs_row: list) -> None:
    if not obs_row or len(obs_row) < 18:
        return

    conn.execute(
        """
        INSERT OR IGNORE INTO obs_st (
          obs_epoch, device_id,
          wind_lull, wind_avg, wind_gust, wind_dir,
          wind_interval, station_pressure, air_temperature,
          relative_humidity, illuminance, uv, solar_radiation,
          rain_accumulated, precip_type,
          lightning_avg_dist, lightning_strike_count,
          battery, report_interval, obs_raw_json
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            int(obs_row[0]), int(device_id),
            obs_row[1], obs_row[2], obs_row[3], obs_row[4],
            obs_row[5], obs_row[6], obs_row[7],
            obs_row[8], obs_row[9], obs_row[10], obs_row[11],
            obs_row[12], obs_row[13],
            obs_row[14], obs_row[15],
            obs_row[16], obs_row[17],
            json.dumps(obs_row, separators=(",", ":"))
        )
    )

# =====================
# Startup sanity checks
# =====================
def startup_report(conn: sqlite3.Connection) -> None:
    log(f"DB ready at: {DB_PATH}")
    log(f"Listening device_ids={DEVICE_IDS}")
    log(f"WS_URL host=ws.weatherflow.com (token present={bool(TOKEN)})")

    # last obs seen (helps confirm continuity after restarts)
    try:
        row = conn.execute(
            "SELECT obs_epoch, device_id FROM obs_st ORDER BY obs_epoch DESC LIMIT 1"
        ).fetchone()
        if row:
            log(f"Last obs_st in DB: obs_epoch={row[0]} device_id={row[1]}")
        else:
            log("Last obs_st in DB: none yet")
    except Exception as e:
        log(f"Startup check (last obs) failed: {repr(e)}")

# =====================
# Collector loop
# =====================
def connect_and_listen(ws: websocket.WebSocket) -> None:
    ws.connect(WS_URL, timeout=SOCKET_TIMEOUT_SEC)
    ws.settimeout(SOCKET_TIMEOUT_SEC)

    for did in DEVICE_IDS:
        ws.send(json.dumps({"type": "listen_start", "device_id": did, "id": f"collector_{did}"}))

    log("WebSocket connected; listen_start sent for all devices")

def run():
    conn = db_connect()
    startup_report(conn)

    reconnect_delay = RECONNECT_BASE_SEC

    pending = 0
    last_commit = time.time()

    while True:
        ws = None
        try:
            ws = websocket.WebSocket()
            connect_and_listen(ws)

            # reset backoff after successful connect
            reconnect_delay = RECONNECT_BASE_SEC

            while True:
                try:
                    payload_text = ws.recv()  # may timeout
                    received_at = int(time.time())

                    # Lossless raw capture: store raw text always
                    # Parse if possible, but never drop the message if parsing fails
                    device_id = None
                    msg_type = None
                    payload_json_str = "{}"

                    try:
                        data = json.loads(payload_text)
                        msg_type = data.get("type")
                        device_id = data.get("device_id")
                        payload_json_str = json.dumps(data, separators=(",", ":"))

                        # Parse obs_st to structured table (optional cache)
                        if msg_type == "obs_st" and "obs" in data and data["obs"]:
                            insert_obs_st(conn, device_id, data["obs"][0])
                            log(f"Stored obs_st at obs_epoch={data['obs'][0][0]} (device_id={device_id})")

                    except Exception:
                        # Keep msg_type/device_id as None; payload_json_str stays "{}"
                        log("Warning: JSON parse failed for a message; stored losslessly as text")

                    insert_raw_lossless(conn, received_at, device_id, msg_type, payload_text, payload_json_str)
                    pending += 1

                    now = time.time()
                    if pending >= COMMIT_EVERY_N_MESSAGES or (now - last_commit) >= COMMIT_EVERY_SECONDS:
                        conn.commit()
                        pending = 0
                        last_commit = now

                except WebSocketTimeoutException:
                    # Normal: no message yet, keep looping
                    # Also gives us a chance to flush pending commits on quiet links.
                    now = time.time()
                    if pending > 0 and (now - last_commit) >= COMMIT_EVERY_SECONDS:
                        conn.commit()
                        pending = 0
                        last_commit = now
                    continue

        except KeyboardInterrupt:
            log("Shutdown requested (KeyboardInterrupt). Flushing and exiting.")
            try:
                conn.commit()
            except Exception:
                pass
            break

        except Exception as e:
            log(f"Connection error: {repr(e)}")
            traceback.print_exc()

            # flush any pending work
            try:
                conn.commit()
                pending = 0
                last_commit = time.time()
            except Exception:
                pass

            # exponential backoff
            log(f"Reconnecting in {reconnect_delay}s")
            time.sleep(reconnect_delay)
            reconnect_delay = min(RECONNECT_MAX_SEC, reconnect_delay * 2)

        finally:
            try:
                if ws is not None:
                    ws.close()
            except Exception:
                pass

if __name__ == "__main__":
    run()
