import os
import time
import json
import hashlib
import sqlite3
import traceback
from pathlib import Path
from datetime import datetime

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = PROJECT_ROOT / "data" / "tempest.db"
LOG_DIR = PROJECT_ROOT / "logs"
LOG_PATH = LOG_DIR / "davis_collector.log"

POLL_SEC = int(os.getenv("DAVIS_POLL_SEC", "15"))
HTTP_TIMEOUT = int(os.getenv("DAVIS_HTTP_TIMEOUT", "8"))
RETRY_SEC = int(os.getenv("DAVIS_RETRY_SEC", "5"))

AIRLINK_HOST = os.getenv("DAVIS_AIRLINK_HOST", "").rstrip("/")
ENDPOINT_PATH = "/v1/current_conditions"  # confirmed working on your AirLink

def log(msg: str):
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"{ts} | {msg}"
    print(line, flush=True)
    with open(LOG_PATH, "a", encoding="utf-8") as f:
        f.write(line + "\n")

def sha256_text(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def db_connect():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn

def insert_raw(conn, received_at_epoch: int, device_ip: str, device_did: str | None,
               lsid: int | None, payload_text: str):
    h = sha256_text(payload_text)
    try:
        conn.execute(
            """
            INSERT INTO davis_raw_events
              (received_at_epoch, source, device_ip, device_did, lsid, message_type, payload_text, payload_hash)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (received_at_epoch, "airlink_local", device_ip, device_did, lsid,
             "airlink_current_conditions", payload_text, h),
        )
    except sqlite3.IntegrityError:
        pass  # already stored

def upsert_obs(conn, obs_epoch: int, device_ip: str, device_did: str | None, lsid: int | None,
               temp_f=None, humidity_pct=None, heat_index_f=None, dew_point_f=None, wet_bulb_f=None,
               pm_1=None, pm_2p5=None, pm_10=None,
               pm_2p5_last_1_hour=None, pm_2p5_last_3_hours=None, pm_2p5_last_24_hours=None):
    conn.execute(
        """
        INSERT OR REPLACE INTO airlink_obs
          (obs_epoch, device_ip, device_did, lsid,
           temp_f, humidity_pct, heat_index_f, dew_point_f, wet_bulb_f,
           pm_1, pm_2p5, pm_10,
           pm_2p5_last_1_hour, pm_2p5_last_3_hours, pm_2p5_last_24_hours)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (obs_epoch, device_ip, device_did, lsid,
         temp_f, humidity_pct, heat_index_f, dew_point_f, wet_bulb_f,
         pm_1, pm_2p5, pm_10,
         pm_2p5_last_1_hour, pm_2p5_last_3_hours, pm_2p5_last_24_hours),
    )

def to_float(x):
    try:
        return None if x is None else float(x)
    except Exception:
        return None

def to_int(x):
    try:
        return None if x is None else int(float(x))
    except Exception:
        return None

def run():
    if not AIRLINK_HOST:
        log("ERROR: DAVIS_AIRLINK_HOST not set (e.g. http://192.168.1.19). Exiting.")
        return

    device_ip = AIRLINK_HOST.replace("http://", "").replace("https://", "").split(":")[0]
    url = AIRLINK_HOST + ENDPOINT_PATH

    log(f"DB ready at: {DB_PATH}")
    log(f"Polling AirLink URL={url} every {POLL_SEC}s")

    session = requests.Session()

    while True:
        try:
            r = session.get(url, timeout=HTTP_TIMEOUT)
            r.raise_for_status()
            payload = r.json()

            received_at = int(time.time())
            payload_text = json.dumps(payload, separators=(",", ":"), ensure_ascii=False, sort_keys=True)

            data = payload.get("data") or {}
            did = data.get("did")
            obs_epoch = to_int(data.get("ts")) or received_at

            conditions = data.get("conditions") or []
            c0 = conditions[0] if isinstance(conditions, list) and len(conditions) else {}
            lsid = to_int(c0.get("lsid"))

            temp_f = to_float(c0.get("temp"))
            hum = to_float(c0.get("hum"))
            heat_index_f = to_float(c0.get("heat_index"))
            dew_point_f = to_float(c0.get("dew_point"))
            wet_bulb_f = to_float(c0.get("wet_bulb"))

            pm_1 = to_float(c0.get("pm_1"))
            pm_2p5 = to_float(c0.get("pm_2p5"))
            pm_10 = to_float(c0.get("pm_10"))

            pm_2p5_last_1_hour = to_float(c0.get("pm_2p5_last_1_hour"))
            pm_2p5_last_3_hours = to_float(c0.get("pm_2p5_last_3_hours"))
            pm_2p5_last_24_hours = to_float(c0.get("pm_2p5_last_24_hours"))

            with db_connect() as conn:
                insert_raw(conn, received_at, device_ip, did, lsid, payload_text)
                upsert_obs(
                    conn,
                    obs_epoch, device_ip, did, lsid,
                    temp_f=temp_f, humidity_pct=hum, heat_index_f=heat_index_f,
                    dew_point_f=dew_point_f, wet_bulb_f=wet_bulb_f,
                    pm_1=pm_1, pm_2p5=pm_2p5, pm_10=pm_10,
                    pm_2p5_last_1_hour=pm_2p5_last_1_hour,
                    pm_2p5_last_3_hours=pm_2p5_last_3_hours,
                    pm_2p5_last_24_hours=pm_2p5_last_24_hours,
                )
                conn.commit()

            log(f"Stored airlink_obs obs_epoch={obs_epoch} did={did} pm2.5={pm_2p5} temp_f={temp_f} hum={hum}")
            time.sleep(POLL_SEC)

        except Exception as e:
            log(f"ERROR: {repr(e)}")
            traceback.print_exc()
            time.sleep(RETRY_SEC)

if __name__ == "__main__":
    run()
