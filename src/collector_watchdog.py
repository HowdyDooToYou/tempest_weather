import os
import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = PROJECT_ROOT / "data" / "tempest.db"
LOG_PATH = PROJECT_ROOT / "logs" / "collector_watchdog.log"

HEARTBEAT_TABLE = "collector_heartbeat"

STALE_TEMPEST_HEARTBEAT_SEC = int(os.getenv("WATCHDOG_TEMPEST_HEARTBEAT_SEC", "300"))
STALE_AIRLINK_HEARTBEAT_SEC = int(os.getenv("WATCHDOG_AIRLINK_HEARTBEAT_SEC", "180"))
STALE_TEMPEST_DATA_SEC = int(os.getenv("WATCHDOG_TEMPEST_DATA_SEC", "900"))
STALE_AIRLINK_DATA_SEC = int(os.getenv("WATCHDOG_AIRLINK_DATA_SEC", "300"))


def log(msg: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"{ts} | {msg}"
    print(line, flush=True)
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with LOG_PATH.open("a", encoding="utf-8") as f:
        f.write(line + "\n")


def table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (name,),
    ).fetchone()
    return row is not None


def resolve_table(conn: sqlite3.Connection, candidates):
    for name in candidates:
        if table_exists(conn, name):
            return name
    return None


def format_age(age_seconds: int | None) -> str:
    if age_seconds is None:
        return "--"
    if age_seconds < 60:
        return f"{age_seconds}s"
    if age_seconds < 3600:
        return f"{age_seconds/60:.1f}m"
    return f"{age_seconds/3600:.1f}h"


def age_seconds(epoch: int | None) -> int | None:
    if epoch is None:
        return None
    return max(0, int(time.time()) - int(epoch))


def fetch_heartbeat(conn: sqlite3.Connection, name: str) -> dict | None:
    row = conn.execute(
        f"""
        SELECT last_ok_epoch, last_error_epoch, last_ok_message, last_error
        FROM {HEARTBEAT_TABLE}
        WHERE name = ?
        """,
        (name,),
    ).fetchone()
    if not row:
        return None
    return {
        "last_ok_epoch": row[0],
        "last_error_epoch": row[1],
        "last_ok_message": row[2],
        "last_error": row[3],
    }


def latest_epoch(conn: sqlite3.Connection, table: str, col: str) -> int | None:
    row = conn.execute(f"SELECT MAX({col}) FROM {table}").fetchone()
    if not row or row[0] is None:
        return None
    try:
        return int(row[0])
    except Exception:
        return None


def check_heartbeat(conn: sqlite3.Connection, name: str, label: str, stale_sec: int) -> tuple[bool, str]:
    hb = fetch_heartbeat(conn, name)
    if not hb:
        return False, f"{label}: missing heartbeat"
    ok_age = age_seconds(hb["last_ok_epoch"])
    if ok_age is None:
        return False, f"{label}: last_ok missing"
    if ok_age > stale_sec:
        return False, f"{label}: stale heartbeat ({format_age(ok_age)})"
    return True, f"{label}: ok ({format_age(ok_age)} ago)"


def check_data(conn: sqlite3.Connection, table: str | None, col: str, label: str, stale_sec: int) -> tuple[bool, str]:
    if not table:
        return False, f"{label}: table missing"
    last_epoch = latest_epoch(conn, table, col)
    data_age = age_seconds(last_epoch)
    if data_age is None:
        return False, f"{label}: no data"
    if data_age > stale_sec:
        return False, f"{label}: stale data ({format_age(data_age)})"
    return True, f"{label}: ok ({format_age(data_age)} ago)"


def main() -> int:
    if not DB_PATH.exists():
        log("ERROR: DB missing, run collectors first.")
        return 1

    with sqlite3.connect(DB_PATH) as conn:
        if not table_exists(conn, HEARTBEAT_TABLE):
            log("ERROR: collector_heartbeat table missing, update collectors first.")
            return 1

        ok = True
        results = []

        hb_ok, hb_msg = check_heartbeat(conn, "tempest_collector", "Tempest Collector", STALE_TEMPEST_HEARTBEAT_SEC)
        results.append(hb_msg)
        ok = ok and hb_ok

        hb_ok, hb_msg = check_heartbeat(conn, "airlink_collector", "AirLink Collector", STALE_AIRLINK_HEARTBEAT_SEC)
        results.append(hb_msg)
        ok = ok and hb_ok

        airlink_table = resolve_table(conn, ["airlink_current_obs", "airlink_obs"])
        data_ok, data_msg = check_data(conn, airlink_table, "ts", "AirLink Data", STALE_AIRLINK_DATA_SEC)
        results.append(data_msg)
        ok = ok and data_ok

        data_ok, data_msg = check_data(conn, "obs_st", "obs_epoch", "Tempest Data", STALE_TEMPEST_DATA_SEC)
        results.append(data_msg)
        ok = ok and data_ok

    summary = " | ".join(results)
    if ok:
        log(f"OK: {summary}")
        return 0
    log(f"WARN: {summary}")
    return 2


if __name__ == "__main__":
    sys.exit(main())
