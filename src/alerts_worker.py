import os
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from src.alerting import (
    build_freeze_alert_message,
    determine_freeze_alerts,
    load_alert_state,
    resolve_alert_recipients,
    save_alert_state,
    send_email,
    send_verizon_sms,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
LOG_PATH = PROJECT_ROOT / "logs" / "alerts_worker.log"

LOCAL_TZ = os.getenv("LOCAL_TZ", "America/New_York")
ALERT_WORKER_INTERVAL_SECONDS = int(os.getenv("ALERT_WORKER_INTERVAL_SECONDS", "60"))


def resolve_db_path() -> Path:
    raw_path = os.getenv("TEMPEST_DB_PATH")
    if raw_path:
        path = Path(raw_path)
        return path if path.is_absolute() else PROJECT_ROOT / path
    return PROJECT_ROOT / "data" / "tempest.db"


def log(message: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"{ts} | {message}"
    print(line, flush=True)
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with LOG_PATH.open("a", encoding="utf-8") as file:
        file.write(line + "\n")


def table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (name,),
    ).fetchone()
    return row is not None


def latest_temp_c(conn: sqlite3.Connection) -> tuple[int | None, float | None]:
    row = conn.execute(
        "SELECT obs_epoch, air_temperature FROM obs_st ORDER BY obs_epoch DESC LIMIT 1"
    ).fetchone()
    if not row:
        return None, None
    epoch, temp_c = row
    if epoch is None or temp_c is None:
        return None, None
    return int(epoch), float(temp_c)


def c_to_f(temp_c: float) -> float:
    return (temp_c * 9 / 5) + 32


def run_once(db_path: Path) -> None:
    if not db_path.exists():
        log(f"ERROR: DB missing at {db_path}.")
        return
    with sqlite3.connect(db_path) as conn:
        if not table_exists(conn, "obs_st"):
            log("ERROR: obs_st table missing, alerts cannot run.")
            return
        obs_epoch, temp_c = latest_temp_c(conn)
    if obs_epoch is None or temp_c is None:
        log("WARN: No recent Tempest data found.")
        return

    temp_f = c_to_f(temp_c)
    now_local = datetime.now(timezone.utc).astimezone(ZoneInfo(LOCAL_TZ))

    alert_state = load_alert_state(str(db_path))
    alerts_to_send, reset_updates = determine_freeze_alerts(temp_f, alert_state)
    if reset_updates:
        save_alert_state(str(db_path), reset_updates)

    if not alerts_to_send:
        log(f"OK: Tempest {temp_f:.1f} F - no alerts.")
        return

    email_to, sms_to = resolve_alert_recipients(str(db_path))
    if not email_to and not sms_to:
        log("WARN: Alert triggered but no recipients configured.")
        return

    for alert in alerts_to_send:
        temp_value = float(temp_f)
        message_body = build_freeze_alert_message(alert["title"], temp_value, now_local)
        subject = f"{alert['title']} - Tempest {temp_value:.1f} F"
        email_sent = False
        sms_sent = False
        if email_to:
            email_sent, email_error = send_email(
                subject,
                message_body,
                to_address=email_to,
                return_error=True,
            )
            if not email_sent:
                log(f"WARN: Email send failed ({email_error}).")
        if sms_to:
            sms_sent, sms_error = send_verizon_sms(
                message_body,
                sms_number=sms_to,
                return_error=True,
            )
            if not sms_sent:
                log(f"WARN: SMS send failed ({sms_error}).")
        if email_sent or sms_sent:
            save_alert_state(str(db_path), alert["state_updates"])
            log(f"ALERT: {alert['title']} sent.")
        else:
            log(f"WARN: {alert['title']} not sent.")


def main() -> int:
    db_path = resolve_db_path()
    run_once_flag = "--once" in sys.argv
    if run_once_flag:
        run_once(db_path)
        return 0
    log(f"Starting alerts worker (interval={ALERT_WORKER_INTERVAL_SECONDS}s).")
    while True:
        try:
            run_once(db_path)
        except Exception as exc:
            log(f"ERROR: worker exception ({exc}).")
        time.sleep(max(5, ALERT_WORKER_INTERVAL_SECONDS))


if __name__ == "__main__":
    sys.exit(main())
