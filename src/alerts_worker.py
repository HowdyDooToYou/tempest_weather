import os
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import requests

from src.alerting import (
    build_freeze_alert_message,
    determine_freeze_alerts,
    load_alert_state,
    resolve_alert_recipients,
    save_alert_state,
    send_email,
    send_verizon_sms,
)
from src.config_store import connect as config_connect
from src.config_store import get_bool, get_float
from src.nws_alerts import fetch_active_alerts, fetch_hwo_text, summarize_alerts, summarize_hwo

PROJECT_ROOT = Path(__file__).resolve().parents[1]
LOG_PATH = PROJECT_ROOT / "logs" / "alerts_worker.log"

LOCAL_TZ = os.getenv("LOCAL_TZ", "America/New_York")
ALERT_WORKER_INTERVAL_SECONDS = int(os.getenv("ALERT_WORKER_INTERVAL_SECONDS", "60"))
NWS_ALERTS_ENABLED = os.getenv("NWS_ALERTS_ENABLED", "1").lower() in ("1", "true", "yes", "on")
NWS_HWO_NOTIFY = os.getenv("NWS_HWO_NOTIFY", "0").lower() in ("1", "true", "yes", "on")
TEMPEST_API_TOKEN = os.getenv("TEMPEST_API_TOKEN")
TEMPEST_STATION_ID = int(os.getenv("TEMPEST_STATION_ID", "475329"))


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


def ensure_nws_alert_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS nws_alert_log (
            alert_id TEXT PRIMARY KEY,
            sent_at INTEGER NOT NULL
        )
        """
    )
    conn.commit()


def load_sent_nws_alert_ids(conn: sqlite3.Connection) -> set[str]:
    ensure_nws_alert_table(conn)
    rows = conn.execute("SELECT alert_id FROM nws_alert_log").fetchall()
    return {row[0] for row in rows if row and row[0]}


def record_nws_alerts(conn: sqlite3.Connection, alert_ids: list[str]) -> None:
    ensure_nws_alert_table(conn)
    now_epoch = int(datetime.now(timezone.utc).timestamp())
    for alert_id in alert_ids:
        if alert_id:
            conn.execute(
                "INSERT OR REPLACE INTO nws_alert_log (alert_id, sent_at) VALUES (?, ?)",
                (alert_id, now_epoch),
            )
    conn.commit()


def ensure_nws_hwo_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS nws_hwo_log (
            product_id TEXT PRIMARY KEY,
            sent_at INTEGER NOT NULL
        )
        """
    )
    conn.commit()


def load_sent_nws_hwo_ids(conn: sqlite3.Connection) -> set[str]:
    ensure_nws_hwo_table(conn)
    rows = conn.execute("SELECT product_id FROM nws_hwo_log").fetchall()
    return {row[0] for row in rows if row and row[0]}


def record_nws_hwo(conn: sqlite3.Connection, product_id: str) -> None:
    ensure_nws_hwo_table(conn)
    now_epoch = int(datetime.now(timezone.utc).timestamp())
    conn.execute(
        "INSERT OR REPLACE INTO nws_hwo_log (product_id, sent_at) VALUES (?, ?)",
        (product_id, now_epoch),
    )
    conn.commit()


def fetch_station_location(token: str | None, station_id: int):
    if not token:
        return None
    try:
        resp = requests.get(
            "https://swd.weatherflow.com/swd/rest/stations",
            params={"token": token},
            timeout=8,
        )
        resp.raise_for_status()
        payload = resp.json()
        stations = payload.get("stations", []) if isinstance(payload, dict) else []
        for station in stations:
            if station.get("station_id") == station_id:
                lat = station.get("latitude") or station.get("station_latitude")
                lon = station.get("longitude") or station.get("station_longitude") or station.get("lng")
                return {"name": station.get("name") or "Tempest Station", "lat": lat, "lon": lon}
        if stations:
            station = stations[0]
            lat = station.get("latitude") or station.get("station_latitude")
            lon = station.get("longitude") or station.get("station_longitude") or station.get("lng")
            return {"name": station.get("name") or "Tempest Station", "lat": lat, "lon": lon}
    except Exception:
        return None
    return None


def resolve_location(db_path: Path) -> tuple[float | None, float | None]:
    try:
        with config_connect(db_path) as conn:
            override_enabled = bool(get_bool(conn, "override_location_enabled") or False)
            lat_override = get_float(conn, "station_lat_override")
            lon_override = get_float(conn, "station_lon_override")
            if override_enabled and lat_override is not None and lon_override is not None:
                return lat_override, lon_override
    except Exception:
        pass
    try:
        with config_connect(db_path) as conn:
            lat_override = get_float(conn, "station_lat_override")
            lon_override = get_float(conn, "station_lon_override")
            if lat_override is not None and lon_override is not None:
                return lat_override, lon_override
    except Exception:
        pass
    station = fetch_station_location(TEMPEST_API_TOKEN, TEMPEST_STATION_ID)
    if station and station.get("lat") is not None and station.get("lon") is not None:
        return float(station["lat"]), float(station["lon"])
    return None, None


def run_once(db_path: Path) -> None:
    if not db_path.exists():
        log(f"ERROR: DB missing at {db_path}.")
        return
    with sqlite3.connect(db_path) as conn:
        if not table_exists(conn, "obs_st"):
            log("WARN: obs_st table missing, freeze alerts cannot run.")
            obs_epoch, temp_c = None, None
        else:
            obs_epoch, temp_c = latest_temp_c(conn)
    now_local = datetime.now(timezone.utc).astimezone(ZoneInfo(LOCAL_TZ))
    now_epoch = int(now_local.timestamp())

    if obs_epoch is None or temp_c is None:
        log("WARN: No recent Tempest data found for freeze alerts.")
    else:
        temp_f = c_to_f(temp_c)
        alert_state = load_alert_state(str(db_path))
        alerts_to_send, reset_updates = determine_freeze_alerts(temp_f, alert_state, now_epoch=now_epoch)
        if reset_updates:
            save_alert_state(str(db_path), reset_updates)

        if alerts_to_send:
            email_to, sms_to = resolve_alert_recipients(str(db_path))
            if not email_to and not sms_to:
                log("WARN: Freeze alert triggered but no recipients configured.")
            else:
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
        else:
            log(f"OK: Tempest {temp_f:.1f} F - no freeze alerts.")

    if NWS_ALERTS_ENABLED or NWS_HWO_NOTIFY:
        lat, lon = resolve_location(db_path)
        if lat is None or lon is None:
            log("WARN: NWS checks skipped (no location).")
        else:
            if NWS_ALERTS_ENABLED:
                alerts = fetch_active_alerts(lat, lon, LOCAL_TZ)
                if not alerts:
                    log("OK: No active NWS alerts.")
                else:
                    with sqlite3.connect(db_path) as conn:
                        sent_ids = load_sent_nws_alert_ids(conn)
                    new_alerts = [alert for alert in alerts if alert.get("id") not in sent_ids]
                    if not new_alerts:
                        log("OK: No new NWS alerts to send.")
                    else:
                        email_to, sms_to = resolve_alert_recipients(str(db_path))
                        if not email_to and not sms_to:
                            log("WARN: NWS alerts found but no recipients configured.")
                        else:
                            summary_lines = summarize_alerts(new_alerts, LOCAL_TZ, max_items=4)
                            message_body = "NWS Alerts:\n" + "\n".join(f"- {line}" for line in summary_lines)
                            subject = f"NWS Alerts ({len(new_alerts)})"
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
                                    log(f"WARN: NWS email send failed ({email_error}).")
                            if sms_to and summary_lines:
                                sms_body = "NWS Alerts: " + " ".join(summary_lines[:2])
                                sms_sent, sms_error = send_verizon_sms(
                                    sms_body,
                                    sms_number=sms_to,
                                    return_error=True,
                                )
                                if not sms_sent:
                                    log(f"WARN: NWS SMS send failed ({sms_error}).")
                            if email_sent or sms_sent:
                                with sqlite3.connect(db_path) as conn:
                                    record_nws_alerts(conn, [a.get("id") for a in new_alerts if a.get("id")])
                                log(f"ALERT: Sent {len(new_alerts)} NWS alert(s).")
                            else:
                                log("WARN: NWS alerts not sent.")
            if NWS_HWO_NOTIFY:
                hwo = fetch_hwo_text(lat, lon)
                if not hwo:
                    log("OK: No NWS outlook available.")
                else:
                    product_id = hwo.get("id")
                    if not product_id:
                        log("WARN: NWS outlook missing product id.")
                    else:
                        with sqlite3.connect(db_path) as conn:
                            sent_ids = load_sent_nws_hwo_ids(conn)
                        if product_id in sent_ids:
                            log("OK: No new NWS outlook to send.")
                        else:
                            headline = hwo.get("headline") or "Hazardous Weather Outlook"
                            summary = summarize_hwo(hwo, max_chars=320) or "New outlook issued."
                            email_to, sms_to = resolve_alert_recipients(str(db_path))
                            if not email_to and not sms_to:
                                log("WARN: NWS outlook found but no recipients configured.")
                            else:
                                subject = f"NWS Outlook: {headline}"
                                message_body = f"{headline}\n\n{summary}"
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
                                        log(f"WARN: NWS outlook email failed ({email_error}).")
                                if sms_to:
                                    sms_body = f"NWS Outlook: {headline}. {summary}"
                                    sms_body = sms_body[:240]
                                    sms_sent, sms_error = send_verizon_sms(
                                        sms_body,
                                        sms_number=sms_to,
                                        return_error=True,
                                    )
                                    if not sms_sent:
                                        log(f"WARN: NWS outlook SMS failed ({sms_error}).")
                                if email_sent or sms_sent:
                                    with sqlite3.connect(db_path) as conn:
                                        record_nws_hwo(conn, product_id)
                                    log("ALERT: Sent NWS outlook.")
                                else:
                                    log("WARN: NWS outlook not sent.")


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
