import json
import os
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pandas as pd
import requests

from src.alerting import send_email
from src.config_store import connect as config_connect
from src.config_store import get_bool, get_float
from src.nws_alerts import fetch_active_alerts, fetch_hwo_text, summarize_alerts, summarize_hwo

DB_PATH = os.getenv("TEMPEST_DB_PATH", "data/tempest.db")
LOCAL_TZ = os.getenv("LOCAL_TZ", "America/New_York")
EMAIL_HOUR = int(os.getenv("DAILY_EMAIL_HOUR", "7"))
EMAIL_MINUTE = int(os.getenv("DAILY_EMAIL_MINUTE", "0"))
EMAIL_TO = os.getenv("DAILY_EMAIL_TO", "john.kipe+weatheralert@gmail.com")
DAILY_EMAIL_LAT = os.getenv("DAILY_EMAIL_LAT")
DAILY_EMAIL_LON = os.getenv("DAILY_EMAIL_LON")
TEMPEST_API_TOKEN = os.getenv("TEMPEST_API_TOKEN")
TEMPEST_API_KEY = os.getenv("TEMPEST_API_KEY")
TEMPEST_STATION_ID = int(os.getenv("TEMPEST_STATION_ID", "475329"))


def _tzinfo() -> ZoneInfo:
    try:
        return ZoneInfo(LOCAL_TZ)
    except Exception:
        return ZoneInfo("UTC")


def ensure_email_log_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS daily_email_log (
            date TEXT PRIMARY KEY,
            sent_at TEXT,
            status TEXT,
            error TEXT
        )
        """
    )
    conn.commit()


def load_last_sent_date(conn: sqlite3.Connection) -> str | None:
    ensure_email_log_table(conn)
    row = conn.execute(
        "SELECT date FROM daily_email_log ORDER BY sent_at DESC LIMIT 1"
    ).fetchone()
    return row[0] if row else None


def record_send(conn: sqlite3.Connection, date_str: str, status: str, error: str | None = None) -> None:
    ensure_email_log_table(conn)
    conn.execute(
        """
        INSERT INTO daily_email_log (date, sent_at, status, error)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(date) DO UPDATE SET
          sent_at=excluded.sent_at,
          status=excluded.status,
          error=excluded.error
        """,
        (
            date_str,
            datetime.now(timezone.utc).isoformat(),
            status,
            error or "",
        ),
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


def resolve_location() -> tuple[float | None, float | None]:
    try:
        with config_connect(DB_PATH) as conn:
            override_enabled = bool(get_bool(conn, "override_location_enabled") or False)
            lat_override = get_float(conn, "station_lat_override")
            lon_override = get_float(conn, "station_lon_override")
            if override_enabled and lat_override is not None and lon_override is not None:
                return lat_override, lon_override
    except Exception:
        pass

    if DAILY_EMAIL_LAT and DAILY_EMAIL_LON:
        try:
            return float(DAILY_EMAIL_LAT), float(DAILY_EMAIL_LON)
        except (TypeError, ValueError):
            pass

    try:
        with config_connect(DB_PATH) as conn:
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


def fetch_current_conditions(conn: sqlite3.Connection):
    query = """
        SELECT obs_epoch, air_temperature, wind_avg, station_pressure, relative_humidity, rain_accumulated
        FROM obs_st
        ORDER BY obs_epoch DESC
        LIMIT 1
    """
    try:
        row = conn.execute(query).fetchone()
    except sqlite3.OperationalError:
        row = conn.execute(
            """
            SELECT obs_epoch, air_temperature, wind_avg, station_pressure
            FROM obs_st
            ORDER BY obs_epoch DESC
            LIMIT 1
            """
        ).fetchone()
    if not row:
        return None
    keys = ["obs_epoch", "air_temperature", "wind_avg", "station_pressure", "relative_humidity", "rain_accumulated"]
    data = dict(zip(keys, row + (None,) * (len(keys) - len(row))))
    return data


def fetch_aqi(conn: sqlite3.Connection):
    try:
        row = conn.execute(
            """
            SELECT ts, pm_2p5
            FROM airlink_current_obs
            ORDER BY ts DESC
            LIMIT 1
            """
        ).fetchone()
    except sqlite3.OperationalError:
        return None
    if not row:
        return None
    return {"ts": row[0], "pm_2p5": row[1]}


def fetch_openmeteo_forecast(lat: float, lon: float, tz_name: str):
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": [
            "temperature_2m",
            "precipitation",
            "windspeed_10m",
            "windgusts_10m",
            "weathercode",
        ],
        "temperature_unit": "fahrenheit",
        "windspeed_unit": "mph",
        "precipitation_unit": "inch",
        "timezone": tz_name,
        "forecast_days": 3,
    }
    resp = requests.get("https://api.open-meteo.com/v1/forecast", params=params, timeout=12)
    resp.raise_for_status()
    payload = resp.json()
    hourly = payload.get("hourly") or {}
    if not hourly or "time" not in hourly:
        return None
    df = pd.DataFrame(hourly)
    df["time"] = pd.to_datetime(df["time"])
    if df["time"].dt.tz is None:
        df["time"] = df["time"].dt.tz_localize(tz_name)
    else:
        df["time"] = df["time"].dt.tz_convert(tz_name)
    return df


def summarize_forecast(hourly_df: pd.DataFrame | None, now_local: datetime) -> tuple[str, list[str]]:
    if hourly_df is None or hourly_df.empty:
        return "Forecast unavailable.", []
    cutoff = now_local + timedelta(hours=48)
    window = hourly_df[(hourly_df["time"] >= now_local) & (hourly_df["time"] <= cutoff)].copy()
    if window.empty:
        return "Forecast unavailable.", []

    temp_min = window["temperature_2m"].min()
    temp_max = window["temperature_2m"].max()
    wind_max = window["windgusts_10m"].max() if "windgusts_10m" in window else window["windspeed_10m"].max()
    precip_total = window["precipitation"].sum()
    summary = (
        f"Next 48h: {temp_min:.1f}F to {temp_max:.1f}F, "
        f"max wind {wind_max:.1f} mph, precip {precip_total:.2f} in."
    )

    sample = window.head(6)
    snapshots = []
    for _, row in sample.iterrows():
        time_label = row["time"].strftime("%I %p").lstrip("0")
        snapshots.append(
            f"{time_label}: {row['temperature_2m']:.0f}F, wind {row['windspeed_10m']:.0f} mph"
        )
    return summary, snapshots


def load_daily_brief(conn: sqlite3.Connection, tz: ZoneInfo) -> dict | None:
    today = datetime.now(tz).date().isoformat()
    row = conn.execute(
        """
        SELECT headline, bullets_json, tomorrow_text, generated_at
        FROM daily_briefs
        WHERE date = ?
        """,
        (today,),
    ).fetchone()
    if not row:
        return None
    headline, bullets_json, tomorrow_text, generated_at = row
    bullets = []
    if bullets_json:
        try:
            bullets = json.loads(bullets_json)
            if not isinstance(bullets, list):
                bullets = []
        except Exception:
            bullets = []
    return {
        "headline": headline or "",
        "bullets": bullets,
        "tomorrow": tomorrow_text or "",
        "generated_at": generated_at or "",
    }


def generate_brief_if_missing(conn: sqlite3.Connection, tz: ZoneInfo) -> dict | None:
    brief = load_daily_brief(conn, tz)
    if brief:
        return brief
    if not os.getenv("OPENAI_API_KEY"):
        return None
    try:
        from src.daily_brief_worker import run_once
    except Exception:
        return None
    run_once()
    return load_daily_brief(conn, tz)


def build_email_body(conn: sqlite3.Connection) -> str:
    tz = _tzinfo()
    now_local = datetime.now(tz)
    now_text = now_local.strftime("%b %d %Y %I:%M %p").lstrip("0")

    current = fetch_current_conditions(conn)
    aqi = fetch_aqi(conn)
    lat, lon = resolve_location()
    forecast_df = None
    forecast_summary = "Forecast unavailable."
    forecast_snapshots: list[str] = []
    if lat is not None and lon is not None:
        try:
            tz_name = getattr(tz, "key", LOCAL_TZ)
            forecast_df = fetch_openmeteo_forecast(lat, lon, tz_name)
            forecast_summary, forecast_snapshots = summarize_forecast(forecast_df, now_local)
        except Exception:
            forecast_summary = "Forecast unavailable."

    brief = generate_brief_if_missing(conn, tz)

    lines = [f"Tempest Daily Brief - {now_text}", ""]
    lines.append("Current conditions")
    if current:
        temp_c = current.get("air_temperature")
        wind_ms = current.get("wind_avg")
        pressure_mb = current.get("station_pressure")
        temp_f = (temp_c * 9 / 5 + 32) if temp_c is not None else None
        wind_mph = (wind_ms * 2.23694) if wind_ms is not None else None
        pressure_inhg = (pressure_mb * 0.02953) if pressure_mb is not None else None
        humidity = current.get("relative_humidity")
        if temp_f is not None:
            lines.append(f"- Temp: {temp_f:.1f} F")
        if wind_mph is not None:
            lines.append(f"- Wind: {wind_mph:.1f} mph")
        if pressure_inhg is not None:
            lines.append(f"- Pressure: {pressure_inhg:.2f} inHg")
        if humidity is not None:
            lines.append(f"- Humidity: {humidity:.0f}%")
    else:
        lines.append("- No recent observations available.")

    if aqi and aqi.get("pm_2p5") is not None:
        lines.append(f"- PM2.5: {aqi['pm_2p5']:.0f}")

    lines.append("")
    lines.append("Daily brief (AI)")
    if brief:
        if brief.get("headline"):
            lines.append(f"- {brief['headline']}")
        for bullet in brief.get("bullets", [])[:5]:
            lines.append(f"  - {bullet}")
        if brief.get("tomorrow"):
            lines.append(f"  - Tomorrow: {brief['tomorrow']}")
    else:
        lines.append("- Brief not available yet.")

    lines.append("")
    lines.append("48-hour outlook")
    lines.append(f"- {forecast_summary}")
    if forecast_snapshots:
        lines.append("  - Next hours:")
        for snap in forecast_snapshots:
            lines.append(f"    - {snap}")

    if lat is not None and lon is not None:
        tz_name = tz.key if hasattr(tz, "key") else LOCAL_TZ
        alerts = fetch_active_alerts(lat, lon, tz_name)
        alert_lines = summarize_alerts(alerts, tz_name, max_items=3)
        hwo_summary = summarize_hwo(fetch_hwo_text(lat, lon))
        if alert_lines or hwo_summary:
            lines.append("")
            lines.append("NWS Outlooks & Alerts")
        if hwo_summary:
            lines.append(f"- Outlook: {hwo_summary}")
        if alert_lines:
            for line in alert_lines:
                lines.append(f"- {line}")

    return "\n".join(lines)


def send_daily_email() -> tuple[bool, str | None]:
    with sqlite3.connect(DB_PATH) as conn:
        body = build_email_body(conn)
    subject = "Tempest Morning Brief"
    success, error = send_email(subject, body, to_address=EMAIL_TO, return_error=True)
    return success, error


def next_run_time(now_local: datetime) -> datetime:
    target = now_local.replace(hour=EMAIL_HOUR, minute=EMAIL_MINUTE, second=0, microsecond=0)
    if now_local >= target:
        target = target + timedelta(days=1)
    return target


def main():
    tz = _tzinfo()
    while True:
        now_local = datetime.now(tz)
        today = now_local.date().isoformat()
        with sqlite3.connect(DB_PATH) as conn:
            last_sent = load_last_sent_date(conn)

        if last_sent == today:
            sleep_for = max(60, int((next_run_time(now_local) - now_local).total_seconds()))
            time.sleep(sleep_for)
            continue

        target_today = now_local.replace(hour=EMAIL_HOUR, minute=EMAIL_MINUTE, second=0, microsecond=0)
        if now_local < target_today:
            wait_seconds = (target_today - now_local).total_seconds()
            time.sleep(wait_seconds)

        try:
            ok, error = send_daily_email()
            status = "sent" if ok else "error"
            sent_date = datetime.now(tz).date().isoformat()
            with sqlite3.connect(DB_PATH) as conn:
                record_send(conn, sent_date, status, error)
        except Exception as exc:
            sent_date = datetime.now(tz).date().isoformat()
            with sqlite3.connect(DB_PATH) as conn:
                record_send(conn, sent_date, "error", str(exc))
        time.sleep(30)


if __name__ == "__main__":
    main()
