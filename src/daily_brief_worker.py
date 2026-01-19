import json
import os
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pandas as pd
import requests

from src.config_store import connect as config_connect
from src.config_store import get_bool, get_float
from src.nws_alerts import (
    fetch_active_alerts,
    fetch_afd_text,
    fetch_hwo_text,
    summarize_afd,
    summarize_alerts,
    summarize_hwo,
)

DB_PATH = os.getenv("TEMPEST_DB_PATH", "data/tempest.db")
LOCAL_TZ = os.getenv("LOCAL_TZ", "America/New_York")
INTERVAL_MINUTES = int(os.getenv("DAILY_BRIEF_INTERVAL_MINUTES", "180"))
OPENAI_MODEL = os.getenv("DAILY_BRIEF_MODEL", "gpt-4o-mini")
DAILY_BRIEF_LAT = os.getenv("DAILY_BRIEF_LAT")
DAILY_BRIEF_LON = os.getenv("DAILY_BRIEF_LON")
TEMPEST_API_TOKEN = os.getenv("TEMPEST_API_TOKEN")
TEMPEST_STATION_ID = int(os.getenv("TEMPEST_STATION_ID", "475329"))


def ensure_table(conn: sqlite3.Connection):
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS daily_briefs (
            date TEXT PRIMARY KEY,
            generated_at TEXT,
            tz TEXT,
            headline TEXT,
            bullets_json TEXT,
            tomorrow_text TEXT,
            model TEXT,
            version TEXT
        )
        """
    )
    conn.commit()


def ensure_afd_table(conn: sqlite3.Connection):
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS nws_afd_highlights (
            product_id TEXT PRIMARY KEY,
            issued TEXT,
            cwa TEXT,
            headline TEXT,
            highlights_json TEXT,
            text TEXT,
            created_at TEXT
        )
        """
    )
    conn.commit()


def save_afd_highlights(conn: sqlite3.Connection, afd: dict, highlights: list[str] | None):
    product_id = afd.get("id")
    if not product_id:
        return
    ensure_afd_table(conn)
    conn.execute(
        """
        INSERT INTO nws_afd_highlights (
            product_id,
            issued,
            cwa,
            headline,
            highlights_json,
            text,
            created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(product_id) DO UPDATE SET
          issued=excluded.issued,
          cwa=excluded.cwa,
          headline=excluded.headline,
          highlights_json=excluded.highlights_json,
          text=excluded.text
        """,
        (
            product_id,
            afd.get("issued"),
            afd.get("cwa"),
            afd.get("headline"),
            json.dumps(highlights or []),
            afd.get("text", ""),
            datetime.now(timezone.utc).isoformat(),
        ),
    )
    conn.commit()


def load_obs(conn: sqlite3.Connection, since_epoch: int):
    obs = pd.read_sql_query(
        """
        SELECT obs_epoch, air_temperature, wind_avg, station_pressure, rain_accumulated
        FROM obs_st
        WHERE obs_epoch >= ?
        ORDER BY obs_epoch
        """,
        conn,
        params=(since_epoch,),
    )
    if not obs.empty:
        obs["dt"] = pd.to_datetime(obs["obs_epoch"], unit="s", utc=True)
    return obs


def load_aqi(conn: sqlite3.Connection, since_epoch: int):
    try:
        df = pd.read_sql_query(
            """
            SELECT ts, pm_2p5
            FROM airlink_current_obs
            WHERE ts >= ?
            ORDER BY ts
            """,
            conn,
            params=(since_epoch,),
        )
    except Exception:
        return pd.DataFrame()
    if not df.empty:
        df["dt"] = pd.to_datetime(df["ts"], unit="s", utc=True)
    return df


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

    if DAILY_BRIEF_LAT and DAILY_BRIEF_LON:
        try:
            return float(DAILY_BRIEF_LAT), float(DAILY_BRIEF_LON)
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


def compute_history_line(conn: sqlite3.Connection, tz_name: str, years_back: int = 5) -> str | None:
    try:
        tz = ZoneInfo(tz_name)
    except Exception:
        tz = ZoneInfo("UTC")
    today = datetime.now(tz).date()
    temps_high = []
    temps_low = []
    for year in range(today.year - years_back, today.year):
        try:
            day = today.replace(year=year)
        except ValueError:
            continue
        start_local = datetime(day.year, day.month, day.day, 0, 0, tzinfo=tz)
        end_local = start_local + timedelta(days=1)
        start_epoch = int(start_local.timestamp())
        end_epoch = int(end_local.timestamp())
        df = pd.read_sql_query(
            """
            SELECT air_temperature
            FROM obs_st
            WHERE obs_epoch >= ? AND obs_epoch < ?
            """,
            conn,
            params=(start_epoch, end_epoch),
        )
        if df.empty:
            continue
        temps = pd.to_numeric(df["air_temperature"], errors="coerce").dropna()
        if temps.empty:
            continue
        temps_high.append(temps.max())
        temps_low.append(temps.min())
    if not temps_high or not temps_low:
        return None
    high_f = (sum(temps_high) / len(temps_high)) * 9 / 5 + 32
    low_f = (sum(temps_low) / len(temps_low)) * 9 / 5 + 32
    return f"Typical highs around {high_f:.0f}F, lows near {low_f:.0f}F based on past years."


def compute_history_line_meteostat(lat: float, lon: float, tz_name: str, years_back: int = 10) -> str | None:
    try:
        from meteostat import Point
    except Exception:
        return None
    try:
        tz = ZoneInfo(tz_name)
    except Exception:
        tz = ZoneInfo("UTC")
    today = datetime.now(tz).date()
    start = datetime(today.year - years_back, 1, 1)
    end = datetime(today.year, 12, 31)
    point = Point(lat, lon)
    data = None
    try:
        from meteostat import Daily
        data = Daily(point, start, end).fetch()
    except Exception:
        try:
            from meteostat import daily as daily_fn
            ts = daily_fn(point, start, end)
            data = ts.fetch() if ts is not None else None
        except Exception:
            data = None
    if data is None or data.empty:
        return None
    data = data.copy()
    data["month"] = data.index.month
    data["day"] = data.index.day
    same_day = data[(data["month"] == today.month) & (data["day"] == today.day)]
    if same_day.empty:
        return None
    tmax = pd.to_numeric(same_day.get("tmax"), errors="coerce").dropna()
    tmin = pd.to_numeric(same_day.get("tmin"), errors="coerce").dropna()
    if tmax.empty or tmin.empty:
        return None
    high_f = (tmax.mean() * 9 / 5) + 32
    low_f = (tmin.mean() * 9 / 5) + 32
    return f"On this day in recent years, average highs are {high_f:.0f}F with lows around {low_f:.0f}F."


def compute_history_line_openmeteo(lat: float, lon: float, tz_name: str, years_back: int = 10) -> str | None:
    try:
        tz = ZoneInfo(tz_name)
    except Exception:
        tz = ZoneInfo("UTC")
    today = datetime.now(tz).date()
    highs = []
    lows = []
    for year in range(today.year - years_back, today.year):
        try:
            day = today.replace(year=year)
        except ValueError:
            continue
        date_str = day.isoformat()
        params = {
            "latitude": lat,
            "longitude": lon,
            "start_date": date_str,
            "end_date": date_str,
            "daily": ["temperature_2m_max", "temperature_2m_min"],
            "temperature_unit": "fahrenheit",
            "timezone": tz_name,
        }
        try:
            resp = requests.get("https://archive-api.open-meteo.com/v1/archive", params=params, timeout=10)
            resp.raise_for_status()
            payload = resp.json()
            daily = payload.get("daily") or {}
            tmax = daily.get("temperature_2m_max") or []
            tmin = daily.get("temperature_2m_min") or []
            if tmax and tmin:
                highs.append(float(tmax[0]))
                lows.append(float(tmin[0]))
        except Exception:
            continue
    if not highs or not lows:
        return None
    return (
        f"On this day in recent years, average highs are {sum(highs)/len(highs):.0f}F "
        f"with lows around {sum(lows)/len(lows):.0f}F."
    )


def format_nws_time(value: str | None, tz_name: str) -> str | None:
    if not value:
        return None
    try:
        cleaned = value.replace("Z", "+00:00")
        dt = datetime.fromisoformat(cleaned)
        tz = ZoneInfo(tz_name)
        return dt.astimezone(tz).strftime("%b %d %I:%M %p").lstrip("0")
    except Exception:
        return value


def build_prompt(
    obs: pd.DataFrame,
    aqi: pd.DataFrame,
    tz: str,
    history_line: str | None = None,
    alert_lines: list[str] | None = None,
    afd_highlights: list[str] | None = None,
    afd_issued: str | None = None,
):
    lines = []
    if not obs.empty:
        obs["air_temperature_f"] = obs["air_temperature"] * 9 / 5 + 32
        obs["wind_mph"] = obs["wind_avg"] * 2.23694 if "wind_avg" in obs else None
        obs["pressure_inhg"] = obs["station_pressure"] * 0.02953 if "station_pressure" in obs else None
        lines.append(
            f"Temp: min {obs['air_temperature_f'].min():.1f}F, max {obs['air_temperature_f'].max():.1f}F, avg {obs['air_temperature_f'].mean():.1f}F."
        )
        if "wind_mph" in obs:
            lines.append(f"Wind avg {obs['wind_mph'].mean():.1f} mph, max {obs['wind_mph'].max():.1f} mph.")
        if "pressure_inhg" in obs:
            lines.append(
                f"Pressure range {obs['pressure_inhg'].min():.2f} to {obs['pressure_inhg'].max():.2f} inHg."
            )
    if not aqi.empty:
        lines.append(f"AQI (PM2.5) max {aqi['pm_2p5'].max():.0f}, avg {aqi['pm_2p5'].mean():.0f}.")
    if history_line:
        lines.append(f"History: {history_line}")
    if alert_lines:
        lines.append("Active alerts:")
        lines.extend(alert_lines)
    if afd_highlights:
        if afd_issued:
            lines.append(f"AFD issued: {afd_issued}")
        lines.append("AFD highlights:")
        lines.extend(f"- {item}" for item in afd_highlights)
    return "\n".join(lines) or "No data."


def call_openai(prompt: str):
    try:
        import openai
    except Exception:
        return None
    client = openai.OpenAI()
    schema = {
        "type": "object",
        "properties": {
            "headline": {"type": "string"},
            "bullets": {"type": "array", "items": {"type": "string"}},
            "tomorrow": {"type": ["string", "null"]},
        },
        "required": ["headline", "bullets", "tomorrow"],
    }
    resp = client.chat.completions.create(
        model=OPENAI_MODEL,
        messages=[
            {
                "role": "system",
                "content": (
                "Write a short, story-forward daily weather brief in a warm local-meteorologist voice. "
                "Include a vivid headline, 3-5 bullets, and a light narrative arc (what happened, what it felt like, "
                "what to watch). If a History line is provided, include a bullet that starts with 'On this day:' "
                "and weave it in naturally. If AFD highlights are provided, include at most one bullet that starts "
                "with 'Forecaster context:' and summarize the highlights without quoting them. Avoid sounding like "
                "raw stats."
            ),
        },
        {"role": "user", "content": prompt},
        ],
        response_format={"type": "json_schema", "json_schema": {"name": "brief", "schema": schema}},
    )
    content = resp.choices[0].message.content
    try:
        return json.loads(content)
    except Exception:
        return None


def save_brief(conn: sqlite3.Connection, date_str: str, tz: str, brief: dict):
    ensure_table(conn)
    conn.execute(
        """
        INSERT INTO daily_briefs (date, generated_at, tz, headline, bullets_json, tomorrow_text, model, version)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(date) DO UPDATE SET
          generated_at=excluded.generated_at,
          tz=excluded.tz,
          headline=excluded.headline,
          bullets_json=excluded.bullets_json,
          tomorrow_text=excluded.tomorrow_text,
          model=excluded.model,
          version=excluded.version
        """,
        (
            date_str,
            datetime.now(timezone.utc).isoformat(),
            tz,
            brief.get("headline", ""),
            json.dumps(brief.get("bullets", [])),
            brief.get("tomorrow"),
            brief.get("model", OPENAI_MODEL),
            "1",
        ),
    )
    conn.commit()


def run_once():
    tz = LOCAL_TZ
    now = datetime.now(timezone.utc)
    start = now - timedelta(hours=24)
    since_epoch = int(start.timestamp())
    with sqlite3.connect(DB_PATH) as conn:
        ensure_table(conn)
        obs = load_obs(conn, since_epoch)
        aqi = load_aqi(conn, since_epoch)
        smoke_event_active = False
        try:
            with config_connect(DB_PATH) as cfg:
                smoke_event_active = bool(get_bool(cfg, "aqi_smoke_event_enabled") or False)
        except Exception:
            smoke_event_active = False
        if smoke_event_active:
            aqi = pd.DataFrame()
        lat, lon = resolve_location()
        history_line = None
        alert_lines = []
        hwo_summary = None
        afd_highlights = None
        afd_issued = None
        if lat is not None and lon is not None:
            history_line = compute_history_line_meteostat(lat, lon, tz)
            if not history_line:
                history_line = compute_history_line_openmeteo(lat, lon, tz)
            alerts = fetch_active_alerts(lat, lon, tz)
            alert_lines = summarize_alerts(alerts, tz, max_items=2)
            hwo_summary = summarize_hwo(fetch_hwo_text(lat, lon))
            afd = fetch_afd_text(lat, lon)
            afd_highlights = summarize_afd(afd, max_items=3)
            if afd and afd.get("issued"):
                afd_issued = format_nws_time(afd.get("issued"), tz)
            if afd:
                save_afd_highlights(conn, afd, afd_highlights)
        if not history_line:
            history_line = compute_history_line(conn, tz)
        if hwo_summary:
            alert_lines = alert_lines + [f"Outlook: {hwo_summary}"]
        prompt = build_prompt(
            obs,
            aqi,
            tz,
            history_line=history_line,
            alert_lines=alert_lines,
            afd_highlights=afd_highlights,
            afd_issued=afd_issued,
        )
        brief = call_openai(prompt)
        if brief:
            save_brief(conn, now.astimezone().date().isoformat(), tz, brief)


def main():
    while True:
        try:
            run_once()
        except Exception:
            pass
        time.sleep(max(300, INTERVAL_MINUTES * 60))


if __name__ == "__main__":
    main()
