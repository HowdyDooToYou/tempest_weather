import json
import os
import sqlite3
import time
from datetime import datetime, timedelta, timezone

import pandas as pd

DB_PATH = os.getenv("TEMPEST_DB_PATH", "data/tempest.db")
LOCAL_TZ = os.getenv("LOCAL_TZ", "America/New_York")
INTERVAL_MINUTES = int(os.getenv("DAILY_BRIEF_INTERVAL_MINUTES", "180"))
OPENAI_MODEL = os.getenv("DAILY_BRIEF_MODEL", "gpt-4o-mini")


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


def build_prompt(obs: pd.DataFrame, aqi: pd.DataFrame, tz: str):
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
                "content": "Summarize the past day of weather and AQI into a brief: headline + 3-5 bullets. Keep it concise and local.",
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
        prompt = build_prompt(obs, aqi, tz)
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
