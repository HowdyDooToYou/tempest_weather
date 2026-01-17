import pandas as pd


def parse_tempest_forecast(payload: dict | None, tz_fallback: str = "UTC"):
    """
    Parse Tempest better_forecast payload into hourly/daily DataFrames.

    Returns (hourly_df | None, daily_df | None, tz_name).
    """
    if not payload or not isinstance(payload, dict):
        return None, None, tz_fallback

    tz_name = payload.get("timezone") or tz_fallback
    forecast = payload.get("forecast") or {}

    hourly_raw = forecast.get("hourly") or []
    hourly_df = pd.DataFrame(hourly_raw)
    if not hourly_df.empty and "time" in hourly_df:
        hourly_df["time"] = pd.to_datetime(hourly_df["time"], unit="s", utc=True).dt.tz_convert(tz_name)
    else:
        hourly_df = None

    daily_raw = forecast.get("daily") or []
    daily_df = pd.DataFrame(daily_raw)
    if not daily_df.empty and "day_start_local" in daily_df:
        daily_df["day_start_local"] = pd.to_datetime(daily_df["day_start_local"], unit="s", utc=True).dt.tz_convert(tz_name)
        if "sunrise" in daily_df:
            daily_df["sunrise"] = pd.to_datetime(daily_df["sunrise"], unit="s", utc=True).dt.tz_convert(tz_name)
        if "sunset" in daily_df:
            daily_df["sunset"] = pd.to_datetime(daily_df["sunset"], unit="s", utc=True).dt.tz_convert(tz_name)
    else:
        daily_df = None

    return hourly_df, daily_df, tz_name
