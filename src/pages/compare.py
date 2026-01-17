from datetime import timedelta

import altair as alt
import pandas as pd
import streamlit as st

from src.ui.components.cards import chart_card, status_card


def _metric_source(metric: str):
    metric = metric.lower()
    if metric == "aqi":
        return "airlink", "aqi_pm25"
    if metric == "wind":
        return "tempest", "wind_speed_mph"
    return "tempest", "air_temperature_f"


def _build_day_series(df, value_col, tz_name, target_date, label):
    if df.empty:
        return None
    work = df[["time", value_col]].dropna().copy()
    work["local_time"] = work["time"].dt.tz_convert(tz_name)
    work["local_date"] = work["local_time"].dt.date
    day = work[work["local_date"] == target_date].copy()
    if day.empty:
        return None
    base = pd.Timestamp("2000-01-01", tz=tz_name)
    day["time"] = base + (day["local_time"] - day["local_time"].dt.normalize())
    day["value"] = pd.to_numeric(day[value_col], errors="coerce")
    day["metric"] = label
    return day[["time", "value", "metric"]].dropna()


def _build_week_series(df, value_col, tz_name, week_start, label):
    if df.empty:
        return None
    work = df[["time", value_col]].dropna().copy()
    work["local_time"] = work["time"].dt.tz_convert(tz_name)
    week_end = week_start + timedelta(days=7)
    week = work[(work["local_time"] >= week_start) & (work["local_time"] < week_end)].copy()
    if week.empty:
        return None
    base = pd.Timestamp("2000-01-01", tz=tz_name)
    week["time"] = base + (week["local_time"] - week_start)
    week["value"] = pd.to_numeric(week[value_col], errors="coerce")
    week["metric"] = label
    return week[["time", "value", "metric"]].dropna()


def render(ctx):
    tz_name = ctx.get("tz_name") or "UTC"
    tempest = ctx.get("tempest", pd.DataFrame())
    airlink = ctx.get("airlink", pd.DataFrame())

    st.markdown("<div class='section-title'>Compare</div>", unsafe_allow_html=True)
    mode = st.radio(
        "Compare mode",
        ["Today vs Yesterday", "This week vs Last week", "Same day last year"],
        index=0,
    )
    metric = st.selectbox("Metric", ["Temperature", "AQI", "Wind"], index=0)

    source_name, value_col = _metric_source(metric)
    source_df = tempest if source_name == "tempest" else airlink
    if source_df is None or source_df.empty or value_col not in source_df:
        st.info("No comparison data available.")
        return

    compare_df = None
    labels = ()
    if mode == "Today vs Yesterday":
        local_dates = source_df["time"].dt.tz_convert(tz_name).dt.date
        latest_date = local_dates.max()
        prev_date = latest_date - timedelta(days=1)
        today = _build_day_series(source_df, value_col, tz_name, latest_date, "Today")
        yesterday = _build_day_series(source_df, value_col, tz_name, prev_date, "Yesterday")
        labels = ("Today", "Yesterday")
        compare_df = pd.concat([today, yesterday]) if today is not None or yesterday is not None else None
    elif mode == "This week vs Last week":
        local_time = source_df["time"].dt.tz_convert(tz_name)
        week_start = (local_time.dt.floor("D") - pd.to_timedelta(local_time.dt.weekday, unit="D")).max()
        prev_week_start = week_start - timedelta(days=7)
        this_week = _build_week_series(source_df, value_col, tz_name, week_start, "This week")
        last_week = _build_week_series(source_df, value_col, tz_name, prev_week_start, "Last week")
        labels = ("This week", "Last week")
        compare_df = pd.concat([this_week, last_week]) if this_week is not None or last_week is not None else None
    else:
        local_dates = source_df["time"].dt.tz_convert(tz_name).dt.date
        latest_date = local_dates.max()
        prev_date = (pd.Timestamp(latest_date) - pd.DateOffset(years=1)).date()
        current = _build_day_series(source_df, value_col, tz_name, latest_date, "This year")
        last_year = _build_day_series(source_df, value_col, tz_name, prev_date, "Last year")
        labels = ("This year", "Last year")
        compare_df = pd.concat([current, last_year]) if current is not None or last_year is not None else None

    if compare_df is None or compare_df.empty:
        st.info("Not enough data to compare.")
        return

    chart_renderer = ctx.get("chart_renderer")

    def chart_body():
        if chart_renderer:
            st.altair_chart(chart_renderer(compare_df, height=260, title=None), use_container_width=True)
        else:
            chart = (
                alt.Chart(compare_df)
                .mark_line(interpolate="monotone")
                .encode(
                    x=alt.X("time:T", title="Time"),
                    y=alt.Y("value:Q", title=None),
                    color=alt.Color("metric:N", legend=None),
                )
                .properties(height=260)
            )
            st.altair_chart(chart, use_container_width=True)

    chart_card("Comparison", chart_body)

    def stats_for(label):
        series = compare_df[compare_df["metric"] == label]["value"]
        return {
            "max": series.max() if not series.empty else None,
            "min": series.min() if not series.empty else None,
            "avg": series.mean() if not series.empty else None,
        }

    stats_a = stats_for(labels[0])
    stats_b = stats_for(labels[1])
    if stats_a["avg"] is not None and stats_b["avg"] is not None:
        delta_avg = stats_a["avg"] - stats_b["avg"]
        delta_max = stats_a["max"] - stats_b["max"]
        delta_min = stats_a["min"] - stats_b["min"]
        status_card(
            "Summary deltas",
            [
                ("Avg", f"{delta_avg:+.1f}"),
                ("Max", f"{delta_max:+.1f}"),
                ("Min", f"{delta_min:+.1f}"),
            ],
        )
