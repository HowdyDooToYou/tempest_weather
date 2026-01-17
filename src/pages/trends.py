import json
import os

import altair as alt
import streamlit as st

from src.config_store import connect as config_connect
from src.config_store import get_config, set_config

from src.ui.components.cards import chart_card

DB_PATH = os.getenv("TEMPEST_DB_PATH", "data/tempest.db")
PREF_KEY = "trends_selected_metrics"


def load_metric_prefs(all_metrics: list[str]) -> list[str] | None:
    try:
        with config_connect(DB_PATH) as conn:
            raw = get_config(conn, PREF_KEY)
    except Exception:
        return None
    if not raw:
        return None
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return None
    if not isinstance(parsed, list):
        return None
    return [metric for metric in parsed if metric in all_metrics]


def save_metric_prefs(selected: list[str]) -> None:
    try:
        with config_connect(DB_PATH) as conn:
            set_config(conn, PREF_KEY, json.dumps(selected))
    except Exception:
        return


def render(ctx):
    trend_series = ctx.get("trend_series", {})
    if not trend_series:
        st.info("No trend data available.")
        return

    all_metrics = list(trend_series.keys())
    stored_prefs = load_metric_prefs(all_metrics)
    defaults = stored_prefs or all_metrics

    st.markdown("<div class='section-title'>Trends</div>", unsafe_allow_html=True)
    with st.expander("Customize", expanded=False):
        selected = st.multiselect(
            "Charts (select in desired order)",
            options=all_metrics,
            default=defaults,
            key="trends_selected_metrics_ui",
        )
        if selected != st.session_state.get("trends_selected_metrics_saved"):
            save_metric_prefs(selected)
            st.session_state.trends_selected_metrics_saved = selected

    if not selected:
        st.info("Select at least one metric to show trends.")
        return

    chart_renderer = ctx.get("chart_renderer")

    for name in selected:
        df = trend_series.get(name)
        if df is None or df.empty:
            continue

        st.markdown(f"<div class='chart-label'>{name}</div>", unsafe_allow_html=True)

        def body_renderer(df=df):
            if chart_renderer:
                st.altair_chart(chart_renderer(df, height=260, title=None), use_container_width=True)
            else:
                chart = (
                    alt.Chart(df)
                    .mark_line(interpolate="monotone")
                    .encode(
                        x=alt.X("time:T", title="Time"),
                        y=alt.Y("value:Q", title=None),
                        color=alt.Color("metric:N", legend=None),
                    )
                    .properties(height=260)
                )
                st.altair_chart(chart, use_container_width=True)

        chart_card("", body_renderer)
