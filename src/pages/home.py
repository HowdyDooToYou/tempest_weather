import os
import re

import pandas as pd
import streamlit as st

from src.ui.components.cards import chart_card


def render(ctx):
    tz_name = ctx.get("tz_name")
    forecast_chart = ctx.get("forecast_chart")
    forecast_outlook = ctx.get("forecast_outlook")
    forecast_source = ctx.get("forecast_source")
    forecast_status = ctx.get("forecast_status")
    forecast_updated = ctx.get("forecast_updated")
    smoke_event_active = bool(ctx.get("aqi_smoke_event_enabled", False))

    def forecast_body():
        if forecast_chart is not None:
            st.altair_chart(forecast_chart, use_container_width=True)
        else:
            st.info(forecast_status or "No forecast data available.")

    meta_bits = []
    if forecast_source:
        meta_bits.append(f"Source: {forecast_source}")
    if forecast_updated is not None:
        if isinstance(forecast_updated, pd.Timestamp):
            meta_bits.append(f"Updated {forecast_updated.strftime('%b %d %H:%M')}")
        else:
            meta_bits.append(f"Updated {forecast_updated}")
    if forecast_status:
        meta_bits.append(f"Status: {forecast_status}")

    st.markdown("<div class='section-title'>Daily brief</div>", unsafe_allow_html=True)
    if st.button("Refresh brief", key="refresh_brief"):
        if not os.getenv("OPENAI_API_KEY"):
            st.error("OPENAI_API_KEY is not set for the UI process. Set it and restart the UI service.")
        else:
            with st.spinner("Refreshing brief..."):
                try:
                    from src.daily_brief_worker import run_once
                    run_once()
                    st.success("Daily brief refreshed.")
                    st.rerun()
                except Exception as exc:
                    st.error(f"Unable to refresh the brief: {exc}")
    brief_today = ctx.get("brief_today")
    brief_yesterday = ctx.get("brief_yesterday")
    show_yesterday = False
    if brief_yesterday:
        show_yesterday = st.toggle("Show yesterday", value=False)
    brief = brief_yesterday if show_yesterday else brief_today
    def format_brief_generated_at(value, tz_name):
        if not value:
            return ""
        try:
            ts = pd.to_datetime(value, utc=True)
            if tz_name:
                try:
                    ts = ts.tz_convert(tz_name)
                except Exception:
                    pass
            return ts.strftime("%b %d %I:%M %p").lstrip("0")
        except Exception:
            return str(value)

    if brief:
        headline = brief.get("headline", "")
        if smoke_event_active and re.search(r"\b(aqi|pm2\.?5|air quality)\b", headline, re.IGNORECASE):
            headline = "Daily brief"
        bullets_list = brief.get("bullets", [])
        if smoke_event_active:
            bullets_list = [
                item
                for item in bullets_list
                if not re.search(r"\b(aqi|pm2\.?5|air quality)\b", item, re.IGNORECASE)
            ]
        bullets = "".join(f"<li>{item}</li>" for item in bullets_list)
        tomorrow = brief.get("tomorrow")
        if smoke_event_active and tomorrow and re.search(r"\b(aqi|pm2\.?5|air quality)\b", tomorrow, re.IGNORECASE):
            tomorrow = None
        generated_at = format_brief_generated_at(brief.get("generated_at"), tz_name)
        st.markdown(
            f"""
            <div class="card brief-card">
              <div class="section-title">{headline}</div>
              <ul>{bullets}</ul>
              {f"<div class='metric-sub'>Tomorrow: {tomorrow}</div>" if tomorrow else ""}
              {f"<div class='metric-sub'>Last generated {generated_at}</div>" if generated_at else ""}
            </div>
            """,
            unsafe_allow_html=True,
        )
    else:
        st.info("Daily brief will appear here once generated.")

    st.markdown("<div class='section-title'>Forecast</div>", unsafe_allow_html=True)
    chart_card("", forecast_body)
    if meta_bits:
        st.caption(" | ".join(meta_bits))

    if forecast_outlook is not None:
        chart_card("7-day outlook", lambda: st.altair_chart(forecast_outlook, use_container_width=True))
    else:
        st.info("No daily outlook available.")
