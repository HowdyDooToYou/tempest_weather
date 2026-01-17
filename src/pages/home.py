import pandas as pd
import streamlit as st

from src.ui.components.cards import chart_card


def render(ctx):
    forecast_chart = ctx.get("forecast_chart")
    forecast_outlook = ctx.get("forecast_outlook")
    forecast_source = ctx.get("forecast_source")
    forecast_status = ctx.get("forecast_status")
    forecast_updated = ctx.get("forecast_updated")

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
    brief_today = ctx.get("brief_today")
    brief_yesterday = ctx.get("brief_yesterday")
    show_yesterday = False
    if brief_yesterday:
        show_yesterday = st.toggle("Show yesterday", value=False)
    brief = brief_yesterday if show_yesterday else brief_today
    if brief:
        bullets = "".join(f"<li>{item}</li>" for item in brief.get("bullets", []))
        tomorrow = brief.get("tomorrow")
        generated_at = brief.get("generated_at") or ""
        st.markdown(
            f"""
            <div class="card brief-card">
              <div class="section-title">{brief.get('headline','')}</div>
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
