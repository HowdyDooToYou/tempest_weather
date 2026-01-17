import pandas as pd
import streamlit as st

from src.ui.components.cards import status_card


def render(ctx):
    st.markdown("<div class='section-title'>Data</div>", unsafe_allow_html=True)
    section = st.radio(
        "Data sections",
        ["Raw Tables", "Health", "Logs/Status"],
        horizontal=True,
        label_visibility="collapsed",
    )

    if section == "Raw Tables":
        raw_tables = ctx.get("raw_tables") or []
        if not raw_tables:
            st.info("No raw tables available.")
            return
        for table in raw_tables:
            st.markdown(f"<div class='section-title'>{table['title']}</div>", unsafe_allow_html=True)
            st.dataframe(table["df"], use_container_width=True)
        return

    if section == "Health":
        health = ctx.get("health", {})
        sources = health.get("ingest_sources", [])
        if not sources:
            st.info("No health data available.")
            return
        rows = []
        for src in sources:
            rows.append(
                {
                    "Source": src.get("name", "--"),
                    "Last seen": src.get("last_seen", "--"),
                    "Latency": src.get("latency_text", "--"),
                    "Rate": src.get("load_text", "--"),
                }
            )
        st.dataframe(pd.DataFrame(rows), use_container_width=True)
        return

    last_updated = ctx.get("last_updated", {})
    status_card(
        "Status",
        [
            ("Tempest", last_updated.get("Tempest", "--")),
            ("AirLink", last_updated.get("AirLink", "--")),
            ("Hub", last_updated.get("Hub", "--")),
            ("Forecast", ctx.get("forecast_status", "--") or "--"),
        ],
    )
