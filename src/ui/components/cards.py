import streamlit as st


def metric_card(icon: str, label: str, value: str, subvalue: str | None = None, trend: str | None = None):
    sub_html = f"<div class=\"metric-sub\">{subvalue}</div>" if subvalue else ""
    trend_html = f"<div class=\"metric-sub\">{trend}</div>" if trend else ""
    st.markdown(
        f"""
        <div class="card metric-card">
          <div class="metric-icon">{icon}</div>
          <div class="metric-body">
            <div class="metric-label">{label}</div>
            <div class="metric-value">{value}</div>
            {sub_html}
            {trend_html}
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def chart_card(title: str | None, body_renderer, controls: str | None = None):
    if (title and title.strip()) or controls:
        st.markdown(
            f"""
            <div class="chart-label-row">
              {f"<div class=\"chart-label\">{title}</div>" if title and title.strip() else "<div></div>"}
              {f"<div class=\"chart-controls\">{controls}</div>" if controls else ""}
            </div>
            """,
            unsafe_allow_html=True,
        )
    st.markdown(
        """
        <div class="card chart-card">
          <div class="body">
        """,
        unsafe_allow_html=True,
    )
    body_renderer()
    st.markdown("</div></div>", unsafe_allow_html=True)


def status_card(title: str, items: list[tuple[str, str]]):
    lines = "".join(
        f"<div class=\"status-line\"><span>{label}</span><span>{value}</span></div>"
        for label, value in items
    )
    st.markdown(
        f"""
        <div class="card status-card">
          <div class="section-title">{title}</div>
          {lines}
        </div>
        """,
        unsafe_allow_html=True,
    )


def MetricCard(icon: str, label: str, value: str, subvalue: str | None = None, trend: str | None = None):
    metric_card(icon, label, value, subvalue=subvalue, trend=trend)


def ChartCard(title: str, body_renderer, controls: str | None = None):
    chart_card(title, body_renderer, controls=controls)


def StatusCard(title: str, items: list[tuple[str, str]]):
    status_card(title, items)
