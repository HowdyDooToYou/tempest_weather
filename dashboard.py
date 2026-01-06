import sqlite3
import pandas as pd
import streamlit as st

DB_PATH = "data/tempest.db"

st.set_page_config(
    page_title="Weather & Air Quality Dashboard",
    layout="wide"
)

st.title("Weather & Air Quality Dashboard")


# ------------------------
# Helpers
# ------------------------
def load_df(query, params=None):
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query(query, conn, params=params or {})
    conn.close()
    return df


def epoch_to_dt(series):
    return pd.to_datetime(series, unit="s", utc=True).dt.tz_convert("America/New_York")


def c_to_f(c):
    return (c * 9 / 5) + 32


def hpa_to_inhg(hpa):
    return hpa * 0.0295299830714


def mps_to_mph(mps):
    return mps * 2.2369362921


def compute_heat_index(temp_f, humidity):
    # NOAA formula with low-temp fallback and standard adjustments
    t = pd.Series(temp_f, dtype="float64")
    r = pd.Series(humidity, dtype="float64")

    simple = 0.5 * (t + 61.0 + ((t - 68.0) * 1.2) + (r * 0.094))

    rothfusz = (
        -42.379
        + 2.04901523 * t
        + 10.14333127 * r
        - 0.22475541 * t * r
        - 6.83783e-3 * t * t
        - 5.481717e-2 * r * r
        + 1.22874e-3 * t * t * r
        + 8.5282e-4 * t * r * r
        - 1.99e-6 * t * t * r * r
    )

    hi = rothfusz.where((t >= 80) & (r >= 40), simple)

    adj1_mask = (r < 13) & (t >= 80) & (t <= 112)
    adj1 = ((13 - r) / 4) * ((17 - (t - 95).abs()) / 17)
    hi = hi - adj1.where(adj1_mask, 0)

    adj2_mask = (r > 85) & (t >= 80) & (t <= 87)
    adj2 = ((r - 85) / 10) * ((87 - t) / 5)
    hi = hi + adj2.where(adj2_mask, 0)

    return hi.fillna(t)


# ------------------------
# Sidebar
# ------------------------
st.sidebar.header("Controls")

hours = st.sidebar.slider(
    "Time window (hours)",
    min_value=1,
    max_value=168,
    value=24
)

since_epoch = int(
    (pd.Timestamp.utcnow() - pd.Timedelta(hours=hours)).timestamp()
)

# ------------------------
# Tempest (Outdoor)
# ------------------------
st.subheader("Outdoor Tempest Station")

tempest = load_df(
    """
    SELECT
        obs_epoch,
        air_temperature,
        relative_humidity,
        station_pressure,
        wind_avg
    FROM obs_st
    WHERE obs_epoch >= :since
    ORDER BY obs_epoch
    """,
    {"since": since_epoch},
)

if not tempest.empty:
    tempest["time"] = epoch_to_dt(tempest["obs_epoch"])
    tempest["air_temperature_f"] = c_to_f(tempest["air_temperature"])
    tempest["heat_index_f"] = compute_heat_index(
        tempest["air_temperature_f"],
        tempest["relative_humidity"]
    )
    tempest["pressure_inhg"] = hpa_to_inhg(tempest["station_pressure"])
    tempest["wind_speed_mph"] = mps_to_mph(tempest["wind_avg"])

    c1, c2, c3, c4 = st.columns(4)

    c1.metric(
        "Temperature (F)",
        f"{tempest.air_temperature_f.iloc[-1]:.1f}"
    )
    c2.metric(
        "Heat Index (F)",
        f"{tempest.heat_index_f.iloc[-1]:.1f}"
    )
    c3.metric(
        "Humidity (%)",
        f"{tempest.relative_humidity.iloc[-1]:.0f}"
    )
    c4.metric(
        "Pressure (inHg)",
        f"{tempest.pressure_inhg.iloc[-1]:.2f}"
    )

    st.line_chart(
        tempest.set_index("time")[
            ["air_temperature_f", "heat_index_f"]
        ],
        height=300,
    )

    st.line_chart(
        tempest.set_index("time")[
            ["wind_speed_mph"]
        ],
        height=200,
    )
else:
    st.info("No Tempest data in selected window.")

# ------------------------
# AirLink (Indoor AQ)
# ------------------------
st.subheader("Indoor AirLink")

airlink = load_df(
    """
    SELECT *
    FROM airlink_obs
    WHERE ts >= :since
    ORDER BY ts
    """,
    {"since": since_epoch},
)

if not airlink.empty:
    airlink["time"] = epoch_to_dt(airlink["ts"])

    c1, c2, c3, c4 = st.columns(4)

    c1.metric(
        "Indoor Temp (F)",
        f"{airlink.temp_f.iloc[-1]:.1f}"
    )
    c2.metric(
        "Heat Index (F)",
        f"{airlink.heat_index_f.iloc[-1]:.1f}"
    )
    c3.metric(
        "Humidity (%)",
        f"{airlink.hum.iloc[-1]:.0f}"
    )
    c4.metric(
        "PM2.5 (ug/m3)",
        f"{airlink.pm_2p5.iloc[-1]:.1f}"
    )

    st.line_chart(
        airlink.set_index("time")[
            ["temp_f", "heat_index_f"]
        ],
        height=300,
    )

    st.line_chart(
        airlink.set_index("time")[
            ["pm_1", "pm_2p5", "pm_10"]
        ],
        height=300,
    )
else:
    st.info("No AirLink data in selected window.")

# ------------------------
# Raw Event Explorer
# ------------------------
st.subheader("Raw Event Explorer")

raw = load_df(
    """
    SELECT
        received_at_epoch,
        message_type,
        device_id,
        payload_json
    FROM raw_events
    ORDER BY received_at_epoch DESC
    LIMIT 200
    """
)

if not raw.empty:
    raw["received_at"] = epoch_to_dt(raw["received_at_epoch"])
    st.dataframe(
        raw[[
            "received_at",
            "message_type",
            "device_id",
            "payload_json"
        ]],
        use_container_width=True
    )
else:
    st.info("No raw events available.")
