import json
import os
import re

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

from src.config_store import connect as config_connect
from src.config_store import get_bool, get_config, set_bool, set_config
from src.ui.components.cards import chart_card

DB_PATH = os.getenv("TEMPEST_DB_PATH", "data/tempest.db")
RADAR_LAYER_PREF_KEY = "radar_overlay_layers"
RADAR_SHOW_PREF_KEY = "radar_show_imagery"
NOWCOAST_WMS = "https://nowcoast.noaa.gov/geoserver/ows"
RADAR_OVERLAY_CATALOG = [
    {
        "id": "precip",
        "label": "Precipitation (6-hr)",
        "wms_layer": "ndfd_precipitation:6hr_precipitation_amount",
        "legend_items": [
            {"label": "0-0.1 in", "color": "#bfe5ff"},
            {"label": "0.1-0.5", "color": "#7cc4ff"},
            {"label": "0.5-1", "color": "#4f9e75"},
            {"label": "1-2", "color": "#f4b65c"},
            {"label": "2+", "color": "#e35b5b"},
        ],
        "opacity": 0.55,
    },
    {
        "id": "snow",
        "label": "Snowfall (6-hr)",
        "wms_layer": "ndfd_precipitation:6hr_snow_amount",
        "legend_items": [
            {"label": "0-1 in", "color": "#e6f2ff"},
            {"label": "1-2", "color": "#b8d4ff"},
            {"label": "2-4", "color": "#86a9ff"},
            {"label": "4-8", "color": "#5a78ff"},
            {"label": "8+", "color": "#4b3fd1"},
        ],
        "opacity": 0.55,
    },
    {
        "id": "temperature",
        "label": "Temperature (F)",
        "wms_layer": "ndfd_temperature:air_temperature",
        "legend_items": [
            {"label": "<=32F", "color": "#6bb6ff"},
            {"label": "33-50", "color": "#7fd0d6"},
            {"label": "51-70", "color": "#86c05c"},
            {"label": "71-85", "color": "#f2c14e"},
            {"label": "86+", "color": "#e06b5a"},
        ],
        "opacity": 0.45,
    },
    {
        "id": "wind_speed",
        "label": "Wind Speed (kn)",
        "wms_layer": "ndfd_wind:wind_speed",
        "legend_items": [
            {"label": "0-5", "color": "#b7f3c1"},
            {"label": "6-15", "color": "#86d9a7"},
            {"label": "16-25", "color": "#f0c766"},
            {"label": "26-40", "color": "#f08b4b"},
            {"label": "40+", "color": "#e35b5b"},
        ],
        "opacity": 0.5,
    },
    {
        "id": "wind_gust",
        "label": "Wind Gust (kn)",
        "wms_layer": "ndfd_wind:wind_gust",
        "legend_items": [
            {"label": "0-10", "color": "#c7f0ff"},
            {"label": "11-20", "color": "#86c5f6"},
            {"label": "21-35", "color": "#f2b25f"},
            {"label": "36-50", "color": "#f17f4c"},
            {"label": "50+", "color": "#e05252"},
        ],
        "opacity": 0.5,
    },
    {
        "id": "lightning",
        "label": "Lightning Density",
        "wms_layer": "lightning_detection:ldn_lightning_strike_density",
        "legend_items": [
            {"label": "Low", "color": "#d9f2ff"},
            {"label": "Moderate", "color": "#82c5ff"},
            {"label": "High", "color": "#ffb454"},
            {"label": "Extreme", "color": "#e35b5b"},
        ],
        "opacity": 0.6,
    },
]


def load_radar_layer_prefs(valid_ids: list[str]) -> list[str] | None:
    try:
        with config_connect(DB_PATH) as conn:
            raw = get_config(conn, RADAR_LAYER_PREF_KEY)
    except Exception:
        return None
    if raw is None:
        return None
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return None
    if not isinstance(parsed, list):
        return None
    return [layer_id for layer_id in parsed if layer_id in valid_ids]


def save_radar_layer_prefs(selected: list[str]) -> None:
    try:
        with config_connect(DB_PATH) as conn:
            set_config(conn, RADAR_LAYER_PREF_KEY, json.dumps(selected))
    except Exception:
        return


def load_radar_show_pref() -> bool | None:
    try:
        with config_connect(DB_PATH) as conn:
            return get_bool(conn, RADAR_SHOW_PREF_KEY)
    except Exception:
        return None


def save_radar_show_pref(value: bool) -> None:
    try:
        with config_connect(DB_PATH) as conn:
            set_bool(conn, RADAR_SHOW_PREF_KEY, value)
    except Exception:
        return


def render(ctx):
    tz_name = ctx.get("tz_name")
    forecast_chart = ctx.get("forecast_chart")
    forecast_outlook = ctx.get("forecast_outlook")
    forecast_source = ctx.get("forecast_source")
    forecast_status = ctx.get("forecast_status")
    forecast_updated = ctx.get("forecast_updated")
    smoke_event_active = bool(ctx.get("aqi_smoke_event_enabled", False))
    station_lat = ctx.get("station_lat")
    station_lon = ctx.get("station_lon")

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

    def coerce_float(value):
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

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

    st.markdown("<div class='section-title'>Radar</div>", unsafe_allow_html=True)
    lat_value = coerce_float(station_lat)
    lon_value = coerce_float(station_lon)
    if lat_value is None or lon_value is None:
        st.info("Radar unavailable (location not set).")
    else:
        overlay_ids = [layer["id"] for layer in RADAR_OVERLAY_CATALOG]
        stored_layers = load_radar_layer_prefs(overlay_ids)
        default_ids = overlay_ids if stored_layers is None else stored_layers
        stored_show_radar = load_radar_show_pref()
        default_show_radar = True if stored_show_radar is None else stored_show_radar
        label_by_id = {layer["id"]: layer["label"] for layer in RADAR_OVERLAY_CATALOG}
        options = [layer["label"] for layer in RADAR_OVERLAY_CATALOG]
        default_labels = [label_by_id[layer_id] for layer_id in default_ids if layer_id in label_by_id]
        with st.expander("Radar layers", expanded=False):
            show_radar = st.checkbox(
                "Show radar imagery",
                value=default_show_radar,
                key="radar_show_imagery_ui",
            )
            if show_radar != st.session_state.get("radar_show_imagery_saved"):
                save_radar_show_pref(show_radar)
                st.session_state.radar_show_imagery_saved = show_radar
            selected_labels = st.multiselect(
                "Overlay layers",
                options=options,
                default=default_labels,
                key="radar_overlay_layers_ui",
            )
            selected_ids = [
                layer["id"]
                for layer in RADAR_OVERLAY_CATALOG
                if layer["label"] in selected_labels
            ]
            if selected_ids != st.session_state.get("radar_overlay_layers_saved"):
                save_radar_layer_prefs(selected_ids)
                st.session_state.radar_overlay_layers_saved = selected_ids
            st.caption("Selections are saved in the app database.")
        overlay_catalog = [
            {
                "id": layer["id"],
                "label": layer["label"],
                "layer": layer["wms_layer"],
                "legend": layer.get("legend"),
                "legend_items": layer.get("legend_items", []),
                "opacity": layer["opacity"],
            }
            for layer in RADAR_OVERLAY_CATALOG
        ]
        overlay_catalog_json = json.dumps(overlay_catalog)
        selected_ids_json = json.dumps(selected_ids)
        has_layers = bool(selected_ids)
        show_radar_json = "true" if show_radar and has_layers else "false"
        past_tab, future_tab = st.tabs(["Past 2 hours", "Next 2 hours (forecast)"])
        with past_tab:
            radar_html = f"""
            <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
            <div id="radar-map-past-shell" class="radar-map-shell">
              <button id="radar-map-past-fullscreen" class="radar-fullscreen-btn" type="button">Full screen</button>
              <button id="radar-map-past-layers-btn" class="radar-layers-btn" type="button">Layers</button>
              <div id="radar-map-past-layers-panel" class="radar-layers-panel" style="display:none;"></div>
              <div id="radar-map-past" class="radar-map"></div>
            </div>
            <div id="radar-map-past-label" style="margin-top: 6px; font-size: 0.85rem; color: var(--text-secondary);"></div>
            <div id="radar-map-past-controls" style="margin-top: 8px;">
              <div style="display:flex; justify-content:space-between; font-size:0.75rem; color:var(--text-muted); margin-bottom:4px;">
                <span id="radar-map-past-start">--</span>
                <span id="radar-map-past-end">--</span>
              </div>
              <input id="radar-map-past-slider" class="radar-slider" type="range" min="0" max="1" step="1" value="0" />
              <div style="margin-top:6px; height:6px; border-radius:999px; background: var(--surface-4); overflow:hidden;">
                <div id="radar-map-past-progress" style="height:100%; width:0%; background: linear-gradient(90deg, var(--accent-soft), var(--accent));"></div>
              </div>
            </div>
            <style>
              .radar-map-shell {{
                position: relative;
                height: 300px;
                width: 100%;
                border-radius: 16px;
                overflow: hidden;
                background: var(--surface-3);
              }}
              .radar-map {{
                height: 100%;
                width: 100%;
              }}
              .radar-fullscreen-btn {{
                position: absolute;
                top: 10px;
                right: 10px;
                z-index: 600;
                padding: 6px 10px;
                border-radius: 999px;
                border: 1px solid var(--border);
                background: var(--surface-3);
                color: var(--text-primary);
                font-size: 0.7rem;
                letter-spacing: 0.02em;
                cursor: pointer;
              }}
              .radar-fullscreen-btn:hover {{
                background: var(--surface-2);
              }}
              .radar-layers-btn {{
                position: absolute;
                top: 10px;
                left: 10px;
                z-index: 600;
                padding: 6px 10px;
                border-radius: 999px;
                border: 1px solid var(--border);
                background: var(--surface-3);
                color: var(--text-primary);
                font-size: 0.7rem;
                letter-spacing: 0.02em;
                cursor: pointer;
                display: none;
              }}
              .radar-layers-btn:hover {{
                background: var(--surface-2);
              }}
              .radar-layers-panel {{
                position: absolute;
                top: 44px;
                left: 10px;
                z-index: 610;
                min-width: 200px;
                max-width: 260px;
                padding: 10px;
                border-radius: 12px;
                background: var(--surface-3);
                border: 1px solid var(--border);
                color: var(--text-primary);
                font-size: 0.7rem;
                box-shadow: var(--shadow);
              }}
              .radar-layers-title {{
                text-transform: uppercase;
                letter-spacing: 0.08em;
                font-size: 0.6rem;
                color: var(--text-secondary);
                margin-bottom: 6px;
              }}
              .radar-layers-item {{
                display: flex;
                align-items: center;
                gap: 8px;
                margin: 4px 0;
              }}
              .radar-layers-item input {{
                accent-color: var(--accent);
              }}
              .radar-layers-divider {{
                height: 1px;
                background: var(--border);
                margin: 6px 0;
              }}
              .radar-slider {{
                width: 100%;
                height: 6px;
                border-radius: 999px;
                background: var(--surface-4);
                outline: none;
                cursor: pointer;
              }}
              .radar-slider::-webkit-slider-thumb {{
                -webkit-appearance: none;
                appearance: none;
                width: 14px;
                height: 14px;
                border-radius: 50%;
                background: var(--accent);
                border: 2px solid var(--surface-3);
                box-shadow: 0 2px 6px rgba(0,0,0,0.4);
              }}
              .radar-slider::-moz-range-thumb {{
                width: 14px;
                height: 14px;
                border-radius: 50%;
                background: var(--accent);
                border: 2px solid var(--surface-3);
                box-shadow: 0 2px 6px rgba(0,0,0,0.4);
              }}
              .radar-legend {{
                background: var(--surface-3);
                border: 1px solid var(--border);
                border-radius: 12px;
                padding: 6px 8px;
                display: grid;
                gap: 6px;
                font-size: 0.7rem;
                color: var(--text-primary);
                box-shadow: var(--shadow);
              }}
              .radar-legend-title {{
                text-transform: uppercase;
                letter-spacing: 0.08em;
                font-size: 0.6rem;
                color: var(--text-secondary);
              }}
              .radar-legend-row {{
                display: flex;
                flex-direction: column;
                align-items: flex-start;
                gap: 4px;
              }}
              .radar-legend-scale {{
                display: flex;
                flex-wrap: wrap;
                gap: 4px;
              }}
              .radar-legend-chip {{
                display: inline-flex;
                align-items: center;
                gap: 6px;
                padding: 2px 6px;
                border-radius: 6px;
                background: var(--surface-4);
                border: 1px solid var(--border);
                font-size: 0.6rem;
                color: var(--text-primary);
              }}
              .radar-legend-swatch {{
                width: 12px;
                height: 12px;
                border-radius: 4px;
                display: inline-block;
              }}
              .radar-legend-label {{
                font-size: 0.65rem;
                color: var(--text-primary);
                text-shadow: 0 1px 2px rgba(0,0,0,0.6);
              }}
              .radar-legend-img {{
                height: auto;
                width: auto;
                max-width: 300px;
                max-height: 100px;
                border-radius: 6px;
                background: var(--surface-4);
                padding: 3px 6px;
                border: 1px solid var(--border);
                display: block;
              }}
            </style>
            <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
            <script>
              (function() {{
                const map = L.map('radar-map-past', {{
                  zoomControl: true,
                  attributionControl: false
                }}).setView([{lat_value:.4f}, {lon_value:.4f}], 8);
                const baseMaxZoom = 18;
                const imageryMaxZoom = 12;

                const base = L.tileLayer(
                  'https://{{s}}.basemaps.cartocdn.com/dark_all/{{z}}/{{x}}/{{y}}{{r}}.png',
                  {{
                    maxZoom: baseMaxZoom,
                    subdomains: 'abcd'
                  }}
                );
                base.addTo(map);

                const wmsUrl = 'https://mesonet.agron.iastate.edu/cgi-bin/wms/nexrad/n0q-t.cgi';
                const frames = [];
                const now = new Date();
                const minutesStep = 5;
                const totalMinutes = 120;
                const stepMillis = minutesStep * 60000;
                const roundedNow = new Date(Math.floor(now.getTime() / stepMillis) * stepMillis);
                for (let offset = totalMinutes; offset >= 0; offset -= minutesStep) {{
                  const t = new Date(roundedNow.getTime() - offset * 60000);
                  const iso = t.toISOString().replace(/\\.\\d{{3}}Z$/, 'Z');
                  frames.push(iso);
                }}
                const targetOpacity = 0.7;
                const frameIntervalMs = 1200;
                const baseParams = {{
                  layers: 'nexrad-n0q-wmst',
                  format: 'image/png',
                  version: '1.3.0',
                  transparent: true
                }};
                const radarTileOptions = {{
                  maxZoom: imageryMaxZoom,
                  maxNativeZoom: imageryMaxZoom,
                  tileSize: 256,
                  keepBuffer: 2,
                  updateWhenIdle: true
                }};
                const overlayTileOptions = {{
                  maxZoom: imageryMaxZoom,
                  maxNativeZoom: imageryMaxZoom,
                  tileSize: 256,
                  keepBuffer: 2,
                  updateWhenIdle: true
                }};
                let idx = 0;
                let isLoading = false;
                let activeLoadHandler = null;
                let activeLoadTimeout = null;
                const loadTimeoutMs = 4000;
                let showRadar = {show_radar_json};
                const overlayWmsUrl = '{NOWCOAST_WMS}';
                const overlayCatalog = {overlay_catalog_json};
                const selectedIds = new Set({selected_ids_json});
                let front = null;
                let back = null;
                let legendControl = null;
                const overlayLayers = new Map();
                const clearLoading = () => {{
                  if (activeLoadTimeout) {{
                    clearTimeout(activeLoadTimeout);
                    activeLoadTimeout = null;
                  }}
                  if (activeLoadHandler && back) {{
                    back.off('load', activeLoadHandler);
                  }}
                  activeLoadHandler = null;
                  isLoading = false;
                }};

                const getSelectedOverlays = () =>
                  overlayCatalog.filter((overlay) => selectedIds.has(overlay.id));
                const hasActiveImagery = () => showRadar || getSelectedOverlays().length > 0;
                const applyImageryZoomLimits = () => {{
                  const targetMaxZoom = hasActiveImagery() ? imageryMaxZoom : baseMaxZoom;
                  map.setMaxZoom(targetMaxZoom);
                  if (map.getZoom() > targetMaxZoom) {{
                    map.setZoom(targetMaxZoom);
                  }}
                }};

                const ensureRadarPane = () => {{
                  if (!map.getPane('radarPane')) {{
                    map.createPane('radarPane');
                    map.getPane('radarPane').style.zIndex = 360;
                  }}
                }};

                const ensureOverlayPane = () => {{
                  if (!map.getPane('overlayPane')) {{
                    map.createPane('overlayPane');
                    map.getPane('overlayPane').style.zIndex = 420;
                  }}
                }};

                const initRadarLayers = () => {{
                  if (front || back) return;
                  ensureRadarPane();
                  front = L.tileLayer.wms(wmsUrl, {{
                    ...baseParams,
                    ...radarTileOptions,
                    opacity: targetOpacity,
                    time: frames[0],
                    pane: 'radarPane'
                  }}).addTo(map);
                  back = L.tileLayer.wms(wmsUrl, {{
                    ...baseParams,
                    ...radarTileOptions,
                    opacity: 0,
                    time: frames[1] || frames[0],
                    pane: 'radarPane'
                  }}).addTo(map);
                  front.setZIndex(200);
                  back.setZIndex(190);
                  setLayerFade(front);
                  setLayerFade(back);
                }};

                const destroyRadarLayers = () => {{
                  clearLoading();
                  if (front) map.removeLayer(front);
                  if (back) map.removeLayer(back);
                  front = null;
                  back = null;
                }};

                const setRadarUiState = () => {{
                  const label = document.getElementById('radar-map-past-label');
                  const controls = document.getElementById('radar-map-past-controls');
                  if (!showRadar) {{
                    label.textContent = selectedIds.size
                      ? 'Radar imagery hidden'
                      : 'No radar layers selected';
                    if (controls) controls.style.display = 'none';
                  }} else {{
                    if (controls) controls.style.display = '';
                  }}
                }};

                const setShowRadar = (next) => {{
                  showRadar = Boolean(next);
                  if (!selectedIds.size) {{
                    showRadar = false;
                  }}
                  if (showRadar) {{
                    initRadarLayers();
                  }} else {{
                    destroyRadarLayers();
                  }}
                  applyImageryZoomLimits();
                  setRadarUiState();
                }};

                const renderLegend = (selected) => {{
                  if (legendControl) {{
                    map.removeControl(legendControl);
                    legendControl = null;
                  }}
                  if (!selected.length) return;
                  legendControl = L.control({{ position: 'bottomleft' }});
                  legendControl.onAdd = () => {{
                    const div = L.DomUtil.create('div', 'radar-legend');
                    const legendRows = selected.map((overlay) => {{
                      let legend = '';
                      if (overlay.legend_items && overlay.legend_items.length) {{
                        const chips = overlay.legend_items.map((item) =>
                          '<span class="radar-legend-chip"><span class="radar-legend-swatch" style="background:' +
                          item.color + ';"></span>' + item.label + '</span>'
                        ).join('');
                        legend = '<div class="radar-legend-scale">' + chips + '</div>';
                      }} else if (overlay.legend) {{
                        legend = '<img class="radar-legend-img" src="' + overlay.legend + '" />';
                      }}
                      return '<div class="radar-legend-row"><span class="radar-legend-label">' +
                        overlay.label + '</span>' + legend + '</div>';
                    }}).join('');
                    div.innerHTML = '<div class="radar-legend-title">Layers</div>' + legendRows;
                    L.DomEvent.disableClickPropagation(div);
                    L.DomEvent.disableScrollPropagation(div);
                    return div;
                  }};
                  legendControl.addTo(map);
                }};

                const syncOverlays = () => {{
                  const selected = getSelectedOverlays();
                  const selectedSet = new Set(selected.map((overlay) => overlay.id));
                  if (selected.length) {{
                    ensureOverlayPane();
                  }}
                  overlayCatalog.forEach((overlay) => {{
                    if (selectedSet.has(overlay.id)) {{
                      if (!overlayLayers.has(overlay.id)) {{
                        const layer = L.tileLayer.wms(overlayWmsUrl, {{
                          layers: overlay.layer,
                          format: 'image/png',
                          version: '1.3.0',
                          transparent: true,
                          opacity: overlay.opacity || 0.5,
                          ...overlayTileOptions,
                          pane: 'overlayPane'
                        }});
                        overlayLayers.set(overlay.id, layer);
                        layer.addTo(map);
                      }}
                    }} else {{
                      const existing = overlayLayers.get(overlay.id);
                      if (existing) {{
                        map.removeLayer(existing);
                        overlayLayers.delete(overlay.id);
                      }}
                    }}
                  }});
                  if (selected.length) {{
                    attribution.addAttribution(overlaysAttribution);
                  }} else {{
                    attribution.removeAttribution(overlaysAttribution);
                  }}
                  renderLegend(selected);
                  setShowRadar(showRadar);
                }};


                const label = document.getElementById('radar-map-past-label');
                const progress = document.getElementById('radar-map-past-progress');
                const slider = document.getElementById('radar-map-past-slider');
                const startLabel = document.getElementById('radar-map-past-start');
                const endLabel = document.getElementById('radar-map-past-end');
                const manualHoldMs = 5000;
                let manualHoldUntil = 0;
                const formatTime = (ts) =>
                  new Date(ts).toLocaleTimeString([], {{ hour: 'numeric', minute: '2-digit' }});
                const setLabel = (ts) => {{
                  const t = new Date(ts);
                  label.textContent = `Radar frame: ${{t.toLocaleString()}}`;
                }};
                const updateProgress = () => {{
                  const maxIdx = Math.max(frames.length - 1, 1);
                  const pct = (idx / maxIdx) * 100;
                  progress.style.width = `${{pct}}%`;
                }};
                const updateUi = (frameIdx) => {{
                  idx = frameIdx;
                  const frame = frames[frameIdx];
                  setLabel(frame);
                  slider.value = String(frameIdx);
                  updateProgress();
                }};
                slider.max = String(Math.max(frames.length - 1, 0));
                slider.value = "0";
                startLabel.textContent = formatTime(frames[0]);
                endLabel.textContent = formatTime(frames[frames.length - 1]);
                updateUi(0);

                function setLayerFade(layer) {{
                  if (!layer) return;
                  const container = layer.getContainer();
                  if (container) {{
                    container.style.transition = 'opacity 0.6s ease';
                  }}
                }}
                setLayerFade(front);
                setLayerFade(back);

                const attribution = L.control.attribution({{ position: 'bottomright' }});
                const overlaysAttribution = 'Overlays: NOAA nowCOAST';
                attribution.addTo(map);
                attribution.addAttribution('Radar: IEM');
                attribution.addAttribution('Base: OpenStreetMap, CARTO');

                setShowRadar(showRadar);
                syncOverlays();

                const layersBtn = document.getElementById('radar-map-past-layers-btn');
                const layersPanel = document.getElementById('radar-map-past-layers-panel');
                const renderLayersPanel = () => {{
                  if (!layersPanel) return;
                  const overlayItems = overlayCatalog.map((overlay) => {{
                    const checked = selectedIds.has(overlay.id) ? 'checked' : '';
                    return '<label class="radar-layers-item">' +
                      '<input type="checkbox" data-layer="' + overlay.id + '" ' + checked + ' />' +
                      overlay.label + '</label>';
                  }}).join('');
                  const radarChecked = showRadar ? 'checked' : '';
                  layersPanel.innerHTML =
                    '<div class="radar-layers-title">Layers</div>' +
                    '<label class="radar-layers-item">' +
                    '<input type="checkbox" data-role="radar" ' + radarChecked + ' />Radar imagery</label>' +
                    '<div class="radar-layers-divider"></div>' +
                    overlayItems;
                  const radarToggle = layersPanel.querySelector('input[data-role=\"radar\"]');
                  if (radarToggle) {{
                    radarToggle.disabled = selectedIds.size === 0;
                    radarToggle.addEventListener('change', (event) => {{
                      setShowRadar(event.target.checked);
                      renderLayersPanel();
                    }});
                  }}
                  layersPanel.querySelectorAll('input[data-layer]').forEach((input) => {{
                    input.addEventListener('change', (event) => {{
                      const id = event.target.getAttribute('data-layer');
                      if (event.target.checked) {{
                        selectedIds.add(id);
                      }} else {{
                        selectedIds.delete(id);
                      }}
                      syncOverlays();
                      renderLayersPanel();
                    }});
                  }});
                }};
                renderLayersPanel();
                if (layersPanel) {{
                  L.DomEvent.disableClickPropagation(layersPanel);
                  L.DomEvent.disableScrollPropagation(layersPanel);
                }}

                const fsButton = document.getElementById('radar-map-past-fullscreen');
                const fsShell = document.getElementById('radar-map-past-shell');
                const updateFsLabel = () => {{
                  const active = document.fullscreenElement === fsShell;
                  fsButton.textContent = active ? 'Exit full screen' : 'Full screen';
                  if (layersBtn) {{
                    layersBtn.style.display = active ? 'block' : 'none';
                  }}
                  if (!active && layersPanel) {{
                    layersPanel.style.display = 'none';
                  }}
                }};
                if (layersBtn) {{
                  layersBtn.addEventListener('click', () => {{
                    if (!layersPanel) return;
                    layersPanel.style.display =
                      layersPanel.style.display === 'none' ? 'block' : 'none';
                  }});
                }}
                fsButton.addEventListener('click', () => {{
                  if (document.fullscreenElement === fsShell) {{
                    document.exitFullscreen();
                  }} else if (fsShell.requestFullscreen) {{
                    fsShell.requestFullscreen();
                  }}
                }});
                document.addEventListener('fullscreenchange', () => {{
                  updateFsLabel();
                  setTimeout(() => map.invalidateSize(), 200);
                }});
                updateFsLabel();

                function applyFrame(frameIdx, force = false) {{
                  if (!frames.length) return;
                  if (frameIdx === idx && !force) {{
                    updateUi(frameIdx);
                    return;
                  }}
                  if (!showRadar || !back || !front) {{
                    updateUi(frameIdx);
                    return;
                  }}
                  if (isLoading && !force) {{
                    return;
                  }}
                  if (isLoading && force) {{
                    clearLoading();
                  }}
                  const frame = frames[frameIdx];
                  let swapped = false;
                  const onLoad = () => {{
                    if (swapped) return;
                    swapped = true;
                    back.off('load', onLoad);
                    clearLoading();
                    back.setOpacity(targetOpacity);
                    front.setOpacity(0);
                    const temp = front;
                    front = back;
                    back = temp;
                    updateUi(frameIdx);
                  }};
                  activeLoadHandler = onLoad;
                  isLoading = true;
                  back.on('load', onLoad);
                  back.setParams({{ time: frame }});
                  updateUi(frameIdx);
                  activeLoadTimeout = setTimeout(() => {{
                    if (swapped) return;
                    back.off('load', onLoad);
                    clearLoading();
                  }}, loadTimeoutMs);
                }}

                slider.addEventListener('input', (event) => {{
                  manualHoldUntil = Date.now() + manualHoldMs;
                  applyFrame(Number(event.target.value), true);
                }});

                setInterval(() => {{
                  if (!showRadar) return;
                  if (isLoading) return;
                  if (Date.now() < manualHoldUntil) return;
                  const next = (idx + 1) % frames.length;
                  applyFrame(next);
                }}, frameIntervalMs);
              }})();
            </script>
            """
            components.html(radar_html, height=410)
        with future_tab:
            forecast_html = f"""
            <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
            <div id="radar-map-forecast-shell" class="radar-map-shell">
              <button id="radar-map-forecast-fullscreen" class="radar-fullscreen-btn" type="button">Full screen</button>
              <button id="radar-map-forecast-layers-btn" class="radar-layers-btn" type="button">Layers</button>
              <div id="radar-map-forecast-layers-panel" class="radar-layers-panel" style="display:none;"></div>
              <div id="radar-map-forecast" class="radar-map"></div>
            </div>
            <div id="radar-map-forecast-label" style="margin-top: 6px; font-size: 0.85rem; color: var(--text-secondary);"></div>
            <div id="radar-map-forecast-controls" style="margin-top: 8px;">
              <div style="display:flex; justify-content:space-between; font-size:0.75rem; color:var(--text-muted); margin-bottom:4px;">
                <span id="radar-map-forecast-start">--</span>
                <span id="radar-map-forecast-end">--</span>
              </div>
              <input id="radar-map-forecast-slider" class="radar-slider" type="range" min="0" max="1" step="1" value="0" />
              <div style="margin-top:6px; height:6px; border-radius:999px; background: var(--surface-4); overflow:hidden;">
                <div id="radar-map-forecast-progress" style="height: 100%; width: 0%; background: linear-gradient(90deg, var(--accent-2-soft), var(--accent-2));"></div>
              </div>
            </div>
            <style>
              .radar-map-shell {{
                position: relative;
                height: 300px;
                width: 100%;
                border-radius: 16px;
                overflow: hidden;
                background: var(--surface-3);
              }}
              .radar-map {{
                height: 100%;
                width: 100%;
              }}
              .radar-fullscreen-btn {{
                position: absolute;
                top: 10px;
                right: 10px;
                z-index: 600;
                padding: 6px 10px;
                border-radius: 999px;
                border: 1px solid var(--border);
                background: var(--surface-3);
                color: var(--text-primary);
                font-size: 0.7rem;
                letter-spacing: 0.02em;
                cursor: pointer;
              }}
              .radar-fullscreen-btn:hover {{
                background: var(--surface-2);
              }}
              .radar-layers-btn {{
                position: absolute;
                top: 10px;
                left: 10px;
                z-index: 600;
                padding: 6px 10px;
                border-radius: 999px;
                border: 1px solid var(--border);
                background: var(--surface-3);
                color: var(--text-primary);
                font-size: 0.7rem;
                letter-spacing: 0.02em;
                cursor: pointer;
                display: none;
              }}
              .radar-layers-btn:hover {{
                background: var(--surface-2);
              }}
              .radar-layers-panel {{
                position: absolute;
                top: 44px;
                left: 10px;
                z-index: 610;
                min-width: 200px;
                max-width: 260px;
                padding: 10px;
                border-radius: 12px;
                background: var(--surface-3);
                border: 1px solid var(--border);
                color: var(--text-primary);
                font-size: 0.7rem;
                box-shadow: var(--shadow);
              }}
              .radar-layers-title {{
                text-transform: uppercase;
                letter-spacing: 0.08em;
                font-size: 0.6rem;
                color: var(--text-secondary);
                margin-bottom: 6px;
              }}
              .radar-layers-item {{
                display: flex;
                align-items: center;
                gap: 8px;
                margin: 4px 0;
              }}
              .radar-layers-item input {{
                accent-color: var(--accent-2);
              }}
              .radar-layers-divider {{
                height: 1px;
                background: var(--border);
                margin: 6px 0;
              }}
              .radar-slider {{
                width: 100%;
                height: 6px;
                border-radius: 999px;
                background: var(--surface-4);
                outline: none;
                cursor: pointer;
              }}
              .radar-slider::-webkit-slider-thumb {{
                -webkit-appearance: none;
                appearance: none;
                width: 14px;
                height: 14px;
                border-radius: 50%;
                background: var(--accent-2);
                border: 2px solid var(--surface-3);
                box-shadow: 0 2px 6px rgba(0,0,0,0.4);
              }}
              .radar-slider::-moz-range-thumb {{
                width: 14px;
                height: 14px;
                border-radius: 50%;
                background: var(--accent-2);
                border: 2px solid var(--surface-3);
                box-shadow: 0 2px 6px rgba(0,0,0,0.4);
              }}
              .radar-legend {{
                background: var(--surface-3);
                border: 1px solid var(--border);
                border-radius: 12px;
                padding: 6px 8px;
                display: grid;
                gap: 6px;
                font-size: 0.7rem;
                color: var(--text-primary);
                box-shadow: var(--shadow);
              }}
              .radar-legend-title {{
                text-transform: uppercase;
                letter-spacing: 0.08em;
                font-size: 0.6rem;
                color: var(--text-secondary);
              }}
              .radar-legend-row {{
                display: flex;
                flex-direction: column;
                align-items: flex-start;
                gap: 4px;
              }}
              .radar-legend-scale {{
                display: flex;
                flex-wrap: wrap;
                gap: 4px;
              }}
              .radar-legend-chip {{
                display: inline-flex;
                align-items: center;
                gap: 6px;
                padding: 2px 6px;
                border-radius: 6px;
                background: var(--surface-4);
                border: 1px solid var(--border);
                font-size: 0.6rem;
                color: var(--text-primary);
              }}
              .radar-legend-swatch {{
                width: 12px;
                height: 12px;
                border-radius: 4px;
                display: inline-block;
              }}
              .radar-legend-label {{
                font-size: 0.65rem;
                color: var(--text-primary);
                text-shadow: 0 1px 2px rgba(0,0,0,0.6);
              }}
              .radar-legend-img {{
                height: auto;
                width: auto;
                max-width: 300px;
                max-height: 100px;
                border-radius: 6px;
                background: var(--surface-4);
                padding: 3px 6px;
                border: 1px solid var(--border);
                display: block;
              }}
            </style>
            <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
            <script>
              (function() {{
                const map = L.map('radar-map-forecast', {{
                  zoomControl: true,
                  attributionControl: false
                }}).setView([{lat_value:.4f}, {lon_value:.4f}], 8);
                const baseMaxZoom = 18;
                const imageryMaxZoom = 12;

                const base = L.tileLayer(
                  'https://{{s}}.basemaps.cartocdn.com/dark_all/{{z}}/{{x}}/{{y}}{{r}}.png',
                  {{
                    maxZoom: baseMaxZoom,
                    subdomains: 'abcd'
                  }}
                );
                base.addTo(map);

                const wmsUrl = 'https://mesonet.agron.iastate.edu/cgi-bin/wms/hrrr/refd.cgi';
                const steps = [0, 15, 30, 45, 60, 75, 90, 105, 120];
                const layers = steps.map((min) => `refd_${{String(min).padStart(4, '0')}}`);
                const now = new Date();
                const stepMillis = 15 * 60000;
                const baseTime = new Date(Math.floor(now.getTime() / stepMillis) * stepMillis);
                const forecastTimes = steps.map((min) => new Date(baseTime.getTime() + min * 60000));
                const targetOpacity = 0.6;
                const frameIntervalMs = 1200;
                const baseParams = {{
                  format: 'image/png',
                  version: '1.3.0',
                  transparent: true
                }};
                const radarTileOptions = {{
                  maxZoom: imageryMaxZoom,
                  maxNativeZoom: imageryMaxZoom,
                  tileSize: 256,
                  keepBuffer: 2,
                  updateWhenIdle: true
                }};
                const overlayTileOptions = {{
                  maxZoom: imageryMaxZoom,
                  maxNativeZoom: imageryMaxZoom,
                  tileSize: 256,
                  keepBuffer: 2,
                  updateWhenIdle: true
                }};
                let idx = 0;
                let isLoading = false;
                let activeLoadHandler = null;
                let activeLoadTimeout = null;
                const loadTimeoutMs = 4000;
                let showRadar = {show_radar_json};
                const overlayWmsUrl = '{NOWCOAST_WMS}';
                const overlayCatalog = {overlay_catalog_json};
                const selectedIds = new Set({selected_ids_json});
                let front = null;
                let back = null;
                let legendControl = null;
                const overlayLayers = new Map();
                const clearLoading = () => {{
                  if (activeLoadTimeout) {{
                    clearTimeout(activeLoadTimeout);
                    activeLoadTimeout = null;
                  }}
                  if (activeLoadHandler && back) {{
                    back.off('load', activeLoadHandler);
                  }}
                  activeLoadHandler = null;
                  isLoading = false;
                }};

                const getSelectedOverlays = () =>
                  overlayCatalog.filter((overlay) => selectedIds.has(overlay.id));
                const hasActiveImagery = () => showRadar || getSelectedOverlays().length > 0;
                const applyImageryZoomLimits = () => {{
                  const targetMaxZoom = hasActiveImagery() ? imageryMaxZoom : baseMaxZoom;
                  map.setMaxZoom(targetMaxZoom);
                  if (map.getZoom() > targetMaxZoom) {{
                    map.setZoom(targetMaxZoom);
                  }}
                }};

                const ensureRadarPane = () => {{
                  if (!map.getPane('radarPane')) {{
                    map.createPane('radarPane');
                    map.getPane('radarPane').style.zIndex = 360;
                  }}
                }};

                const ensureOverlayPane = () => {{
                  if (!map.getPane('overlayPane')) {{
                    map.createPane('overlayPane');
                    map.getPane('overlayPane').style.zIndex = 420;
                  }}
                }};

                const initRadarLayers = () => {{
                  if (front || back) return;
                  ensureRadarPane();
                  front = L.tileLayer.wms(wmsUrl, {{
                    ...baseParams,
                    ...radarTileOptions,
                    layers: layers[0],
                    opacity: targetOpacity,
                    pane: 'radarPane'
                  }}).addTo(map);
                  back = L.tileLayer.wms(wmsUrl, {{
                    ...baseParams,
                    ...radarTileOptions,
                    layers: layers[1] || layers[0],
                    opacity: 0,
                    pane: 'radarPane'
                  }}).addTo(map);
                  front.setZIndex(200);
                  back.setZIndex(190);
                  setLayerFade(front);
                  setLayerFade(back);
                }};

                const destroyRadarLayers = () => {{
                  clearLoading();
                  if (front) map.removeLayer(front);
                  if (back) map.removeLayer(back);
                  front = null;
                  back = null;
                }};

                const setRadarUiState = () => {{
                  const label = document.getElementById('radar-map-forecast-label');
                  const controls = document.getElementById('radar-map-forecast-controls');
                  if (!showRadar) {{
                    label.textContent = selectedIds.size
                      ? 'Radar imagery hidden'
                      : 'No radar layers selected';
                    if (controls) controls.style.display = 'none';
                  }} else {{
                    if (controls) controls.style.display = '';
                  }}
                }};

                const setShowRadar = (next) => {{
                  showRadar = Boolean(next);
                  if (!selectedIds.size) {{
                    showRadar = false;
                  }}
                  if (showRadar) {{
                    initRadarLayers();
                  }} else {{
                    destroyRadarLayers();
                  }}
                  applyImageryZoomLimits();
                  setRadarUiState();
                }};

                const renderLegend = (selected) => {{
                  if (legendControl) {{
                    map.removeControl(legendControl);
                    legendControl = null;
                  }}
                  if (!selected.length) return;
                  legendControl = L.control({{ position: 'bottomleft' }});
                  legendControl.onAdd = () => {{
                    const div = L.DomUtil.create('div', 'radar-legend');
                    const legendRows = selected.map((overlay) => {{
                      let legend = '';
                      if (overlay.legend_items && overlay.legend_items.length) {{
                        const chips = overlay.legend_items.map((item) =>
                          '<span class="radar-legend-chip"><span class="radar-legend-swatch" style="background:' +
                          item.color + ';"></span>' + item.label + '</span>'
                        ).join('');
                        legend = '<div class="radar-legend-scale">' + chips + '</div>';
                      }} else if (overlay.legend) {{
                        legend = '<img class="radar-legend-img" src="' + overlay.legend + '" />';
                      }}
                      return '<div class="radar-legend-row"><span class="radar-legend-label">' +
                        overlay.label + '</span>' + legend + '</div>';
                    }}).join('');
                    div.innerHTML = '<div class="radar-legend-title">Layers</div>' + legendRows;
                    L.DomEvent.disableClickPropagation(div);
                    L.DomEvent.disableScrollPropagation(div);
                    return div;
                  }};
                  legendControl.addTo(map);
                }};

                const syncOverlays = () => {{
                  const selected = getSelectedOverlays();
                  const selectedSet = new Set(selected.map((overlay) => overlay.id));
                  if (selected.length) {{
                    ensureOverlayPane();
                  }}
                  overlayCatalog.forEach((overlay) => {{
                    if (selectedSet.has(overlay.id)) {{
                      if (!overlayLayers.has(overlay.id)) {{
                        const layer = L.tileLayer.wms(overlayWmsUrl, {{
                          layers: overlay.layer,
                          format: 'image/png',
                          version: '1.3.0',
                          transparent: true,
                          opacity: overlay.opacity || 0.5,
                          ...overlayTileOptions,
                          pane: 'overlayPane'
                        }});
                        overlayLayers.set(overlay.id, layer);
                        layer.addTo(map);
                      }}
                    }} else {{
                      const existing = overlayLayers.get(overlay.id);
                      if (existing) {{
                        map.removeLayer(existing);
                        overlayLayers.delete(overlay.id);
                      }}
                    }}
                  }});
                  if (selected.length) {{
                    attribution.addAttribution(overlaysAttribution);
                  }} else {{
                    attribution.removeAttribution(overlaysAttribution);
                  }}
                  renderLegend(selected);
                  setShowRadar(showRadar);
                }};

                const label = document.getElementById('radar-map-forecast-label');
                const progress = document.getElementById('radar-map-forecast-progress');
                const slider = document.getElementById('radar-map-forecast-slider');
                const startLabel = document.getElementById('radar-map-forecast-start');
                const endLabel = document.getElementById('radar-map-forecast-end');
                const manualHoldMs = 5000;
                let manualHoldUntil = 0;
                const formatTime = (dateObj) =>
                  dateObj.toLocaleTimeString([], {{ hour: 'numeric', minute: '2-digit' }});
                const setLabel = (frameIdx) => {{
                  const min = steps[frameIdx];
                  const hours = Math.floor(min / 60);
                  const mins = String(min % 60).padStart(2, '0');
                  const timeLabel = formatTime(forecastTimes[frameIdx]);
                  label.textContent = `Forecast frame: ${{timeLabel}} (+${{hours}}:${{mins}})`;
                }};
                const updateProgress = () => {{
                  const maxIdx = Math.max(layers.length - 1, 1);
                  const pct = (idx / maxIdx) * 100;
                  progress.style.width = `${{pct}}%`;
                }};
                const updateUi = (frameIdx) => {{
                  idx = frameIdx;
                  setLabel(frameIdx);
                  slider.value = String(frameIdx);
                  updateProgress();
                }};
                slider.max = String(Math.max(layers.length - 1, 0));
                slider.value = "0";
                startLabel.textContent = formatTime(forecastTimes[0]);
                endLabel.textContent = formatTime(forecastTimes[forecastTimes.length - 1]);
                updateUi(0);

                function setLayerFade(layer) {{
                  if (!layer) return;
                  const container = layer.getContainer();
                  if (container) {{
                    container.style.transition = 'opacity 0.6s ease';
                  }}
                }}
                setLayerFade(front);
                setLayerFade(back);

                const attribution = L.control.attribution({{ position: 'bottomright' }});
                const overlaysAttribution = 'Overlays: NOAA nowCOAST';
                attribution.addTo(map);
                attribution.addAttribution('Forecast: IEM HRRR');
                attribution.addAttribution('Base: OpenStreetMap, CARTO');

                setShowRadar(showRadar);
                syncOverlays();

                const layersBtn = document.getElementById('radar-map-forecast-layers-btn');
                const layersPanel = document.getElementById('radar-map-forecast-layers-panel');
                const renderLayersPanel = () => {{
                  if (!layersPanel) return;
                  const overlayItems = overlayCatalog.map((overlay) => {{
                    const checked = selectedIds.has(overlay.id) ? 'checked' : '';
                    return '<label class="radar-layers-item">' +
                      '<input type="checkbox" data-layer="' + overlay.id + '" ' + checked + ' />' +
                      overlay.label + '</label>';
                  }}).join('');
                  const radarChecked = showRadar ? 'checked' : '';
                  layersPanel.innerHTML =
                    '<div class="radar-layers-title">Layers</div>' +
                    '<label class="radar-layers-item">' +
                    '<input type="checkbox" data-role="radar" ' + radarChecked + ' />Radar imagery</label>' +
                    '<div class="radar-layers-divider"></div>' +
                    overlayItems;
                  const radarToggle = layersPanel.querySelector('input[data-role=\"radar\"]');
                  if (radarToggle) {{
                    radarToggle.disabled = selectedIds.size === 0;
                    radarToggle.addEventListener('change', (event) => {{
                      setShowRadar(event.target.checked);
                      renderLayersPanel();
                    }});
                  }}
                  layersPanel.querySelectorAll('input[data-layer]').forEach((input) => {{
                    input.addEventListener('change', (event) => {{
                      const id = event.target.getAttribute('data-layer');
                      if (event.target.checked) {{
                        selectedIds.add(id);
                      }} else {{
                        selectedIds.delete(id);
                      }}
                      syncOverlays();
                      renderLayersPanel();
                    }});
                  }});
                }};
                renderLayersPanel();
                if (layersPanel) {{
                  L.DomEvent.disableClickPropagation(layersPanel);
                  L.DomEvent.disableScrollPropagation(layersPanel);
                }}

                const fsButton = document.getElementById('radar-map-forecast-fullscreen');
                const fsShell = document.getElementById('radar-map-forecast-shell');
                const updateFsLabel = () => {{
                  const active = document.fullscreenElement === fsShell;
                  fsButton.textContent = active ? 'Exit full screen' : 'Full screen';
                  if (layersBtn) {{
                    layersBtn.style.display = active ? 'block' : 'none';
                  }}
                  if (!active && layersPanel) {{
                    layersPanel.style.display = 'none';
                  }}
                }};
                if (layersBtn) {{
                  layersBtn.addEventListener('click', () => {{
                    if (!layersPanel) return;
                    layersPanel.style.display =
                      layersPanel.style.display === 'none' ? 'block' : 'none';
                  }});
                }}
                fsButton.addEventListener('click', () => {{
                  if (document.fullscreenElement === fsShell) {{
                    document.exitFullscreen();
                  }} else if (fsShell.requestFullscreen) {{
                    fsShell.requestFullscreen();
                  }}
                }});
                document.addEventListener('fullscreenchange', () => {{
                  updateFsLabel();
                  setTimeout(() => map.invalidateSize(), 200);
                }});
                updateFsLabel();

                function applyLayer(frameIdx, force = false) {{
                  if (!layers.length) return;
                  if (frameIdx === idx && !force) {{
                    updateUi(frameIdx);
                    return;
                  }}
                  if (!showRadar || !back || !front) {{
                    updateUi(frameIdx);
                    return;
                  }}
                  if (isLoading && !force) {{
                    return;
                  }}
                  if (isLoading && force) {{
                    clearLoading();
                  }}
                  const layer = layers[frameIdx];
                  let swapped = false;
                  const onLoad = () => {{
                    if (swapped) return;
                    swapped = true;
                    back.off('load', onLoad);
                    clearLoading();
                    back.setOpacity(targetOpacity);
                    front.setOpacity(0);
                    const temp = front;
                    front = back;
                    back = temp;
                    updateUi(frameIdx);
                  }};
                  activeLoadHandler = onLoad;
                  isLoading = true;
                  back.on('load', onLoad);
                  back.setParams({{ layers: layer }});
                  updateUi(frameIdx);
                  activeLoadTimeout = setTimeout(() => {{
                    if (swapped) return;
                    back.off('load', onLoad);
                    clearLoading();
                  }}, loadTimeoutMs);
                }}

                slider.addEventListener('input', (event) => {{
                  manualHoldUntil = Date.now() + manualHoldMs;
                  applyLayer(Number(event.target.value), true);
                }});

                setInterval(() => {{
                  if (!showRadar) return;
                  if (isLoading) return;
                  if (Date.now() < manualHoldUntil) return;
                  const next = (idx + 1) % layers.length;
                  applyLayer(next);
                }}, frameIntervalMs);
              }})();
            </script>
            """
            components.html(forecast_html, height=410)

    if forecast_outlook is not None:
        chart_card("7-day outlook", lambda: st.altair_chart(forecast_outlook, use_container_width=True))
    else:
        st.info("No daily outlook available.")
