import os
import re
from datetime import datetime
from zoneinfo import ZoneInfo

import requests


def _user_agent() -> str:
    return os.getenv("NWS_USER_AGENT", "TempestWeather/1.0 (contact: unknown)")


def _fmt_time(value: str | None, tz_name: str) -> str | None:
    if not value:
        return None
    try:
        cleaned = value.replace("Z", "+00:00")
        dt = datetime.fromisoformat(cleaned)
        tz = ZoneInfo(tz_name)
        return dt.astimezone(tz).strftime("%b %d %I:%M %p").lstrip("0")
    except Exception:
        return value


def _extract_zone_id(url: str | None) -> str | None:
    if not url:
        return None
    return url.rstrip("/").split("/")[-1]


def _strip_html(text: str | None) -> str:
    if not text:
        return ""
    clean = re.sub(r"<[^>]+>", "", text)
    return " ".join(clean.split())


def resolve_alert_zones(lat: float, lon: float) -> list[str]:
    headers = {"User-Agent": _user_agent(), "Accept": "application/geo+json"}
    try:
        resp = requests.get(
            f"https://api.weather.gov/points/{lat},{lon}",
            headers=headers,
            timeout=10,
        )
        resp.raise_for_status()
        payload = resp.json()
    except Exception:
        return []
    props = payload.get("properties") if isinstance(payload, dict) else None
    if not props:
        return []
    zones = []
    forecast_zone_url = props.get("forecastZone")
    county_url = props.get("county")
    forecast_zone = _extract_zone_id(forecast_zone_url)
    county_zone = _extract_zone_id(county_url)
    if forecast_zone:
        zones.append(forecast_zone)
    if county_zone and county_zone not in zones:
        zones.append(county_zone)
    return zones


def _fetch_alerts_by_params(params: dict) -> list[dict]:
    headers = {"User-Agent": _user_agent(), "Accept": "application/geo+json"}
    try:
        resp = requests.get("https://api.weather.gov/alerts/active", params=params, headers=headers, timeout=10)
        resp.raise_for_status()
        payload = resp.json()
    except Exception:
        return []
    alerts = []
    for feature in payload.get("features", []) if isinstance(payload, dict) else []:
        props = feature.get("properties") or {}
        alert_id = props.get("id") or feature.get("id") or props.get("@id")
        event = props.get("event") or "Weather Alert"
        headline = props.get("headline") or props.get("description") or event
        severity = props.get("severity") or "Unknown"
        urgency = props.get("urgency") or "Unknown"
        area = props.get("areaDesc")
        ends = props.get("ends") or props.get("expires")
        sent = props.get("sent")
        alerts.append(
            {
                "id": alert_id,
                "event": event,
                "headline": headline,
                "severity": severity,
                "urgency": urgency,
                "area": area,
                "ends": ends,
                "sent": sent,
                "ends_local": _fmt_time(ends, params.get("timezone", "UTC")),
                "sent_local": _fmt_time(sent, params.get("timezone", "UTC")),
            }
        )
    return alerts


def fetch_hwo_text(lat: float, lon: float) -> dict | None:
    headers = {"User-Agent": _user_agent(), "Accept": "application/geo+json"}
    try:
        resp = requests.get(
            f"https://api.weather.gov/points/{lat},{lon}",
            headers=headers,
            timeout=10,
        )
        resp.raise_for_status()
        payload = resp.json()
    except Exception:
        return None
    props = payload.get("properties") if isinstance(payload, dict) else None
    if not props:
        return None
    cwa = props.get("cwa")
    if not cwa:
        return None

    products_url = f"https://api.weather.gov/products/types/HWO/locations/{cwa}"
    try:
        resp = requests.get(products_url, headers=headers, timeout=10)
        resp.raise_for_status()
        prod_payload = resp.json()
    except Exception:
        return None
    items = prod_payload.get("products") if isinstance(prod_payload, dict) else None
    if not items:
        return None
    latest = items[0]
    product_id = latest.get("id")
    issued = latest.get("issuanceTime")
    if not product_id:
        return None
    try:
        detail = requests.get(f"https://api.weather.gov/products/{product_id}", headers=headers, timeout=10)
        detail.raise_for_status()
        detail_payload = detail.json()
    except Exception:
        return None
    raw_text = detail_payload.get("productText") or ""
    return {
        "id": product_id,
        "issued": issued,
        "text": raw_text,
        "headline": latest.get("productName") or "Hazardous Weather Outlook",
        "cwa": cwa,
    }


def summarize_hwo(hwo: dict | None, max_chars: int = 320) -> str | None:
    if not hwo:
        return None
    text = _strip_html(hwo.get("text", ""))
    if not text:
        return None
    return text[:max_chars].rstrip() + ("..." if len(text) > max_chars else "")


def format_hwo_html(hwo: dict | None, tz_name: str) -> str | None:
    if not hwo:
        return None
    headline = hwo.get("headline") or "Hazardous Weather Outlook"
    issued = _fmt_time(hwo.get("issued"), tz_name) if hwo.get("issued") else None
    summary = summarize_hwo(hwo, max_chars=360)
    if not summary:
        return None
    issued_line = f"<div class=\"nws-alert-meta\">Issued {issued}</div>" if issued else ""
    return (
        "<div class=\"card status-card nws-alerts-card\">"
        f"<div class=\"section-title\">NWS Outlooks</div>"
        f"<div class=\"nws-alert\">"
        f"<div class=\"nws-alert-title\">{headline}</div>"
        f"{issued_line}"
        f"<div class=\"nws-alert-headline\">{summary}</div>"
        f"</div>"
        "</div>"
    )


def fetch_active_alerts(lat: float, lon: float, tz_name: str = "UTC") -> list[dict]:
    if lat is None or lon is None:
        return []

    zone_override = os.getenv("NWS_ZONE")
    if zone_override:
        zones = [z.strip() for z in re.split(r"[,\s]+", zone_override) if z.strip()]
        alerts = []
        for zone in zones:
            alerts.extend(_fetch_alerts_by_params({"zone": zone, "timezone": tz_name}))
        return alerts

    zones = resolve_alert_zones(lat, lon)
    if zones:
        alerts = []
        seen = set()
        for zone_id in zones:
            for alert in _fetch_alerts_by_params({"zone": zone_id, "timezone": tz_name}):
                alert_id = alert.get("id")
                if alert_id and alert_id in seen:
                    continue
                if alert_id:
                    seen.add(alert_id)
                alerts.append(alert)
        return alerts

    return _fetch_alerts_by_params({"point": f"{lat},{lon}", "timezone": tz_name})


def summarize_alerts(alerts: list[dict], tz_name: str, max_items: int = 2) -> list[str]:
    lines = []
    for alert in alerts[:max_items]:
        ends_text = alert.get("ends_local") or alert.get("ends")
        severity = alert.get("severity") or "Unknown"
        event = alert.get("event") or "Weather Alert"
        suffix = f" until {ends_text}" if ends_text else ""
        lines.append(f"{event} ({severity}){suffix}.")
    return lines


def format_alerts_html(alerts: list[dict], tz_name: str, max_items: int = 4) -> str | None:
    if not alerts:
        return None
    items = []
    for alert in alerts[:max_items]:
        ends_text = alert.get("ends_local") or alert.get("ends")
        meta_bits = [alert.get("severity"), alert.get("urgency")]
        if ends_text:
            meta_bits.append(f"Until {ends_text}")
        meta = " - ".join(bit for bit in meta_bits if bit)
        items.append(
            f"<div class=\"nws-alert\">"
            f"<div class=\"nws-alert-title\">{alert.get('event','Weather Alert')}</div>"
            f"<div class=\"nws-alert-meta\">{meta}</div>"
            f"<div class=\"nws-alert-headline\">{alert.get('headline','')}</div>"
            f"</div>"
        )
    return (
        "<div class=\"card status-card nws-alerts-card\">"
        "<div class=\"section-title\">NWS Alerts</div>"
        + "".join(items)
        + "</div>"
    )
