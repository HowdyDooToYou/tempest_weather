import hashlib
import html
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


def _strip_html_preserve_lines(text: str | None) -> str:
    if not text:
        return ""
    cleaned = re.sub(r"(?i)<br\\s*/?>", "\n", text)
    cleaned = re.sub(r"(?i)</p>", "\n", cleaned)
    cleaned = re.sub(r"<[^>]+>", "", cleaned)
    lines = []
    for line in cleaned.splitlines():
        line = " ".join(line.split())
        if line:
            lines.append(line)
    return "\n".join(lines)


def _format_hwo_full_html(text: str) -> str:
    escaped = html.escape(text)
    return escaped.replace("\n", "<br>")


def _strip_afd_header(text: str) -> str:
    lines = []
    started = False
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("$$"):
            break
        if line.startswith("&&"):
            continue
        if line.startswith("PRELIMINARY POINT TEMPS/POPS"):
            break
        if line.startswith(".") or re.match(r"^(UPDATE|SYNOPSIS|NEAR TERM|SHORT TERM|LONG TERM)\b", line, re.I):
            started = True
        if started:
            lines.append(line)
    return "\n".join(lines)


def _extract_afd_sections(text: str) -> dict[str, list[str]]:
    sections: dict[str, list[str]] = {}
    current = None
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("$$") or line.startswith("&&"):
            continue
        header = None
        if line.startswith("."):
            header = line.lstrip(".").split("...")[0].strip()
        elif re.match(
            r"^(UPDATE|SYNOPSIS|NEAR TERM|SHORT TERM|LONG TERM|AVIATION|MARINE|FIRE WEATHER|HYDROLOGY)\b",
            line,
            re.I,
        ):
            header = line.split("...")[0].strip()
        if header:
            header = header.split("/", 1)[0].strip().upper()
            current = header
            sections.setdefault(current, [])
            continue
        if current:
            sections[current].append(line)
    return sections


def _split_sentences(text: str) -> list[str]:
    parts = re.split(r"(?<=[.!?])\s+", text)
    return [part.strip() for part in parts if part.strip()]


def fetch_afd_text(lat: float, lon: float) -> dict | None:
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
    products_url = f"https://api.weather.gov/products/types/AFD/locations/{cwa}"
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
        "headline": latest.get("productName") or "Area Forecast Discussion",
        "cwa": cwa,
    }


def summarize_afd(
    afd: dict | None,
    max_items: int = 4,
    max_chars: int = 700,
    per_item_max: int = 220,
) -> list[str] | None:
    if not afd:
        return None
    text = _strip_html_preserve_lines(afd.get("text", ""))
    if not text:
        return None
    cleaned = _strip_afd_header(text)
    if not cleaned:
        cleaned = text
    sections = _extract_afd_sections(cleaned)
    if not sections:
        sections = {"GENERAL": cleaned.splitlines()}
    preferred = ["UPDATE", "SYNOPSIS", "NEAR TERM", "SHORT TERM", "LONG TERM"]
    keywords = (
        "rain",
        "shower",
        "storm",
        "thunder",
        "severe",
        "flood",
        "front",
        "cold",
        "warm",
        "wind",
        "gust",
        "snow",
        "ice",
        "fog",
        "heat",
        "temperature",
        "humid",
        "dry",
        "risk",
        "confidence",
        "uncertainty",
        "timing",
    )
    highlights = []
    seen = set()

    def add_highlight(sentence: str):
        sentence = " ".join(sentence.split())
        if not sentence:
            return
        if len(sentence) > per_item_max:
            sentence = sentence[:per_item_max].rstrip() + "..."
        if sentence in seen:
            return
        seen.add(sentence)
        highlights.append(sentence)

    for label in preferred:
        lines = sections.get(label)
        if not lines:
            continue
        block = " ".join(lines)
        for sentence in _split_sentences(block):
            if len(highlights) >= max_items:
                break
            lowered = sentence.lower()
            if any(keyword in lowered for keyword in keywords):
                add_highlight(sentence)
        if len(highlights) < max_items:
            sentences = _split_sentences(block)
            if sentences:
                add_highlight(sentences[0])
        if len(highlights) >= max_items:
            break

    if not highlights:
        fallback = " ".join(cleaned.split())
        if not fallback:
            return None
        return [fallback[:max_chars].rstrip() + ("..." if len(fallback) > max_chars else "")]

    trimmed = []
    total = 0
    for item in highlights:
        if total + len(item) > max_chars and trimmed:
            break
        trimmed.append(item)
        total += len(item) + 1
    return trimmed


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


def _extract_pre_text(html_text: str) -> str | None:
    match = re.search(r"<pre[^>]*>(.*?)</pre>", html_text, re.S | re.I)
    if not match:
        return None
    return match.group(1).strip()


def _parse_hwo_issued(text: str) -> str | None:
    for line in text.splitlines():
        line = line.strip()
        if re.search(r"\b\d{3,4}\s[AP]M\s[A-Z]{3}\s\w{3}\s\w{3}\s\d{1,2}\s\d{4}\b", line):
            return line
    return None


def _fetch_hwo_fallback(
    lat: float,
    lon: float,
    forecast_zone: str | None,
    county_zone: str | None,
    cwa: str | None,
) -> dict | None:
    headers = {"User-Agent": _user_agent(), "Accept": "text/html"}
    params = {
        "warnzone": forecast_zone or "",
        "warncounty": county_zone or "",
        "firewxzone": forecast_zone or "",
        "local_place1": "",
        "product1": "Hazardous Weather Outlook",
        "lat": f"{lat:.3f}",
        "lon": f"{lon:.3f}",
    }
    try:
        resp = requests.get(
            "https://forecast.weather.gov/showsigwx.php",
            headers=headers,
            params=params,
            timeout=10,
        )
        resp.raise_for_status()
        text = _extract_pre_text(resp.text or "")
    except Exception:
        return None
    if not text:
        return None
    issued = _parse_hwo_issued(text)
    digest = hashlib.md5(text.encode("utf-8")).hexdigest()
    product_id = f"hwo-{cwa or forecast_zone or 'zone'}-{digest[:10]}"
    return {
        "id": product_id,
        "issued": issued,
        "text": text,
        "headline": "Hazardous Weather Outlook",
        "cwa": cwa,
        "source": "forecast.weather.gov",
    }


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
    forecast_zone = _extract_zone_id(props.get("forecastZone"))
    county_zone = _extract_zone_id(props.get("county"))
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
        return _fetch_hwo_fallback(lat, lon, forecast_zone, county_zone, cwa)
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
    text = _strip_html_preserve_lines(hwo.get("text", ""))
    if not text:
        return None
    sections = _extract_hwo_sections(text)
    if not sections:
        clean = " ".join(text.split())
        return clean[:max_chars].rstrip() + ("..." if len(clean) > max_chars else "")
    summary_bits = []
    for label, lines in sections:
        if not lines:
            continue
        block = " ".join(line.strip() for line in lines if line.strip())
        if not block:
            continue
        summary_bits.append(f"{label}: {block}")
    if not summary_bits:
        return None
    summary = " ".join(summary_bits)
    summary = " ".join(summary.split())
    return summary[:max_chars].rstrip() + ("..." if len(summary) > max_chars else "")


def _extract_hwo_sections(text: str) -> list[tuple[str, list[str]]]:
    sections = []
    current_label = None
    current_lines: list[str] = []
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("$$"):
            break
        if line.startswith("."):
            if current_label and current_lines:
                sections.append((current_label, current_lines))
            if line.startswith(".DAY ONE"):
                current_label = "Today"
            elif line.startswith(".DAYS TWO"):
                current_label = "Next 7 days"
            elif line.startswith(".SPOTTER"):
                current_label = "Spotter info"
            else:
                current_label = "Outlook"
            current_lines = []
            continue
        if current_label:
            current_lines.append(line)
    if current_label and current_lines:
        sections.append((current_label, current_lines))
    return sections


def format_hwo_html(hwo: dict | None, tz_name: str) -> str | None:
    if not hwo:
        return None
    headline = hwo.get("headline") or "Hazardous Weather Outlook"
    issued = _fmt_time(hwo.get("issued"), tz_name) if hwo.get("issued") else None
    summary = summarize_hwo(hwo, max_chars=260)
    if not summary:
        return None
    issued_line = f"<div class=\"nws-alert-meta\">Issued {issued}</div>" if issued else ""
    full_text = _strip_html_preserve_lines(hwo.get("text", ""))
    details_html = ""
    if full_text:
        details_html = (
            "<details class=\"nws-alert-details\">"
            "<summary>Read full outlook</summary>"
            f"<div class=\"nws-alert-full\">{_format_hwo_full_html(full_text)}</div>"
            "</details>"
        )
    return (
        "<div class=\"card status-card nws-alerts-card\">"
        f"<div class=\"section-title\">NWS Outlooks</div>"
        f"<div class=\"nws-alert\">"
        f"<div class=\"nws-alert-title\">{headline}</div>"
        f"{issued_line}"
        f"<div class=\"nws-alert-headline\">{summary}</div>"
        f"{details_html}"
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
