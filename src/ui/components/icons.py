ICONS = {
    "home": "H",
    "trends": "T",
    "compare": "C",
    "data": "D",
    "temp": "T",
    "aqi": "AQ",
    "wind": "W",
    "forecast": "FC",
    "brief": "BR",
    "filters": "F",
}


def icon(name: str) -> str:
    return ICONS.get(name, "?")
