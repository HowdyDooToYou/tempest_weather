import json
import os
import urllib.request
from datetime import datetime, timezone

HOST = os.environ.get("DAVIS_AIRLINK_HOST", "").rstrip("/")
URL = f"{HOST}/v1/current_conditions"

def fetch(url: str) -> dict:
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=8) as r:
        raw = r.read().decode("utf-8", errors="replace")
    return json.loads(raw)

def main() -> int:
    if not HOST:
        print("ERROR: DAVIS_AIRLINK_HOST not set (e.g. http://192.168.1.19)")
        return 2

    data = fetch(URL)
    d = data.get("data", {})
    did = d.get("did")
    ts = d.get("ts")
    name = d.get("name")

    print(f"HOST: {HOST}")
    print(f"DID: {did}  NAME: {name}  TS: {ts}")
    if isinstance(ts, (int, float)):
        dt = datetime.fromtimestamp(ts, tz=timezone.utc).astimezone()
        print(f"TS local: {dt.isoformat(timespec='seconds')}")

    print("\nTop-level data keys:", sorted(d.keys()))

    conditions = d.get("conditions") or []
    if conditions:
        c0 = conditions[0]
        print("\nconditions[0] keys:", sorted(c0.keys()))
        sample_keys = [
            "temp", "hum", "dew_point", "wet_bulb", "heat_index",
            "pm_1", "pm_2p5", "pm_10",
            "pm_2p5_last_1_hour", "pm_2p5_last_3_hours", "pm_2p5_last_24_hours",
            "aqi", "aqi_nowcast"
        ]
        sample = {k: c0.get(k) for k in sample_keys if k in c0}
        print("\nSample fields:", json.dumps(sample, indent=2))

    return 0

if __name__ == "__main__":
    main()
