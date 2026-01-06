import json
import os
import time
import traceback
import websocket

print("SCRIPT STARTED")

TOKEN = os.getenv("TEMPEST_API_TOKEN")
print("TOKEN PRESENT:", bool(TOKEN))
if not TOKEN:
    raise RuntimeError("TEMPEST_API_TOKEN is not set")

WS_URL = f"wss://ws.weatherflow.com/swd/data?token={TOKEN}"
DEVICE_ID = 475329  # Tempest station device (ST-00150566)

def main():
    try:
        print("CONNECTING...")
        ws = websocket.WebSocket()
        ws.connect(WS_URL, timeout=15)
        print("CONNECTED")

        listen_msg = {"type": "listen_start", "device_id": DEVICE_ID, "id": "listen_1"}
        ws.send(json.dumps(listen_msg))
        print("SENT listen_start:", listen_msg)

        print("LISTENING (30s)...")
        start = time.time()
        while time.time() - start < 30:
            msg = ws.recv()
            print("RECV:", msg)

        ws.close()
        print("CLOSED")
    except Exception as e:
        print("ERROR:", repr(e))
        traceback.print_exc()

if __name__ == "__main__":
    main()
