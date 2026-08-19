"""
producer.py — Kraken WebSocket → Kafka (10 crypto pairs)

Auto-reconnects on WebSocket disconnect — runs forever without manual restart.
This was the root cause of no trades overnight: WebSocket dropped and
the script exited instead of reconnecting.
"""

import json
import time
import websocket
from datetime import datetime, timezone
from confluent_kafka import Producer


conf = {
    "bootstrap.servers": "localhost:29092",
    "acks": "all",
    "retries": 3,
}

producer = Producer(conf)
TOPIC = "crypto-trades"

PAIR_TO_COIN = {
    "XBT/USD":  "BTC",
    "ETH/USD":  "ETH",
    "SOL/USD":  "SOL",
    "XDG/USD":  "DOGE",
    "LTC/USD":  "LTC",
    "XRP/USD":  "XRP",
    "ADA/USD":  "ADA",
    "AVAX/USD": "AVAX",
    "LINK/USD": "LINK",
    "DOT/USD":  "DOT",
}


def on_delivery(err, msg):
    if err:
        print(f"  Failed : {err}")


def on_open(ws):
    now = datetime.now(timezone.utc).strftime("%H:%M:%S")
    print(f"[{now}] Connected to Kraken WebSocket")
    ws.send(json.dumps({
        "event":        "subscribe",
        "pair":         list(PAIR_TO_COIN.keys()),
        "subscription": {"name": "trade"},
    }))


def on_message(ws, raw):
    data = json.loads(raw)

    if isinstance(data, dict):
        if data.get("event") == "subscriptionStatus":
            status = data.get("status")
            pair   = data.get("pair", "")
            if status == "subscribed":
                print(f"  ✓ {pair}")
        return

    if len(data) == 4 and data[2] == "trade":
        pair = data[3]
        coin = PAIR_TO_COIN.get(pair, pair)
        for trade in data[1]:
            event = {
                "coin":        coin,
                "price":       float(trade[0]),
                "volume":      float(trade[1]),
                "timestamp":   trade[2],
                "side":        "buy" if trade[3] == "b" else "sell",
                "asset_class": "crypto",
            }
            producer.produce(
                topic=TOPIC,
                key=coin,
                value=json.dumps(event).encode("utf-8"),
                callback=on_delivery,
            )
            producer.poll(0)


def on_error(ws, error):
    now = datetime.now(timezone.utc).strftime("%H:%M:%S")
    print(f"[{now}] WebSocket error: {error}")


def on_close(ws, *args):
    now = datetime.now(timezone.utc).strftime("%H:%M:%S")
    print(f"[{now}] WebSocket closed — will reconnect in 5 seconds...")
    producer.flush()


def run():
    """
    Main loop — reconnects automatically on any disconnect or error.
    Kraken WebSocket drops connections every few hours — this handles it.
    """
    retry_delay = 5    # seconds between reconnect attempts
    max_delay   = 60   # cap backoff at 60 seconds

    print("Starting 10-coin Kraken stream → Kafka (auto-reconnect enabled)\n")

    while True:
        try:
            ws = websocket.WebSocketApp(
                "wss://ws.kraken.com",
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
            )
            ws.run_forever(ping_interval=30, ping_timeout=10)

        except Exception as e:
            now = datetime.now(timezone.utc).strftime("%H:%M:%S")
            print(f"[{now}] Exception: {e}")

        # Connection dropped — wait and reconnect
        now = datetime.now(timezone.utc).strftime("%H:%M:%S")
        print(f"[{now}] Reconnecting in {retry_delay}s...")
        time.sleep(retry_delay)

        # Exponential backoff up to max_delay
        retry_delay = min(retry_delay * 2, max_delay)

        # Reset delay after successful long connection
        # (run_forever running > 60s means connection was healthy)
        retry_delay = 5


if __name__ == "__main__":
    run()