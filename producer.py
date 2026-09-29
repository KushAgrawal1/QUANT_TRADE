"""
producer.py — Kraken WebSocket → Kafka

Updated asset list:
  Removed: DOGE, ADA, DOT, AVAX, XRP, LINK (ATR always too small)
  Added:   BCH (~$500), AAVE (~$88), XMR (~$180)
  Kept:    BTC, ETH, SOL, LTC

Auto-reconnects on WebSocket disconnect.
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

retry_delay = 5

PAIR_TO_COIN = {
    "XBT/USD":  "BTC",
    "ETH/USD":  "ETH",
    "SOL/USD":  "SOL",
    "LTC/USD":  "LTC",
    "BCH/USD":  "BCH",
    "AAVE/USD": "AAVE",
    "XMR/USD":  "XMR",
}


def on_delivery(err, msg):
    if err:
        print(f"  Failed : {err}")


def on_open(ws):
    global retry_delay
    retry_delay = 5  # reset backoff after a successful (re)connect
    now = datetime.now(timezone.utc).strftime("%H:%M:%S")
    print(f"[{now}] Connected to Kraken WebSocket")
    ws.send(json.dumps({
        "event":        "subscribe",
        "pair":         list(PAIR_TO_COIN.keys()),
        "subscription": {"name": "trade"},
    }))


def parse_kraken_message(data) -> list:
    """Pure: one decoded Kraken WS message -> zero or more Kafka-ready
    event dicts. Non-trade messages and unrecognised shapes return an
    empty list rather than raising, and one malformed trade inside an
    otherwise-good batch is skipped individually - so one odd message
    from Kraken never has to tear down the whole connection.
    """
    if not (isinstance(data, list) and len(data) == 4 and data[2] == "trade"):
        return []

    pair = data[3]
    coin = PAIR_TO_COIN.get(pair, pair)
    events = []
    for trade in data[1]:
        try:
            events.append({
                "coin":        coin,
                "price":       float(trade[0]),
                "volume":      float(trade[1]),
                "timestamp":   trade[2],
                "side":        "buy" if trade[3] == "b" else "sell",
                "asset_class": "crypto",
            })
        except (IndexError, TypeError, ValueError) as e:
            print(f"  [SKIP] malformed trade in {pair}: {e}")
    return events


def on_message(ws, raw):
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        print(f"  [SKIP] unparseable message: {e}")
        return

    if isinstance(data, dict):
        if data.get("event") == "subscriptionStatus" and data.get("status") == "subscribed":
            print(f"  ✓ {data.get('pair', '')}")
        return

    for event in parse_kraken_message(data):
        producer.produce(
            topic=TOPIC,
            key=event["coin"],
            value=json.dumps(event).encode("utf-8"),
            callback=on_delivery,
        )
        producer.poll(0)


def on_error(ws, error):
    now = datetime.now(timezone.utc).strftime("%H:%M:%S")
    print(f"[{now}] WebSocket error: {error}")


def on_close(ws, *args):
    now = datetime.now(timezone.utc).strftime("%H:%M:%S")
    print(f"[{now}] WebSocket closed — reconnecting in 5 seconds...")
    producer.flush()

def run():
    global retry_delay
    max_delay = 60
    print("Starting Kraken stream → Kafka (auto-reconnect enabled)")
    print(f"Assets: {', '.join(PAIR_TO_COIN.values())}\n")

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

        now = datetime.now(timezone.utc).strftime("%H:%M:%S")
        print(f"[{now}] Reconnecting in {retry_delay}s...")
        time.sleep(retry_delay)
        retry_delay = min(retry_delay * 2, max_delay)


if __name__ == "__main__":
    run()