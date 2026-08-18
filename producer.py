"""
producer.py — Kraken WebSocket → Kafka (10 crypto pairs)

More pairs = more breakout opportunities running in parallel.
Lower-priced altcoins (XRP, ADA, DOGE) generate larger position
sizes relative to our $10k notional cap → more profit per win.

Topic: crypto-trades (10 partitions)
"""

import json
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
    print("Connected to Kraken WebSocket\n")
    ws.send(json.dumps({
        "event":        "subscribe",
        "pair":         list(PAIR_TO_COIN.keys()),
        "subscription": {"name": "trade"},
    }))
    print(f"Subscribed to: {', '.join(PAIR_TO_COIN.values())}\n")


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
    print(f"\nWebSocket error: {error}")


def on_close(ws, *args):
    producer.flush()
    print("\nConnection closed.")


if __name__ == "__main__":
    print("Starting 10-coin Kraken stream → Kafka\n")
    ws = websocket.WebSocketApp(
        "wss://ws.kraken.com",
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )
    ws.run_forever()