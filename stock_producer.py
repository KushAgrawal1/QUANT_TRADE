"""
stock_producer.py — Alpaca Live Stock Stream → Kafka

Streams real trades for 4 FTSE/US stocks via Alpaca's data WebSocket.

Topic: stock-trades (4 partitions)
  P0 → AAPL
  P1 → NVDA
  P2 → TSLA
  P3 → AMZN

NOTE: Only produces data during US market hours
      Monday - Friday, 9:30 AM - 4:00 PM Eastern Time.
      Run this alongside producer.py — they write to different topics.
"""

import os
import json
import time
from dotenv import load_dotenv
from confluent_kafka import Producer
from alpaca.data.live import StockDataStream


load_dotenv()


# ── Kafka ─────────────────────────────────────────────────────────────────────

kafka_producer = Producer({
    "bootstrap.servers": "localhost:29092",
    "acks": "all",
    "retries": 3,
})

TOPIC  = "stock-trades"
STOCKS = ["AAPL", "NVDA", "TSLA", "AMZN"]


def on_delivery(err, msg):
    if err:
        print(f"  Failed : {err}")
    else:
        print(
            f"  Sent   : {msg.key().decode():<5} | "
            f"P{msg.partition()} | offset={msg.offset()}"
        )


# ── Alpaca Stream ─────────────────────────────────────────────────────────────

stream = StockDataStream(
    os.getenv("ALPACA_API_KEY"),
    os.getenv("ALPACA_SECRET_KEY"),
)


async def handle_trade(data):
    event = {
        "asset":       data.symbol,
        "price":       float(data.price),
        "volume":      float(data.size),
        "timestamp":   str(data.timestamp),
        "asset_class": "stock",
    }

    print(
        f"  Trade  : {data.symbol:<5} | "
        f"${float(data.price):>8,.2f} | "
        f"vol={float(data.size)}"
    )

    kafka_producer.produce(
        topic=TOPIC,
        key=data.symbol,
        value=json.dumps(event).encode("utf-8"),
        callback=on_delivery,
    )
    kafka_producer.poll(0)


# ── Run ───────────────────────────────────────────────────────────────────────

stream.subscribe_trades(handle_trade, *STOCKS)

print(f"Starting stock stream: {', '.join(STOCKS)}")
print("Note: Active only during US market hours (Mon-Fri 9:30 AM - 4:00 PM ET)\n")

while True:
    try:
        stream.run()
    except Exception as e:
        print(f"Stream error: {e} — reconnecting in 10s")
        time.sleep(10)