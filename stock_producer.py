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

import json
import os
import time

from confluent_kafka import Producer
from dotenv import load_dotenv

TOPIC  = "stock-trades"
STOCKS = ["AAPL", "NVDA", "TSLA", "AMZN"]


def build_stock_event(data) -> dict:
    """Pure: Alpaca's trade object -> the dict we publish to Kafka.

    No I/O and no import-time side effects, so this is testable without a
    live stream, real credentials, or a Kafka broker.
    """
    return {
        "asset":       data.symbol,
        "price":       float(data.price),
        "volume":      float(data.size),
        "timestamp":   str(data.timestamp),
        "asset_class": "stock",
    }


def main():
    """Everything that touches the network lives in here, not at module
    scope - importing this file (e.g. from a test, or a future launcher
    script) must never open a live Alpaca stream or a Kafka connection.
    """
    from alpaca.data.live import StockDataStream

    load_dotenv()

    kafka_producer = Producer({
        "bootstrap.servers": "localhost:29092",
        "acks": "all",
        "retries": 3,
    })

    def on_delivery(err, msg):
        if err:
            print(f"  Failed : {err}")
        else:
            print(
                f"  Sent   : {msg.key().decode():<5} | "
                f"P{msg.partition()} | offset={msg.offset()}"
            )

    stream = StockDataStream(
        os.getenv("ALPACA_API_KEY"),
        os.getenv("ALPACA_SECRET_KEY"),
    )

    async def handle_trade(data):
        try:
            event = build_stock_event(data)
        except (AttributeError, TypeError, ValueError) as e:
            print(f"  [SKIP] malformed trade payload: {e}")
            return

        print(
            f"  Trade  : {event['asset']:<5} | "
            f"${event['price']:>8,.2f} | vol={event['volume']}"
        )

        try:
            kafka_producer.produce(
                topic=TOPIC,
                key=event["asset"],
                value=json.dumps(event).encode("utf-8"),
                callback=on_delivery,
            )
            kafka_producer.poll(0)
        except BufferError as e:
            # librdkafka's local queue is full - drop this tick rather
            # than letting it crash the whole Alpaca stream over what is
            # usually just a transient backlog.
            print(f"  [KAFKA] local queue full, dropping tick: {e}")

    stream.subscribe_trades(handle_trade, *STOCKS)

    print(f"Starting stock stream: {', '.join(STOCKS)}")
    print("Note: Active only during US market hours (Mon-Fri 9:30 AM - 4:00 PM ET)\n")

    while True:
        try:
            stream.run()
        except Exception as e:
            print(f"Stream error: {e} — reconnecting in 10s")
            time.sleep(10)


if __name__ == "__main__":
    main()
