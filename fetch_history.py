"""
fetch_history.py - Pull historical OHLC candles into ./data/ for backtesting.

Kraken's public OHLC endpoint returns roughly the last 720 bars per call.
At 3-minute resolution that is about 36 hours; at 15-minute about 7.5 days.
Longer resolutions give you more calendar time, which matters because a
backtest over 36 hours of one asset tells you essentially nothing.

    python fetch_history.py                 # 3-minute bars (matches live)
    python fetch_history.py --interval 15   # 15-minute bars, ~7.5 days
    python fetch_history.py --interval 60   # hourly, ~30 days

Run the longer intervals too. A strategy that only works on one timeframe
and one week of data is a coincidence, not an edge.
"""

import os
import csv
import time
import argparse
import requests

import strategy as st

DATA_DIR = "data"

KRAKEN_PAIRS = {
    "BTC":  "XBTUSD",
    "ETH":  "ETHUSD",
    "SOL":  "SOLUSD",
    "LTC":  "LTCUSD",
    "BCH":  "BCHUSD",
    "AAVE": "AAVEUSD",
    "XMR":  "XMRUSD",
}

# Kraken only accepts these interval values (minutes).
NATIVE_INTERVALS = {1, 5, 15, 30, 60, 240, 1440}


def fetch(pair: str, interval: int) -> list:
    """Fetch OHLC bars, aggregating from 1-minute when needed."""
    base = interval if interval in NATIVE_INTERVALS else 1
    group = 1 if base == interval else interval

    resp = requests.get(
        "https://api.kraken.com/0/public/OHLC",
        params={"pair": pair, "interval": base},
        timeout=15,
    )
    data = resp.json()
    if data.get("error"):
        raise RuntimeError(data["error"])

    key = [k for k in data["result"] if k != "last"][0]
    raw = data["result"][key][:-1]   # drop the in-progress bar

    bars = [{
        "open":   float(r[1]),
        "high":   float(r[2]),
        "low":    float(r[3]),
        "close":  float(r[4]),
        "volume": float(r[6]),
    } for r in raw]

    if group == 1:
        return bars

    out = []
    for i in range(0, len(bars) - group + 1, group):
        chunk = bars[i:i + group]
        out.append({
            "open":   chunk[0]["open"],
            "high":   max(c["high"] for c in chunk),
            "low":    min(c["low"] for c in chunk),
            "close":  chunk[-1]["close"],
            "volume": sum(c["volume"] for c in chunk),
        })
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--interval", type=int, default=3,
                    help="bar size in minutes (default 3, matches live)")
    args = ap.parse_args()

    os.makedirs(DATA_DIR, exist_ok=True)
    print(f"Fetching {args.interval}-minute candles from Kraken\n")

    for asset in st.CRYPTO_ASSETS:
        pair = KRAKEN_PAIRS.get(asset)
        if not pair:
            continue
        try:
            bars = fetch(pair, args.interval)
            path = os.path.join(DATA_DIR, f"{asset}.csv")
            with open(path, "w", newline="") as f:
                w = csv.DictWriter(
                    f, fieldnames=["open", "high", "low", "close", "volume"]
                )
                w.writeheader()
                w.writerows(bars)

            # Report ATR as a percentage of price - the number that decides
            # whether this instrument can clear transaction costs at all.
            import indicators as ind
            highs  = [b["high"] for b in bars]
            lows   = [b["low"] for b in bars]
            closes = [b["close"] for b in bars]
            atr    = ind.atr(highs, lows, closes)
            px     = closes[-1]
            atr_pct = (atr / px * 100) if atr and px else 0

            print(f"  {asset:<5} {len(bars):>4} bars | ${px:>12,.2f} | "
                  f"ATR {atr_pct:>5.3f}% of price"
                  f"{'   <-- below 0.40% cost floor' if atr_pct < 0.40 else ''}")
            time.sleep(1.5)   # be polite to Kraken's rate limiter
        except Exception as e:
            print(f"  {asset:<5} failed: {e}")

    print(f"\nSaved to {DATA_DIR}/. Now run: python backtest.py")


if __name__ == "__main__":
    main()
