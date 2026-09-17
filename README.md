# QUANT_TRADE

Overnight momentum trading bot for crypto (Kraken) and US stocks (Alpaca),
built on a Kafka streaming pipeline with an optional Claude AI signal
validation layer.

## How it works

1. **`producer.py`** - streams live crypto trades from Kraken's WebSocket
   API into the Kafka topic `crypto-trades`.
2. **`stock_producer.py`** - streams live US stock trades from Alpaca's
   data WebSocket into `stock-trades` (US market hours only).
3. **`trader.py`** - consumes both topics, builds 3-minute candles, runs a
   Donchian breakout + ADX + volume + EMA strategy (`indicators.py`),
   optionally sanity-checks each signal with Claude (`ai_validator.py`),
   and executes paper trades through Alpaca.

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
docker compose up -d   # starts Kafka + Kafka UI (http://localhost:8080)
```

Create a `.env` file in the project root:

```
ALPACA_API_KEY=your_alpaca_paper_key
ALPACA_SECRET_KEY=your_alpaca_paper_secret
ANTHROPIC_API_KEY=your_anthropic_key   # optional - AI validation is skipped if unset
```

Alpaca keys must be for a **paper trading** account. `trader.py` hardcodes
`paper=True` and this project has not been tested or reviewed for live
trading.

## Running

```bash
./run.sh
```

This starts both producers and the trader together, and stops all of them
cleanly on Ctrl+C. To run pieces individually:

```bash
python producer.py    # crypto stream
python trader.py      # strategy + execution
```

## Strategy summary

- **Entry**: ADX > 25 and rising, price breaks the 15-period Donchian
  channel, volume >= 1.5x its 15-period average, EMA(9)/EMA(21) aligned
  with the breakout direction (stocks also require price on the correct
  side of VWAP).
- **Exit**: ATR-based stop (1.5x) and target (3x) for a 2:1 R:R, a
  breakeven stop once price moves 1.5x ATR in favour, and an ADX-decay
  exit once a trade has reached breakeven.
- **Risk**: 1% of capital risked per trade, max 3 concurrent positions,
  automatic slowdown at 3% drawdown and full halt at 5% drawdown, plus a
  cooldown/pause after consecutive losses.

## Known limitations

- No automated tests yet.
- Position sizing and stop/target math have not been independently
  audited. Treat this as a research project, not financial advice.
- Kraken and Alpaca are queried over their REST APIs on startup for warmup
  history; if either is unreachable the affected assets start with fewer
  candles rather than failing outright.