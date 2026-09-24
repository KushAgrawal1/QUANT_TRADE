# QUANT_TRADE

[![CI](https://github.com/KushAgrawal1/QUANT_TRADE/actions/workflows/ci.yml/badge.svg)](https://github.com/KushAgrawal1/QUANT_TRADE/actions/workflows/ci.yml)

Overnight momentum trading bot for crypto (Kraken) and US stocks (Alpaca),
built on a Kafka streaming pipeline with an optional Claude AI signal
validation layer.

## How it works

1. **`producer.py`** - streams live crypto trades from Kraken's WebSocket
   API into the Kafka topic `crypto-trades`.
2. **`stock_producer.py`** - streams live US stock trades from Alpaca's
   data WebSocket into `stock-trades` (US market hours only).
3. **`trader.py`** - consumes both topics, builds 3-minute candles, runs
   the strategy, optionally sanity-checks each signal with Claude
   (`ai_validator.py`), and executes paper trades through Alpaca.

The trading logic is split into pure modules with no network or Kafka
dependency, so it can be tested and backtested:

- **`indicators.py`** - EMA, RSI, ATR, ADX, Donchian channel, volume ratio.
- **`strategy.py`** - the signal engine and all strategy parameters. The live
  trader and the backtester both import this, so they cannot drift apart.
- **`costs.py`** - fee and slippage model, plus a pre-trade gate that rejects
  trades whose reward:risk does not survive transaction costs.
- **`backtest.py`** / **`fetch_history.py`** - historical replay (see below).

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
docker compose up -d   # starts Kafka + Kafka UI (http://localhost:8080)
```

Copy `.env.example` to `.env` and fill it in:

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

## Backtesting

```bash
python fetch_history.py               # 3-minute bars, matches live (~36h)
python fetch_history.py --interval 15 # ~7.5 days of 15-minute bars
python backtest.py                    # run with costs and the edge gate on
python backtest.py --no-costs         # the fantasy version, for comparison
python backtest.py --grid             # sweep stop/target multiples
```

The backtester is deliberately pessimistic:

- Signals use closed candles and fill at the **next** candle's open, never the
  signal candle's close (that would be look-ahead bias).
- If one bar touches both stop and target, the **stop** is assumed to hit first.
- A stop that is gapped through fills at the open, not at the stop price.
- Every fill pays fees and crosses the spread (`costs.py`).
- Trades from all assets are merged by exit time before computing the
  combined drawdown.

Treat a grid-sweep winner with suspicion: picking the best cell of a sweep is
overfitting unless it holds on data you did not sweep over.

## Development

```bash
pip install -r requirements-dev.txt
ruff check .
pytest
```

The tests cover the indicators, cost model, signal engine, backtester, history
aggregation and AI validator (using a fake Anthropic client, so no network or
API key is needed). CI runs both commands on every push and pull request.

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

- `trader.py`'s execution, portfolio and Kafka/Alpaca code paths have no
  automated tests yet; only the pure strategy, cost and backtest logic is
  covered.
- The backtester covers crypto only and Kraken returns limited history per
  call, so a single run says little about long-run performance.
- Position sizing and stop/target math have not been independently
  audited. Treat this as a research project, not financial advice.
- Kraken and Alpaca are queried over their REST APIs on startup for warmup
  history; if either is unreachable the affected assets start with fewer
  candles rather than failing outright.