"""
trader.py — Final Momentum Breakout System

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
RESEARCH BASIS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Donchian Channel Breakout (BTC 2022-2026): +36.5% returns,
36% win rate — profits entirely from large winners.
— CoinQuant Backtests (2026)

Adding volume confirmation: win rate 52% → 58%
Adding ADX>25 filter on top: win rate → 63%
— arxum.com comprehensive backtests (2026)

ADX>25 filter improves win rate by 25% across all assets.
— Fazen Capital Research (2026)

Using ADX for EXITS (not just entries): RET/DD 3.42 → 6.12
"Same market. Same core entry. Smarter exit logic."
— Alpha Algo Trading Research (2026)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
STRATEGY SUMMARY
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Candles : 3-minute OHLCV (from ticks) — better than 1-min
Assets  : 10 crypto (24/7) + 4 stocks (market hours)

ENTRY — 4 conditions ALL required:
  1. ADX > 25 AND rising     → market is trending (not choppy)
  2. Price > 15-candle high  → Donchian breakout above resistance
  3. Volume > 1.5× average   → institutional backing
  4. EMA(9) > EMA(21)        → short-term uptrend confirmed
  + For stocks only: price > VWAP (institutional flow bullish)

EXIT — whichever hits first:
  A. Profit target: +3.0 × ATR (2:1 R:R minimum)
  B. Stop loss:     -1.5 × ATR
  C. Breakeven:     stop moves to entry after +1.5 ATR profit
  D. ADX exit:      ADX drops below 20 while at breakeven (trend dying)

SHORTS (stocks only — Alpaca paper supports it):
  Same 4 conditions but inverted: breakdown below 15-candle LOW

SIZING:
  Risk 1% of capital per trade, stop at 1.5×ATR
  Hard cap: $10,000 notional per trade
  Always floor qty to 4dp (never round up — avoids Alpaca errors)

PORTFOLIO LIMITS:
  Max 3 concurrent positions
  5% drawdown → full halt
  3-candle cooldown per asset after each trade

MATH:
  With 40% win rate, 2:1 R:R:
  EV = (0.40 × 3 ATR) − (0.60 × 1.5 ATR) = +0.3 ATR per trade
  Positive expected value even losing 60% of trades.

  With 63% win rate (ADX-filtered, research-backed):
  EV = (0.63 × 3 ATR) − (0.37 × 1.5 ATR) = +1.335 ATR per trade
  Much stronger positive expectancy.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
WARMUP: 25 × 3-minute candles = 75 minutes
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import os
import json
from collections import deque
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
from confluent_kafka import Consumer, KafkaError
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce
import indicators as ind

load_dotenv()


# ── Config ────────────────────────────────────────────────────────────────────

KAFKA_CONF = {
    "bootstrap.servers": "localhost:29092",
    "group.id":          "final-momentum-trader",
    "auto.offset.reset": "latest",
    "enable.auto.commit": False,
}

TOPICS = ["crypto-trades", "stock-trades"]

CRYPTO_ASSETS = ["BTC", "ETH", "SOL", "DOGE", "LTC", "XRP", "ADA", "AVAX", "LINK", "DOT"]
STOCK_ASSETS  = ["AAPL", "NVDA", "TSLA", "AMZN"]
ALL_ASSETS    = CRYPTO_ASSETS + STOCK_ASSETS

CANDLE_MINUTES  = 3       # 3-min candles — better quality than 1-min
CANDLE_WINDOW   = 80      # candles stored per asset
CANDLES_NEEDED  = 25      # warmup = 25 × 3min = 75 minutes

DONCHIAN_PERIOD  = 15     # 15-candle high/low = 45-minute range
VOLUME_PERIOD    = 15     # volume average period
VOLUME_THRESHOLD = 1.5    # 150% of average = confirmed institutional move

ADX_THRESHOLD  = 25       # ADX must be above 25 (strong trend)
ADX_EXIT_LEVEL = 20       # exit early if ADX drops below 20 while profitable

STOP_ATR_MULT   = 1.5
TARGET_ATR_MULT = 3.0

RISK_PER_TRADE  = 0.01
MAX_NOTIONAL    = 10_000.0   # max $10k per trade
MAX_POSITIONS   = 3
MAX_DRAWDOWN    = 0.05
INITIAL_CAPITAL = 100_000.0
CANDLE_COOLDOWN = 3


def to_alpaca_symbol(asset: str) -> str:
    return f"{asset}/USD" if asset in CRYPTO_ASSETS else asset


def is_us_market_hours() -> bool:
    """9:45 AM - 3:30 PM Eastern Time, Mon-Fri."""
    utc  = datetime.now(timezone.utc)
    ny   = utc + timedelta(hours=-4)   # EDT (summer)
    if ny.weekday() >= 5:
        return False
    return ny.replace(hour=9, minute=45) <= ny <= ny.replace(hour=15, minute=30)


def floor_qty(qty: float) -> float:
    """Always floor to 4dp — never round up. Prevents 'insufficient balance' errors."""
    return int(qty * 10000) / 10000


# ── 3-Minute Candle Builder ───────────────────────────────────────────────────

class CandleBuilder:
    """
    Aggregates ticks into 3-minute OHLCV candles.
    3-min chosen over 1-min because research shows:
    - Higher timeframe = more institutional order flow
    - Less microstructure noise
    - Better follow-through on breakouts
    Still fast enough to generate multiple trades per session.
    """

    def __init__(self):
        self.current = {}

    def _key(self) -> str:
        now   = datetime.now(timezone.utc)
        block = (now.hour * 60 + now.minute) // CANDLE_MINUTES
        return f"{now.strftime('%Y-%m-%d')}_{block}"

    def update(self, asset: str, price: float, volume: float = 0) -> dict | None:
        key = self._key()

        if asset not in self.current or self.current[asset]["key"] != key:
            completed = None
            if asset in self.current:
                c = self.current[asset]
                completed = {k: v for k, v in c.items() if k != "key"}
            self.current[asset] = {
                "key": key, "open": price, "high": price,
                "low": price, "close": price, "volume": volume,
            }
            return completed

        c = self.current[asset]
        c["high"]    = max(c["high"], price)
        c["low"]     = min(c["low"],  price)
        c["close"]   = price
        c["volume"] += volume
        return None


# ── VWAP Tracker (for stocks only) ───────────────────────────────────────────

class VWAPTracker:
    """
    Volume Weighted Average Price — resets each trading day.
    Represents where institutions have been trading on average.
    Price > VWAP = institutions bullish → only buy.
    Price < VWAP = institutions bearish → only short.
    Research: PyQuantLab (2025) — VWAP flow filter as institutional alignment.
    """

    def __init__(self):
        self.data = {}

    def update(self, asset: str, candle: dict) -> float | None:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        if asset not in self.data or self.data[asset]["date"] != today:
            self.data[asset] = {"cum_pv": 0.0, "cum_vol": 0.0, "date": today}

        typical = (candle["high"] + candle["low"] + candle["close"]) / 3
        vol     = candle.get("volume", 0)

        self.data[asset]["cum_pv"]  += typical * vol
        self.data[asset]["cum_vol"] += vol

        if self.data[asset]["cum_vol"] > 0:
            return round(self.data[asset]["cum_pv"] / self.data[asset]["cum_vol"], 4)
        return None


# ── Signal Engine ─────────────────────────────────────────────────────────────

class SignalEngine:
    """
    4-filter breakout scanner backed by research.

    Filter hierarchy (remove these in order if you want more trades):
      1. ADX > 25 AND rising  — most important, biggest win rate impact
      2. Donchian breakout    — core signal
      3. Volume > 1.5x        — institutional confirmation
      4. EMA trend alignment  — direction filter
      5. VWAP alignment       — stocks only, institutional flow

    The ADX filter alone eliminates ~60-70% of false breakouts in ranging
    markets. That's why it was the #1 improvement from all research papers.
    """

    def __init__(self):
        self.closes  = {a: deque(maxlen=CANDLE_WINDOW) for a in ALL_ASSETS}
        self.highs   = {a: deque(maxlen=CANDLE_WINDOW) for a in ALL_ASSETS}
        self.lows    = {a: deque(maxlen=CANDLE_WINDOW) for a in ALL_ASSETS}
        self.volumes = {a: deque(maxlen=CANDLE_WINDOW) for a in ALL_ASSETS}

    def add_candle(self, asset: str, candle: dict):
        self.closes[asset].append(candle["close"])
        self.highs[asset].append(candle["high"])
        self.lows[asset].append(candle["low"])
        self.volumes[asset].append(candle.get("volume", 0))

    def ready(self, asset: str) -> bool:
        return len(self.closes[asset]) >= CANDLES_NEEDED

    def collected(self, asset: str) -> int:
        return len(self.closes[asset])

    def analyse(self, asset: str, current_price: float, vwap: float | None = None) -> tuple:
        closes  = list(self.closes[asset])
        highs   = list(self.highs[asset])
        lows    = list(self.lows[asset])
        volumes = list(self.volumes[asset])

        # ── Compute indicators ──────────────────────────────────────────────

        # ADX — the gatekeeper
        adx_val, adx_rising = ind.adx(highs, lows, closes)

        # Donchian channel (exclude current candle)
        don_high, don_low = ind.donchian_channel(
            highs[:-1], lows[:-1], period=DONCHIAN_PERIOD
        )

        # Volume ratio
        vol_r = ind.volume_ratio(volumes, period=VOLUME_PERIOD)

        # EMA trend
        e9  = ind.ema(closes, 9)
        e21 = ind.ema(closes, 21)

        # ATR for stop/target
        atr_val = ind.atr(highs, lows, closes)

        # ── Evaluate conditions ─────────────────────────────────────────────

        trend_ok     = bool(adx_val and adx_val > ADX_THRESHOLD and adx_rising)
        breakout_up  = bool(don_high and current_price > don_high)
        breakout_dn  = bool(don_low  and current_price < don_low)
        vol_ok       = bool(vol_r and vol_r >= VOLUME_THRESHOLD)
        ema_up       = bool(e9 and e21 and e9 > e21)
        ema_dn       = bool(e9 and e21 and e9 < e21)

        # VWAP check (stocks only)
        above_vwap = bool(vwap and current_price > vwap)
        below_vwap = bool(vwap and current_price < vwap)
        vwap_check_buy  = (above_vwap if asset in STOCK_ASSETS else True)
        vwap_check_sell = (below_vwap if asset in STOCK_ASSETS else True)

        # ── Determine signal ────────────────────────────────────────────────

        is_crypto = asset in CRYPTO_ASSETS

        # BUY: all 4 filters aligned
        if trend_ok and breakout_up and vol_ok and ema_up and vwap_check_buy:
            signal = "BUY"

        # SHORT: all 4 filters aligned, inverted (stocks only)
        elif (not is_crypto and
              trend_ok and breakout_dn and vol_ok and ema_dn and vwap_check_sell):
            signal = "SHORT"

        else:
            signal = "HOLD"

        return signal, {
            "adx":       adx_val,
            "adx_up":    adx_rising,
            "don_high":  don_high,
            "don_low":   don_low,
            "vol_ratio": vol_r,
            "e9":        e9,
            "e21":       e21,
            "atr":       atr_val,
            "trend_ok":  trend_ok,
            "breakout":  breakout_up or breakout_dn,
            "vol_ok":    vol_ok,
            "ema_ok":    ema_up or ema_dn,
            "vwap":      vwap,
        }

    def adx_value(self, asset: str) -> float | None:
        """Get current ADX for exit decisions."""
        closes = list(self.closes[asset])
        highs  = list(self.highs[asset])
        lows   = list(self.lows[asset])
        val, _ = ind.adx(highs, lows, closes)
        return val


# ── Trade Manager ─────────────────────────────────────────────────────────────

class TradeManager:
    """
    Manages stops, targets, and the ADX-based smart exit.

    Stage 0: Initial. Stop at -1.5 ATR. Target at +3 ATR.
    Stage 1: After +1.5 ATR profit → stop moves to breakeven.
             At this stage: also exit if ADX drops below 20
             (trend is dying — lock in the free trade).
    """

    def __init__(self):
        self.trades = {}

    def open(self, asset: str, entry: float, atr: float, direction: str = "LONG"):
        stop   = entry - STOP_ATR_MULT   * atr if direction == "LONG" else entry + STOP_ATR_MULT   * atr
        target = entry + TARGET_ATR_MULT * atr if direction == "LONG" else entry - TARGET_ATR_MULT * atr
        be_trig = entry + STOP_ATR_MULT * atr  if direction == "LONG" else entry - STOP_ATR_MULT * atr

        self.trades[asset] = {
            "entry":      entry,
            "stop":       stop,
            "target":     target,
            "be_trigger": be_trig,
            "atr":        atr,
            "direction":  direction,
            "stage":      0,
        }
        risk   = abs(entry - stop)
        reward = abs(target - entry)
        print(
            f"  [SET] {asset} {direction} | "
            f"entry=${entry:,.4f} | "
            f"stop=${stop:,.4f} (risk ${risk:,.2f}) | "
            f"target=${target:,.4f} (reward ${reward:,.2f}) | "
            f"R:R=2:1"
        )

    def update_breakeven(self, asset: str, price: float):
        if asset not in self.trades:
            return
        t = self.trades[asset]
        if t["stage"] == 0:
            if t["direction"] == "LONG"  and price >= t["be_trigger"]:
                t["stop"]  = t["entry"]
                t["stage"] = 1
                print(f"  [BREAKEVEN] {asset} → stop moved to entry ${t['entry']:,.4f}")
            elif t["direction"] == "SHORT" and price <= t["be_trigger"]:
                t["stop"]  = t["entry"]
                t["stage"] = 1
                print(f"  [BREAKEVEN] {asset} → stop moved to entry ${t['entry']:,.4f}")

    def check_tick(self, asset: str, price: float) -> str:
        """Called on every tick — real-time stop/target monitoring."""
        if asset not in self.trades:
            return "HOLD"
        t = self.trades[asset]
        if t["direction"] == "LONG":
            if price >= t["target"]: return "TARGET"
            if price <= t["stop"]:   return "STOP"
        else:
            if price <= t["target"]: return "TARGET"
            if price >= t["stop"]:   return "STOP"
        return "HOLD"

    def check_adx_exit(self, asset: str, adx_val: float | None) -> bool:
        """
        Smart ADX exit: if we're at breakeven or better AND ADX drops
        below 20, the trend is dying — exit now rather than give back profit.
        Research: Alpha Algo Trading (2026) — ADX exits improved RET/DD by 79%.
        """
        if asset not in self.trades:
            return False
        if adx_val is None:
            return False
        t = self.trades[asset]
        return t["stage"] >= 1 and adx_val < ADX_EXIT_LEVEL

    def direction(self, asset: str) -> str:
        return self.trades.get(asset, {}).get("direction", "LONG")

    def stop_price(self, asset: str) -> float:
        return self.trades.get(asset, {}).get("stop", 0)

    def target_price(self, asset: str) -> float:
        return self.trades.get(asset, {}).get("target", 0)

    def has(self, asset: str) -> bool:
        return asset in self.trades

    def clear(self, asset: str):
        self.trades.pop(asset, None)


# ── Portfolio Manager ─────────────────────────────────────────────────────────

class Portfolio:
    def __init__(self):
        self.positions   = {}
        self.capital     = INITIAL_CAPITAL
        self.total_pnl   = 0.0
        self.trade_count = 0
        self.wins        = 0
        self.losses      = 0
        self.cooldowns   = {}

    def has(self, asset: str) -> bool:
        return asset in self.positions

    def qty(self, asset: str) -> float:
        return self.positions.get(asset, {}).get("qty", 0)

    def tick_cooldowns(self):
        for a in list(self.cooldowns):
            self.cooldowns[a] += 1

    def can_open(self, asset: str) -> tuple[bool, str]:
        if self.has(asset):
            return False, "in position"
        if len(self.positions) >= MAX_POSITIONS:
            return False, f"max {MAX_POSITIONS} positions"
        cd = self.cooldowns.get(asset, CANDLE_COOLDOWN)
        if cd < CANDLE_COOLDOWN:
            return False, f"cooldown ({CANDLE_COOLDOWN - cd})"
        return True, "ok"

    def position_qty(self, price: float, atr: float) -> float:
        """
        Volatility-adjusted sizing. Risk 1% of capital.
        Hard cap at $10k notional. Always floor to avoid oversell errors.
        """
        atr_safe = max(atr, price * 0.001)
        risk     = self.capital * RISK_PER_TRADE
        stop_d   = STOP_ATR_MULT * atr_safe
        qty      = risk / stop_d if stop_d > 0 else 0
        qty      = min(qty, MAX_NOTIONAL / price)
        return floor_qty(qty)

    def open(self, asset: str, qty: float, price: float, atr: float, direction: str):
        self.positions[asset] = {"qty": qty, "entry": price, "atr": atr, "direction": direction}
        self.cooldowns[asset] = 0
        print(
            f"\n  [OPEN] {asset:<5} {direction:<6} | qty={qty} | "
            f"price=${price:,.4f} | "
            f"open={len(self.positions)}/{MAX_POSITIONS}"
        )

    def close(self, asset: str, price: float, reason: str) -> float:
        if not self.has(asset):
            return 0
        pos = self.positions.pop(asset)
        if pos["direction"] == "LONG":
            pnl = (price - pos["entry"]) * pos["qty"]
        else:
            pnl = (pos["entry"] - price) * pos["qty"]

        self.total_pnl   += pnl
        self.capital     += pnl
        self.trade_count += 1
        self.wins   += 1 if pnl >= 0 else 0
        self.losses += 0 if pnl >= 0 else 1

        tag = "PROFIT" if pnl >= 0 else "LOSS  "
        print(
            f"\n  [CLOSE] {asset:<5} | "
            f"entry=${pos['entry']:,.4f} → exit=${price:,.4f} | "
            f"{tag}: ${pnl:+.2f} | "
            f"running P&L: ${self.total_pnl:+.2f} | "
            f"reason={reason}"
        )
        self.cooldowns[asset] = 0

        if self.trade_count % 5 == 0:
            self.dashboard()
        return pnl

    def max_drawdown_hit(self) -> bool:
        return (INITIAL_CAPITAL - self.capital) / INITIAL_CAPITAL >= MAX_DRAWDOWN

    def dashboard(self):
        wr = (self.wins / self.trade_count * 100) if self.trade_count else 0
        dd = max(0, (INITIAL_CAPITAL - self.capital) / INITIAL_CAPITAL * 100)
        open_pos = {k: f"{v['direction']} {v['qty']}" for k, v in self.positions.items()}
        print("\n" + "═" * 68)
        print("  FINAL MOMENTUM TRADER — DASHBOARD")
        print("═" * 68)
        print(f"  Capital     : ${self.capital:>12,.2f}  (started ${INITIAL_CAPITAL:,.2f})")
        print(f"  Total P&L   : ${self.total_pnl:>+12.2f}")
        print(f"  Drawdown    : {dd:.2f}%")
        print(f"  Trades      : {self.trade_count}  ({self.wins}W / {self.losses}L)  Win rate: {wr:.1f}%")
        print(f"  Open        : {open_pos or 'None'}")
        print("═" * 68 + "\n")


# ── Alpaca Executor ───────────────────────────────────────────────────────────

class AlpacaExecutor:
    def __init__(self):
        self.client = TradingClient(
            os.getenv("ALPACA_API_KEY"),
            os.getenv("ALPACA_SECRET_KEY"),
            paper=True,
        )
        account = self.client.get_account()
        print(f"  Alpaca | Paper capital: ${float(account.cash):,.2f}\n")

    def _place(self, asset: str, qty: float, side: OrderSide) -> bool:
        try:
            order = MarketOrderRequest(
                symbol=to_alpaca_symbol(asset),
                qty=floor_qty(qty),
                side=side,
                time_in_force=TimeInForce.GTC,
            )
            r = self.client.submit_order(order)
            print(f"  [ALPACA] {side.value.upper():<5} | id={str(r.id)[:8]}...")
            return True
        except Exception as e:
            print(f"  [ALPACA ERROR] {side.value} {asset}: {e}")
            return False

    def buy(self, asset: str, qty: float) -> bool:
        return self._place(asset, qty, OrderSide.BUY)

    def sell(self, asset: str, qty: float) -> bool:
        return self._place(asset, qty, OrderSide.SELL)


# ── Main Trader ───────────────────────────────────────────────────────────────

class FinalMomentumTrader:
    def __init__(self):
        self.consumer  = Consumer(KAFKA_CONF)
        self.candles   = CandleBuilder()
        self.vwap      = VWAPTracker()
        self.engine    = SignalEngine()
        self.trades    = TradeManager()
        self.portfolio = Portfolio()
        self.alpaca    = AlpacaExecutor()

    def _on_assign(self, consumer, partitions):
        print(f"  Partitions: {[p.partition for p in partitions]}\n")

    # ── Open Position ─────────────────────────────────────────────────────────

    def _open(self, asset: str, price: float, atr: float, direction: str):
        can, reason = self.portfolio.can_open(asset)
        if not can:
            print(f"  [SKIP {direction}] {asset} — {reason}")
            return
        if not atr or atr <= 0:
            return
        qty = self.portfolio.position_qty(price, atr)
        if qty <= 0:
            return

        side = OrderSide.BUY if direction == "LONG" else OrderSide.SELL
        if self.alpaca._place(asset, qty, side):
            self.portfolio.open(asset, qty, price, atr, direction)
            self.trades.open(asset, price, atr, direction)

    # ── Close Position ────────────────────────────────────────────────────────

    def _close(self, asset: str, price: float, reason: str):
        if not self.portfolio.has(asset):
            return
        qty       = self.portfolio.qty(asset)
        direction = self.trades.direction(asset)

        # Clear locally FIRST — prevents repeated firing
        self.portfolio.close(asset, price, reason)
        self.trades.clear(asset)

        # Then tell Alpaca
        side = OrderSide.SELL if direction == "LONG" else OrderSide.BUY
        self.alpaca._place(asset, qty, side)

    # ── Main Loop ─────────────────────────────────────────────────────────────

    def run(self):
        self.consumer.subscribe(TOPICS, on_assign=self._on_assign)
        print(
            "Final Momentum Trader | 3-min Candles | "
            "ADX-Filtered Donchian | VWAP Stocks | Long+Short\n"
        )
        print(f"Warming up — {CANDLES_NEEDED} × {CANDLE_MINUTES}min candles = "
              f"~{CANDLES_NEEDED * CANDLE_MINUTES} minutes per asset\n")

        try:
            while True:
                if self.portfolio.max_drawdown_hit():
                    print("\n  [RISK HALT] 5% drawdown — stopped.")
                    self.portfolio.dashboard()
                    break

                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        print(f"  Kafka error: {msg.error()}")
                    continue

                data   = json.loads(msg.value().decode("utf-8"))
                asset  = data.get("coin") or data.get("asset")
                price  = data.get("price")
                volume = data.get("volume", 0)

                if not asset or not price or asset not in ALL_ASSETS:
                    continue

                # Skip stocks outside market hours
                if asset in STOCK_ASSETS and not is_us_market_hours():
                    self.consumer.commit(asynchronous=False)
                    continue

                # ── 1. Real-time stop/target on every tick ─────────────────

                if self.trades.has(asset):
                    self.trades.update_breakeven(asset, price)
                    outcome = self.trades.check_tick(asset, price)

                    if outcome == "TARGET":
                        print(f"\n  [TARGET ✓] {asset} | ${price:,.4f}")
                        self._close(asset, price, "TARGET_HIT")
                        self.consumer.commit(asynchronous=False)
                        continue

                    elif outcome == "STOP":
                        print(f"\n  [STOP ✗] {asset} | ${price:,.4f}")
                        self._close(asset, price, "STOP_LOSS")
                        self.consumer.commit(asynchronous=False)
                        continue

                # ── 2. Build candle ────────────────────────────────────────

                completed = self.candles.update(asset, price, volume)
                if completed is None:
                    self.consumer.commit(asynchronous=False)
                    continue

                # ── 3. Candle completed ────────────────────────────────────

                self.engine.add_candle(asset, completed)
                vwap_val = self.vwap.update(asset, completed)
                self.portfolio.tick_cooldowns()

                n = self.engine.collected(asset)

                if not self.engine.ready(asset):
                    print(
                        f"  Candle {n:>2}/{CANDLES_NEEDED} | {asset:<5} | "
                        f"O=${completed['open']:,.3f} "
                        f"H=${completed['high']:,.3f} "
                        f"L=${completed['low']:,.3f} "
                        f"C=${completed['close']:,.3f}"
                    )
                    self.consumer.commit(asynchronous=False)
                    continue

                # ── 4. ADX exit check on candle close ──────────────────────

                if self.trades.has(asset):
                    adx_now = self.engine.adx_value(asset)
                    if self.trades.check_adx_exit(asset, adx_now):
                        print(
                            f"\n  [ADX EXIT] {asset} | ADX={adx_now:.1f} < {ADX_EXIT_LEVEL} "
                            f"(trend dying) | locking in profit"
                        )
                        self._close(asset, completed["close"], "ADX_EXIT")
                        self.consumer.commit(asynchronous=False)
                        continue

                # ── 5. Compute signal ──────────────────────────────────────

                signal, idict = self.engine.analyse(
                    asset, completed["close"],
                    vwap=vwap_val if asset in STOCK_ASSETS else None
                )

                adx_v   = idict.get("adx") or 0
                vol_r   = idict.get("vol_ratio") or 0
                atr_v   = idict.get("atr") or 0
                t_ok    = "✓" if idict.get("trend_ok")  else "✗"
                b_ok    = "✓" if idict.get("breakout")  else "✗"
                v_ok    = "✓" if idict.get("vol_ok")    else "✗"
                e_ok    = "✓" if idict.get("ema_ok")    else "✗"

                pos_info = ""
                if self.trades.has(asset):
                    pos_info = (f" | tgt=${self.trades.target_price(asset):,.3f} "
                                f"stp=${self.trades.stop_price(asset):,.3f}")

                print(
                    f"  {asset:<5} | ${completed['close']:>10,.3f} | "
                    f"ADX={adx_v:>5.1f}({t_ok}) "
                    f"broke({b_ok}) "
                    f"vol×{vol_r:.1f}({v_ok}) "
                    f"ema({e_ok}) "
                    f"ATR=${atr_v:,.2f}"
                    f"{pos_info}"
                )

                # ── 6. Execute signal ──────────────────────────────────────

                if signal == "BUY" and not self.portfolio.has(asset):
                    print(
                        f"\n  *** LONG BREAKOUT *** {asset} | "
                        f"${completed['close']:,.3f} | ADX={adx_v:.1f} | "
                        f"vol={vol_r:.1f}x"
                    )
                    self._open(asset, completed["close"], atr_v, "LONG")

                elif signal == "SHORT" and not self.portfolio.has(asset):
                    print(
                        f"\n  *** SHORT BREAKDOWN *** {asset} | "
                        f"${completed['close']:,.3f} | ADX={adx_v:.1f} | "
                        f"vol={vol_r:.1f}x"
                    )
                    self._open(asset, completed["close"], atr_v, "SHORT")

                self.consumer.commit(asynchronous=False)

        except KeyboardInterrupt:
            print("\nTrader stopped.")
        finally:
            self.portfolio.dashboard()
            self.consumer.close()


# ── Run ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    trader = FinalMomentumTrader()
    trader.run()