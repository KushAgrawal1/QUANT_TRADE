"""
trader.py — Overnight AI-Enhanced Momentum Trader — BEST VERSION

All fixes and improvements:
  1. STARTUP POSITION CLEANUP
     Closes all open Alpaca positions on startup.
     Prevents position stacking when trader restarts mid-session.
     This was causing AAVE to accumulate 381 units across 5 restarts.

  2. CAPITAL SYNC FROM ALPACA
     Reads actual portfolio value from Alpaca on startup.
     No more manually updating INITIAL_CAPITAL after each session.

  3. SELL QTY FIX
     close_position() always fetches the real Alpaca position qty rather
     than trusting our own bookkeeping (open_position/close_position have
     explicit intent, since a SHORT's closing side is a BUY, not a SELL).
     Eliminates "insufficient balance" errors permanently.

  4. WIN/LOSS COOLDOWNS
     Win:  7 candles (21 min) — no re-entry after profit
     Loss: 10 candles (30 min) — no chasing reversals

  5. EMA TOLERANCE
     EMA(9) >= EMA(21) * 0.995 — allows trade after big price moves

  6. CLAUDE AI VALIDATION
     max_tokens=200 — fixes unterminated JSON string error
     Robust JSON extraction with regex

  7. CLEAN ASSET LIST
     BTC, ETH, SOL, LTC, BCH, AAVE, XMR
     Removed: DOGE, ADA, DOT, AVAX, XRP, LINK (ATR always too small)
"""

import os
import json
import requests
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
from confluent_kafka import Consumer, KafkaError
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce
import ai_validator as ai
from strategy import (
    SignalEngine, CRYPTO_ASSETS, STOCK_ASSETS, ALL_ASSETS,
    CANDLES_NEEDED, ADX_EXIT_LEVEL,
    STOP_ATR_MULT, TARGET_ATR_MULT,
)
from costs import edge_gate

load_dotenv()


# ── Config ────────────────────────────────────────────────────────────────────

KAFKA_CONF = {
    "bootstrap.servers": "localhost:29092",
    "group.id":          "overnight-trader",
    "auto.offset.reset": "latest",
    "enable.auto.commit": False,
}

TOPICS = ["crypto-trades", "stock-trades"]

# Strategy parameters (assets, indicator periods, ATR multiples) live in
# strategy.py so the live trader and the backtester can never drift apart.
CANDLE_MINUTES   = 3
WARMUP_CANDLES   = 35

RISK_PER_TRADE   = 0.01
MAX_NOTIONAL     = 10_000.0
MAX_POSITIONS    = 3

# Safety floors — set relative to current capital, updated dynamically on startup
DRAWDOWN_SLOW    = 0.03   # 3% below portfolio → half position size
DRAWDOWN_HALT    = 0.05   # 5% below portfolio → full stop

CANDLE_COOLDOWN        = 3
WIN_COOLDOWN           = 7
LOSS_COOLDOWN          = 10
CONSECUTIVE_LOSS_LIMIT = 3
PAUSE_MINUTES          = 45
AI_CONFIDENCE_THRESHOLD = 7

KRAKEN_REST_PAIRS = {
    "BTC":  "XBTUSD",
    "ETH":  "ETHUSD",
    "SOL":  "SOLUSD",
    "LTC":  "LTCUSD",
    "BCH":  "BCHUSD",
    "AAVE": "AAVEUSD",
    "XMR":  "XMRUSD",
}


def to_alpaca_symbol(asset: str) -> str:
    return f"{asset}/USD" if asset in CRYPTO_ASSETS else asset


NY_TZ = ZoneInfo("America/New_York")


def is_us_market_hours() -> bool:
    """
    True during 9:45 AM - 3:30 PM Eastern on weekdays.
    Uses America/New_York so this stays correct across the EST/EDT
    daylight-saving transitions instead of assuming a fixed UTC-4 offset
    (which was wrong for roughly half the year).
    """
    ny = datetime.now(timezone.utc).astimezone(NY_TZ)
    if ny.weekday() >= 5:
        return False
    return ny.replace(hour=9, minute=45) <= ny <= ny.replace(hour=15, minute=30)


def floor_qty(qty: float) -> float:
    return int(qty * 10000) / 10000


# ── Overnight Logger ──────────────────────────────────────────────────────────

class OvernightLogger:
    def __init__(self):
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
        self.filename = f"trading_log_{ts}.txt"
        self._write(f"Session started: {datetime.now(timezone.utc).isoformat()}")
        self._write("=" * 60)

    def _write(self, msg: str):
        ts   = datetime.now(timezone.utc).strftime("%H:%M:%S")
        line = f"[{ts}] {msg}"
        with open(self.filename, "a") as f:
            f.write(line + "\n")

    def log(self, msg: str):
        self._write(msg)

    def log_signal(self, asset, signal, price, adx, vol, ai_conf, ai_reason):
        self._write(
            f"SIGNAL {signal:<6} | {asset:<5} | ${price:,.4f} | "
            f"ADX={adx:.1f} vol={vol:.1f}x | AI={ai_conf}/10 '{ai_reason}'"
        )

    def log_trade(self, action, asset, qty, price, pnl=0, reason=""):
        if action == "OPEN":
            self._write(f"OPEN  | {asset:<5} | qty={qty} | ${price:,.4f}")
        else:
            tag = "PROFIT" if pnl >= 0 else "LOSS"
            self._write(
                f"CLOSE | {asset:<5} | ${price:,.4f} | "
                f"{tag}: ${pnl:+.2f} | {reason}"
            )

    def log_summary(self, capital, pnl, trades, wins, losses):
        wr = (wins / trades * 100) if trades else 0
        self._write("─" * 60)
        self._write(
            f"SUMMARY | Capital=${capital:,.2f} | P&L=${pnl:+.2f} | "
            f"Trades={trades} ({wins}W/{losses}L) Win={wr:.1f}%"
        )
        self._write("─" * 60)


# ── Candle Builder ────────────────────────────────────────────────────────────

class CandleBuilder:
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


# ── VWAP Tracker ─────────────────────────────────────────────────────────────

class VWAPTracker:
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
            return round(
                self.data[asset]["cum_pv"] / self.data[asset]["cum_vol"], 4
            )
        return None


# ── Trade Manager ─────────────────────────────────────────────────────────────

class TradeManager:
    def __init__(self):
        self.trades = {}

    def open(self, asset: str, entry: float, atr: float,
             direction: str = "LONG"):
        stop    = (entry - STOP_ATR_MULT   * atr if direction == "LONG"
                   else entry + STOP_ATR_MULT   * atr)
        target  = (entry + TARGET_ATR_MULT * atr if direction == "LONG"
                   else entry - TARGET_ATR_MULT * atr)
        be_trig = (entry + STOP_ATR_MULT   * atr if direction == "LONG"
                   else entry - STOP_ATR_MULT   * atr)
        self.trades[asset] = {
            "entry": entry, "stop": stop, "target": target,
            "be_trigger": be_trig, "atr": atr,
            "direction": direction, "stage": 0,
        }
        rr = TARGET_ATR_MULT / STOP_ATR_MULT if STOP_ATR_MULT else float("inf")
        print(
            f"  [SET] {asset} {direction} | entry=${entry:,.4f} | "
            f"stop=${stop:,.4f} | target=${target:,.4f} | R:R={rr:.1f}:1"
        )

    def update_breakeven(self, asset: str, price: float):
        if asset not in self.trades:
            return
        t = self.trades[asset]
        if t["stage"] == 0:
            if ((t["direction"] == "LONG"  and price >= t["be_trigger"]) or
                    (t["direction"] == "SHORT" and price <= t["be_trigger"])):
                t["stop"]  = t["entry"]
                t["stage"] = 1
                print(
                    f"  [BREAKEVEN] {asset} → "
                    f"free trade, stop=${t['entry']:,.4f}"
                )

    def check_tick(self, asset: str, price: float) -> str:
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
        if asset not in self.trades or adx_val is None:
            return False
        return (self.trades[asset]["stage"] >= 1
                and adx_val < ADX_EXIT_LEVEL)

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
    def __init__(self, logger: OvernightLogger, initial_capital: float):
        self.logger           = logger
        self.positions        = {}
        self.capital          = initial_capital
        self.initial_capital  = initial_capital
        self.total_pnl        = 0.0
        self.trade_count      = 0
        self.wins             = 0
        self.losses           = 0
        self.cooldowns        = {}
        self.last_result      = {}
        self.consecutive_loss = 0
        self.pause_until      = None

        # Dynamic safety floors based on actual capital
        self.slow_mode_floor = initial_capital * (1 - DRAWDOWN_SLOW)
        self.halt_floor      = initial_capital * (1 - DRAWDOWN_HALT)

    def has(self, asset: str) -> bool:
        return asset in self.positions

    def qty(self, asset: str) -> float:
        return self.positions.get(asset, {}).get("qty", 0)

    def tick_cooldowns(self, asset: str):
        """Advance ONLY this asset's own cooldown counter by one candle.

        CandleBuilder buckets every asset on the same absolute wall-clock
        grid, so when a 3-minute block ends, every actively-ticking asset
        completes a candle within moments of each other - this is called
        once per asset's own completion, not once per elapsed block. A
        version that bumped every asset's counter on every call (as this
        used to) let an 11-asset universe burn through a 30-real-minute
        loss cooldown in about 3 real minutes, since BTC's counter got
        incremented every time ANY other asset's candle also completed.
        """
        if asset in self.cooldowns:
            self.cooldowns[asset] += 1

    def is_paused(self) -> bool:
        if self.pause_until is None:
            return False
        if datetime.now(timezone.utc) >= self.pause_until:
            self.pause_until      = None
            self.consecutive_loss = 0
            msg = "Pause ended — resuming trading"
            print(f"\n  [RESUME] {msg}")
            self.logger.log(f"RESUME | {msg}")
            return False
        return True

    def in_slow_mode(self) -> bool:
        return self.halt_floor <= self.capital < self.slow_mode_floor

    def max_drawdown_hit(self) -> bool:
        return self.capital < self.halt_floor

    def can_open(self, asset: str) -> tuple[bool, str]:
        if self.is_paused():
            remaining = int(
                (self.pause_until - datetime.now(timezone.utc)).total_seconds()
            ) // 60
            return False, f"paused ({remaining}min left)"
        if self.has(asset):
            return False, "in position"
        if len(self.positions) >= MAX_POSITIONS:
            return False, f"max {MAX_POSITIONS}"
        cd = self.cooldowns.get(asset, CANDLE_COOLDOWN)
        if self.last_result.get(asset) == "LOSS" and cd < LOSS_COOLDOWN:
            return False, f"loss cooldown ({LOSS_COOLDOWN - cd} candles)"
        if self.last_result.get(asset) == "WIN" and cd < WIN_COOLDOWN:
            return False, f"win cooldown ({WIN_COOLDOWN - cd} candles)"
        if cd < CANDLE_COOLDOWN:
            return False, f"cooldown ({CANDLE_COOLDOWN - cd})"
        return True, "ok"

    def position_qty(self, price: float, atr: float) -> float:
        atr_safe = max(atr, price * 0.001)
        risk     = self.capital * RISK_PER_TRADE
        if self.in_slow_mode():
            risk *= 0.5
        stop_d = STOP_ATR_MULT * atr_safe
        qty    = risk / stop_d if stop_d > 0 else 0
        qty    = min(qty, MAX_NOTIONAL / price)
        return floor_qty(qty)

    def open(self, asset: str, qty: float, price: float,
             atr: float, direction: str):
        self.positions[asset] = {
            "qty": qty, "entry": price,
            "atr": atr, "direction": direction,
        }
        self.cooldowns[asset] = 0
        self.logger.log_trade("OPEN", asset, qty, price)
        print(
            f"\n  [OPEN] {asset:<5} {direction:<6} | qty={qty} | "
            f"${price:,.4f} | open={len(self.positions)}/{MAX_POSITIONS}"
        )

    def close(self, asset: str, price: float, reason: str) -> float:
        if not self.has(asset):
            return 0
        pos = self.positions.pop(asset)
        pnl = ((price - pos["entry"]) * pos["qty"]
               if pos["direction"] == "LONG"
               else (pos["entry"] - price) * pos["qty"])

        self.total_pnl   += pnl
        self.capital     += pnl
        self.trade_count += 1

        if pnl >= 0:
            self.wins              += 1
            self.consecutive_loss   = 0
            self.last_result[asset] = "WIN"
            self.cooldowns[asset]   = 0   # counts UP each candle, gated above
        else:
            self.losses            += 1
            self.consecutive_loss  += 1
            self.last_result[asset] = "LOSS"
            self.cooldowns[asset]   = 0
            if self.consecutive_loss >= CONSECUTIVE_LOSS_LIMIT:
                self.pause_until = (
                    datetime.now(timezone.utc)
                    + timedelta(minutes=PAUSE_MINUTES)
                )
                msg = (
                    f"{CONSECUTIVE_LOSS_LIMIT} consecutive losses — "
                    f"pausing {PAUSE_MINUTES}min until "
                    f"{self.pause_until.strftime('%H:%M UTC')}"
                )
                print(f"\n  [PAUSE] {msg}")
                self.logger.log(f"PAUSE | {msg}")

        tag = "PROFIT" if pnl >= 0 else "LOSS  "
        print(
            f"\n  [CLOSE] {asset:<5} | "
            f"entry=${pos['entry']:,.4f} → ${price:,.4f} | "
            f"{tag}: ${pnl:+.2f} | "
            f"total: ${self.total_pnl:+.2f} | {reason}"
        )
        self.logger.log_trade("CLOSE", asset, pos["qty"], price, pnl, reason)

        if self.trade_count % 5 == 0:
            self.dashboard()

        return pnl

    def dashboard(self):
        wr   = (self.wins / self.trade_count * 100) if self.trade_count else 0
        dd   = max(
            0, (self.initial_capital - self.capital) / self.initial_capital * 100
        )
        mode = " [SLOW MODE]" if self.in_slow_mode() else ""
        print("\n" + "═" * 68)
        print(f"  OVERNIGHT TRADER DASHBOARD{mode}")
        print("═" * 68)
        print(f"  Capital  : ${self.capital:>12,.2f}  "
              f"(started ${self.initial_capital:,.2f})")
        print(f"  P&L      : ${self.total_pnl:>+12.2f}")
        print(f"  Drawdown : {dd:.2f}%")
        print(f"  Trades   : {self.trade_count} "
              f"({self.wins}W/{self.losses}L) Win: {wr:.1f}%")
        print(f"  Open     : {list(self.positions.keys()) or 'None'}")
        print(f"  Log file : {self.logger.filename}")
        print("═" * 68 + "\n")
        self.logger.log_summary(
            self.capital, self.total_pnl,
            self.trade_count, self.wins, self.losses,
        )


# ── Alpaca Executor ───────────────────────────────────────────────────────────

class AlpacaExecutor:
    def __init__(self):
        api_key    = os.getenv("ALPACA_API_KEY")
        secret_key = os.getenv("ALPACA_SECRET_KEY")
        if not api_key or not secret_key:
            raise RuntimeError(
                "ALPACA_API_KEY / ALPACA_SECRET_KEY are not set. "
                "Add them to your .env file before starting the trader."
            )

        self.client = TradingClient(api_key, secret_key, paper=True)
        try:
            account = self.client.get_account()
        except Exception as e:
            raise RuntimeError(
                f"Could not connect to Alpaca - check your API keys "
                f"and that they're for a paper account: {e}"
            ) from e

        print(f"  Alpaca | Cash: ${float(account.cash):,.2f} | "
              f"Portfolio: ${float(account.portfolio_value):,.2f}\n")

    def get_portfolio_value(self) -> float:
        try:
            return float(self.client.get_account().portfolio_value)
        except Exception:
            return 0.0

    def close_all_positions(self):
        """
        Close all open Alpaca positions on startup.
        Prevents position stacking when trader restarts mid-session.
        This was the root cause of AAVE accumulating 381 units.
        """
        try:
            positions = self.client.get_all_positions()
            if not positions:
                print("  [STARTUP] No open positions found — clean slate\n")
                return
            print(f"\n  [STARTUP CLEANUP] {len(positions)} open position(s) found:")
            for pos in positions:
                pnl = float(pos.unrealized_pl)
                print(
                    f"    Closing {pos.symbol:<12} | "
                    f"qty={float(pos.qty):.4f} | "
                    f"P&L=${pnl:+.2f}"
                )
                self.client.close_position(pos.symbol)
            print("  [STARTUP CLEANUP] All positions closed — starting fresh\n")
        except Exception as e:
            print(f"  [STARTUP CLEANUP] Error: {e}\n")

    def _place(self, asset: str, qty: float, side: OrderSide,
               fetch_qty: bool) -> bool:
        """
        fetch_qty=True means "close a position": always fetch the actual
        Alpaca qty instead of trusting our own bookkeeping. Alpaca fills
        buys at a slightly different qty than requested (e.g. 78.6817 vs
        78.8394), and for a SHORT the closing side is a BUY, so this can no
        longer be inferred from side == SELL - it must be passed explicitly.
        """
        try:
            tif = TimeInForce.DAY if asset in STOCK_ASSETS else TimeInForce.GTC

            if fetch_qty:
                try:
                    position = self.client.get_open_position(
                        to_alpaca_symbol(asset)
                    )
                    safe_qty = floor_qty(abs(float(position.qty)))
                    if safe_qty <= 0:
                        print(
                            f"  [ALPACA] No position for {asset} "
                            f"— skipping close"
                        )
                        return True
                except Exception:
                    print(
                        f"  [ALPACA] Position not found for {asset} "
                        f"— skipping close"
                    )
                    return True
            else:
                safe_qty = floor_qty(qty)
                if safe_qty <= 0:
                    print(f"  [ALPACA] Zero quantity for {asset} — skipping order")
                    return False

            order = MarketOrderRequest(
                symbol=to_alpaca_symbol(asset),
                qty=safe_qty,
                side=side,
                time_in_force=tif,
            )
            r = self.client.submit_order(order)
            print(
                f"  [ALPACA] {side.value.upper():<5} | "
                f"qty={safe_qty} | id={str(r.id)[:8]}..."
            )
            return True

        except Exception as e:
            print(f"  [ALPACA ERROR] {side.value} {asset}: {e}")
            return False

    def open_position(self, asset: str, qty: float, direction: str) -> bool:
        """Opening a LONG is a BUY, opening a SHORT is a SELL. Always uses
        our own computed qty - there is no existing position to fetch."""
        side = OrderSide.BUY if direction == "LONG" else OrderSide.SELL
        return self._place(asset, qty, side, fetch_qty=False)

    def close_position(self, asset: str, direction: str) -> bool:
        """Closing a LONG is a SELL, closing a SHORT is a BUY. Fetches the
        real Alpaca qty rather than trusting our own bookkeeping."""
        side = OrderSide.SELL if direction == "LONG" else OrderSide.BUY
        return self._place(asset, 0, side, fetch_qty=True)


# ── Instant Warmup ────────────────────────────────────────────────────────────

def warmup_crypto(engine) -> int:
    print("Fetching crypto history from Kraken REST API...")
    warmed = 0
    for coin, pair in KRAKEN_REST_PAIRS.items():
        try:
            resp = requests.get(
                "https://api.kraken.com/0/public/OHLC",
                params={"pair": pair, "interval": 1}, timeout=10,
            )
            data = resp.json()
            if data.get("error") and data["error"]:
                print(f"  ✗ {coin}: {data['error']}")
                continue
            result_key = [k for k in data["result"].keys() if k != "last"][0]
            raw        = data["result"][result_key]
            usable     = raw[:-1]
            three_min  = []
            i, group   = len(usable) - 1, []
            while i >= 0 and len(three_min) < WARMUP_CANDLES:
                group.insert(0, usable[i])
                if len(group) == 3:
                    three_min.insert(0, {
                        "open":   float(group[0][1]),
                        "high":   max(float(c[2]) for c in group),
                        "low":    min(float(c[3]) for c in group),
                        "close":  float(group[-1][4]),
                        "volume": sum(float(c[6]) for c in group),
                    })
                    group = []
                i -= 1
            for c in three_min:
                engine.add_candle(coin, c)
            print(
                f"  ✓ {coin:<5} {len(three_min)} candles | "
                f"${three_min[-1]['close']:,.4f}"
            )
            warmed += 1
        except Exception as e:
            print(f"  ✗ {coin}: {e}")
    return warmed


def warmup_stocks(engine) -> int:
    api_key    = os.getenv("ALPACA_API_KEY")
    secret_key = os.getenv("ALPACA_SECRET_KEY")
    if not api_key:
        return 0
    print("\nFetching stock history from Alpaca Data API...")
    warmed  = 0
    headers = {
        "APCA-API-KEY-ID":     api_key,
        "APCA-API-SECRET-KEY": secret_key,
    }
    for stock in STOCK_ASSETS:
        try:
            resp = requests.get(
                f"https://data.alpaca.markets/v2/stocks/{stock}/bars",
                headers=headers,
                params={
                    "timeframe": "3Min", "limit": WARMUP_CANDLES,
                    "feed": "iex", "sort": "asc",
                },
                timeout=10,
            )
            bars = resp.json().get("bars", [])
            if not bars:
                print(f"  ✗ {stock}: no data (market closed?)")
                continue
            for b in bars:
                engine.add_candle(stock, {
                    "open": b["o"], "high": b["h"],
                    "low":  b["l"], "close": b["c"],
                    "volume": b["v"],
                })
            print(
                f"  ✓ {stock:<5} {len(bars)} bars | "
                f"${bars[-1]['c']:,.3f}"
            )
            warmed += 1
        except Exception as e:
            print(f"  ✗ {stock}: {e}")
    return warmed


# ── Main Overnight Trader ─────────────────────────────────────────────────────

class OvernightTrader:
    def __init__(self):
        self.logger = OvernightLogger()
        self.alpaca = AlpacaExecutor()

        # FIX: Close all existing positions on startup
        # Prevents position stacking across restarts
        self.alpaca.close_all_positions()

        # FIX: Sync capital from actual Alpaca portfolio value
        real_capital = self.alpaca.get_portfolio_value()
        if real_capital > 0:
            print(f"  Capital synced    : ${real_capital:,.2f} (from Alpaca)")
        else:
            real_capital = 102_055.0
            print(f"  Capital synced    : ${real_capital:,.2f} (default)")

        self.consumer  = Consumer(KAFKA_CONF)
        self.candles   = CandleBuilder()
        self.vwap      = VWAPTracker()
        self.engine    = SignalEngine()
        self.trades    = TradeManager()
        self.portfolio = Portfolio(self.logger, real_capital)

        fg_val, fg_label = ai.get_fear_greed()
        ai_key = os.getenv("ANTHROPIC_API_KEY")

        self.logger.log(f"Capital: ${real_capital:,.2f}")
        self.logger.log(
            f"Safety: slow_mode<${self.portfolio.slow_mode_floor:,.0f} "
            f"halt<${self.portfolio.halt_floor:,.0f}"
        )

        print(f"  Fear & Greed    : {fg_val}/100 ({fg_label})")
        print(
            f"  Claude AI       : "
            f"{'✓ enabled' if ai_key else '✗ not configured (optional)'}"
        )
        print(f"  Assets          : {CRYPTO_ASSETS}")
        print(
            f"  Safety floors   : "
            f"slow=${self.portfolio.slow_mode_floor:,.0f} | "
            f"halt=${self.portfolio.halt_floor:,.0f}"
        )
        print(f"  Log file        : {self.logger.filename}")
        print()

    def _on_assign(self, consumer, partitions):
        print(f"  Partitions: {[p.partition for p in partitions]}\n")

    def _open(self, asset: str, price: float, atr: float,
              direction: str, signal_idict: dict):
        can, reason = self.portfolio.can_open(asset)
        if not can:
            print(f"  [SKIP] {asset} — {reason}")
            return
        if not atr or atr <= 0:
            return

        # Post-cost edge gate. The old check was `STOP_ATR_MULT * atr < 0.50`,
        # a DOLLAR floor: $0.50 against a $100,000 BTC price is 0.0005% and
        # passed every trade ever generated. What matters is ATR as a FRACTION
        # of price, because that is what has to clear the ~0.60% round-trip
        # cost of fees plus spread. Below roughly 0.40% ATR the advertised
        # 2:1 R:R is negative after costs no matter how good the signal is.
        gate_ok, gate_reason, econ = edge_gate(
            price, atr, STOP_ATR_MULT, TARGET_ATR_MULT,
            is_crypto=asset in CRYPTO_ASSETS,
        )
        if not gate_ok:
            print(f"  [COST SKIP] {asset} — {gate_reason}")
            self.logger.log(f"SKIP {direction} {asset} | cost gate: {gate_reason}")
            return
        print(f"  [EDGE OK] {asset} — {gate_reason}")

        is_crypto = asset in CRYPTO_ASSETS
        fg_ok, fg_reason = ai.fear_greed_filter(direction, is_crypto)
        if not fg_ok:
            print(f"  [F&G SKIP] {asset} — {fg_reason}")
            self.logger.log(f"SKIP {direction} {asset} | F&G: {fg_reason}")
            return

        fg_val, _ = ai.get_fear_greed()
        execute, confidence, ai_reason = ai.validate_with_claude(
            asset=asset, signal=direction, price=price,
            adx=signal_idict.get("adx") or 0,
            vol_ratio=signal_idict.get("vol_ratio") or 0,
            atr=atr,
            recent_candles=self.engine.recent_candles(asset),
            fear_greed_val=fg_val,
        )

        self.logger.log_signal(
            asset, direction, price,
            signal_idict.get("adx") or 0,
            signal_idict.get("vol_ratio") or 0,
            confidence, ai_reason,
        )

        if not execute or confidence < AI_CONFIDENCE_THRESHOLD:
            print(
                f"  [AI SKIP] {asset} {direction} | "
                f"confidence={confidence}/10 '{ai_reason}'"
            )
            return

        print(
            f"  [AI OK] {asset} {direction} | "
            f"confidence={confidence}/10 '{ai_reason}'"
        )

        qty = self.portfolio.position_qty(price, atr)
        if qty <= 0:
            return

        if self.alpaca.open_position(asset, qty, direction):
            self.portfolio.open(asset, qty, price, atr, direction)
            self.trades.open(asset, price, atr, direction)
        else:
            print(f"  [SKIP] {asset} order failed — not opening internally")

    def _close(self, asset: str, price: float, reason: str):
        if not self.portfolio.has(asset):
            return
        direction = self.trades.direction(asset)
        if not self.alpaca.close_position(asset, direction):
            # The real order failed - keep our internal state open so this
            # exit is retried on the next tick instead of us losing track
            # of a position Alpaca still holds.
            print(f"  [CLOSE FAILED] {asset} — will retry")
            return
        self.portfolio.close(asset, price, reason)
        self.trades.clear(asset)

    def _close_stocks_at_market_close(self):
        if is_us_market_hours():
            return
        for asset in list(self.portfolio.positions.keys()):
            if asset in STOCK_ASSETS:
                closes = list(self.engine.closes[asset])
                price  = closes[-1] if closes else 0
                print(f"\n  [MARKET CLOSE] Closing {asset}")
                self._close(asset, price, "MARKET_CLOSE")

    def run(self):
        print("=" * 60)
        print("  INSTANT WARMUP")
        print("=" * 60)
        crypto_w = warmup_crypto(self.engine)
        stock_w  = warmup_stocks(self.engine)
        ready    = [a for a in ALL_ASSETS if self.engine.ready(a)]
        print(f"\n  Warmed: {crypto_w} crypto + {stock_w} stocks")
        print(f"  Ready:  {ready}\n")

        self.consumer.subscribe(TOPICS, on_assign=self._on_assign)
        print(
            "Overnight Trader | AI-Validated | Fear&Greed | "
            "Auto-Cleanup | Capital-Synced\n"
        )
        self.logger.log("Trader started")

        try:
            while True:
                if self.portfolio.max_drawdown_hit():
                    msg = (
                        f"Capital ${self.portfolio.capital:,.2f} < "
                        f"${self.portfolio.halt_floor:,.2f} — HALTED"
                    )
                    print(f"\n  [HALT] {msg}")
                    self.logger.log(f"HALT | {msg}")
                    self.portfolio.dashboard()
                    break

                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        print(f"  Kafka error: {msg.error()}")
                    continue

                try:
                    data = json.loads(msg.value().decode("utf-8"))
                except (json.JSONDecodeError, UnicodeDecodeError) as e:
                    print(f"  [KAFKA] Skipping malformed message: {e}")
                    self.consumer.commit(asynchronous=False)
                    continue

                asset  = data.get("coin") or data.get("asset")
                price  = data.get("price")
                volume = data.get("volume", 0)

                if not asset or not price or asset not in ALL_ASSETS:
                    continue

                if asset in STOCK_ASSETS and not is_us_market_hours():
                    self.consumer.commit(asynchronous=False)
                    continue

                # Real-time stop/target on every tick
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

                # Build candle
                completed = self.candles.update(asset, price, volume)
                if completed is None:
                    self.consumer.commit(asynchronous=False)
                    continue

                self.engine.add_candle(asset, completed)
                vwap_val = self.vwap.update(asset, completed)
                self.portfolio.tick_cooldowns(asset)
                self._close_stocks_at_market_close()

                if not self.engine.ready(asset):
                    n = self.engine.collected(asset)
                    print(f"  Warming {asset:<5} {n}/{CANDLES_NEEDED}")
                    self.consumer.commit(asynchronous=False)
                    continue

                # ADX exit
                if self.trades.has(asset):
                    adx_now = self.engine.adx_value(asset)
                    if self.trades.check_adx_exit(asset, adx_now):
                        print(
                            f"\n  [ADX EXIT] {asset} | "
                            f"ADX={adx_now:.1f} trend dying"
                        )
                        self._close(asset, completed["close"], "ADX_EXIT")
                        self.consumer.commit(asynchronous=False)
                        continue

                # Signal
                signal, idict = self.engine.analyse(
                    asset, completed["close"],
                    vwap=vwap_val if asset in STOCK_ASSETS else None,
                )

                adx_v = idict.get("adx") or 0
                vol_r = idict.get("vol_ratio") or 0
                atr_v = idict.get("atr") or 0
                t_ok  = "✓" if idict.get("trend_ok") else "✗"
                b_ok  = "✓" if idict.get("breakout") else "✗"
                v_ok  = "✓" if idict.get("vol_ok")   else "✗"
                e_ok  = "✓" if idict.get("ema_ok")   else "✗"

                pos_info = ""
                if self.trades.has(asset):
                    pos_info = (
                        f" | tgt=${self.trades.target_price(asset):,.3f} "
                        f"stp=${self.trades.stop_price(asset):,.3f}"
                    )

                print(
                    f"  {asset:<5} | ${completed['close']:>10,.3f} | "
                    f"ADX={adx_v:>5.1f}({t_ok}) "
                    f"broke({b_ok}) "
                    f"vol×{vol_r:.1f}({v_ok}) "
                    f"ema({e_ok}) "
                    f"ATR=${atr_v:,.2f}"
                    f"{pos_info}"
                )

                if signal == "BUY" and not self.portfolio.has(asset):
                    print(
                        f"\n  *** LONG BREAKOUT *** {asset} | "
                        f"${completed['close']:,.3f} | "
                        f"ADX={adx_v:.1f} | vol={vol_r:.1f}x"
                    )
                    self._open(
                        asset, completed["close"], atr_v, "LONG", idict
                    )

                elif signal == "SHORT" and not self.portfolio.has(asset):
                    print(
                        f"\n  *** SHORT BREAKDOWN *** {asset} | "
                        f"${completed['close']:,.3f} | "
                        f"ADX={adx_v:.1f} | vol={vol_r:.1f}x"
                    )
                    self._open(
                        asset, completed["close"], atr_v, "SHORT", idict
                    )

                self.consumer.commit(asynchronous=False)

        except KeyboardInterrupt:
            print("\nTrader stopped.")
            self.logger.log("Trader stopped by user")
        finally:
            self.portfolio.dashboard()
            self.consumer.close()


# ── Run ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    trader = OvernightTrader()
    trader.run()