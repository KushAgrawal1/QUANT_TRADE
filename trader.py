"""
trader.py — Overnight AI-Enhanced Momentum Trader

Fixes in this version:
  1. Longer cooldown after TARGET hit (7 candles = 21 min)
     — prevents re-entering same asset immediately after profit
     — morning BTC losses were caused by 3 back-to-back entries
       on the same choppy breakout

  2. Loss cooldown protection (10 candles = 30 min)
     — after a STOP_LOSS on an asset, wait 30 min before re-entering
     — prevents chasing a reversing market

  3. Last trade result tracked per asset
     — used for intelligent cooldown decisions
"""

import os
import json
import requests
from collections import deque
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
from confluent_kafka import Consumer, KafkaError
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce
import indicators as ind
import ai_validator as ai

load_dotenv()


# ── Config ────────────────────────────────────────────────────────────────────

KAFKA_CONF = {
    "bootstrap.servers": "localhost:29092",
    "group.id":          "overnight-trader",
    "auto.offset.reset": "latest",
    "enable.auto.commit": False,
}

TOPICS = ["crypto-trades", "stock-trades"]

CRYPTO_ASSETS = ["BTC", "ETH", "SOL", "DOGE", "LTC", "XRP", "ADA", "AVAX", "LINK", "DOT"]
STOCK_ASSETS  = ["AAPL", "NVDA", "TSLA", "AMZN"]
ALL_ASSETS    = CRYPTO_ASSETS + STOCK_ASSETS

CANDLE_MINUTES   = 3
CANDLE_WINDOW    = 80
CANDLES_NEEDED   = 30
WARMUP_CANDLES   = 35

DONCHIAN_PERIOD  = 15
VOLUME_PERIOD    = 15
VOLUME_THRESHOLD = 1.5
ADX_THRESHOLD    = 25
ADX_EXIT_LEVEL   = 20

STOP_ATR_MULT    = 1.5
TARGET_ATR_MULT  = 3.0

RISK_PER_TRADE   = 0.01
MAX_NOTIONAL     = 10_000.0
MAX_POSITIONS    = 3
INITIAL_CAPITAL  = 100_000.0

# Cooldown settings
CANDLE_COOLDOWN       = 3    # normal cooldown between trades
WIN_COOLDOWN          = 7    # candles to wait after a TARGET hit (21 min)
LOSS_COOLDOWN         = 10   # candles to wait after a STOP loss (30 min)

CONSECUTIVE_LOSS_LIMIT = 3
PAUSE_MINUTES          = 45
SLOW_MODE_CAPITAL      = 97_000.0
HALT_CAPITAL           = 95_000.0

AI_CONFIDENCE_THRESHOLD = 7

KRAKEN_REST_PAIRS = {
    "BTC": "XBTUSD", "ETH": "ETHUSD", "SOL": "SOLUSD",
    "DOGE": "XDGUSD", "LTC": "LTCUSD", "XRP": "XRPUSD",
    "ADA": "ADAUSD", "AVAX": "AVAXUSD", "LINK": "LINKUSD", "DOT": "DOTUSD",
}


def to_alpaca_symbol(asset: str) -> str:
    return f"{asset}/USD" if asset in CRYPTO_ASSETS else asset


def is_us_market_hours() -> bool:
    utc = datetime.now(timezone.utc)
    ny  = utc + timedelta(hours=-4)
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
        self._write(f"Capital: ${INITIAL_CAPITAL:,.2f}")
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
            self._write(f"CLOSE | {asset:<5} | ${price:,.4f} | {tag}: ${pnl:+.2f} | {reason}")

    def log_summary(self, capital, pnl, trades, wins, losses):
        wr = (wins / trades * 100) if trades else 0
        self._write("─" * 60)
        self._write(f"SUMMARY | Capital=${capital:,.2f} | P&L=${pnl:+.2f} | "
                    f"Trades={trades} ({wins}W/{losses}L) Win={wr:.1f}%")
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
            return round(self.data[asset]["cum_pv"] / self.data[asset]["cum_vol"], 4)
        return None


# ── Signal Engine ─────────────────────────────────────────────────────────────

class SignalEngine:
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

    def recent_candles(self, asset: str) -> list:
        closes = list(self.closes[asset])[-5:]
        highs  = list(self.highs[asset])[-5:]
        lows   = list(self.lows[asset])[-5:]
        return [{"open": closes[i], "high": highs[i],
                 "low": lows[i], "close": closes[i]}
                for i in range(len(closes))]

    def analyse(self, asset: str, price: float, vwap: float | None = None) -> tuple:
        closes  = list(self.closes[asset])
        highs   = list(self.highs[asset])
        lows    = list(self.lows[asset])
        volumes = list(self.volumes[asset])

        adx_val, adx_rising = ind.adx(highs, lows, closes)
        don_high, don_low   = ind.donchian_channel(highs[:-1], lows[:-1], period=DONCHIAN_PERIOD)
        vol_r               = ind.volume_ratio(volumes, period=VOLUME_PERIOD)
        e9                  = ind.ema(closes, 9)
        e21                 = ind.ema(closes, 21)
        atr_val             = ind.atr(highs, lows, closes)

        trend_ok    = bool(adx_val and adx_val > ADX_THRESHOLD and adx_rising)
        breakout_up = bool(don_high and price > don_high)
        breakout_dn = bool(don_low  and price < don_low)
        vol_ok      = bool(vol_r and vol_r >= VOLUME_THRESHOLD)
        ema_up      = bool(e9 and e21 and e9 >= e21 * 0.995)
        ema_dn      = bool(e9 and e21 and e9 < e21)

        above_vwap = bool(vwap and price > vwap)
        below_vwap = bool(vwap and price < vwap)
        vwap_buy   = (above_vwap if asset in STOCK_ASSETS else True)
        vwap_sell  = (below_vwap if asset in STOCK_ASSETS else True)

        if trend_ok and breakout_up and vol_ok and ema_up and vwap_buy:
            signal = "BUY"
        elif (asset in STOCK_ASSETS and
              trend_ok and breakout_dn and vol_ok and ema_dn and vwap_sell):
            signal = "SHORT"
        else:
            signal = "HOLD"

        return signal, {
            "adx": adx_val, "adx_up": adx_rising,
            "don_high": don_high, "don_low": don_low,
            "vol_ratio": vol_r, "atr": atr_val,
            "trend_ok": trend_ok,
            "breakout": breakout_up or breakout_dn,
            "vol_ok":   vol_ok,
            "ema_ok":   ema_up or ema_dn,
        }

    def adx_value(self, asset: str) -> float | None:
        closes = list(self.closes[asset])
        highs  = list(self.highs[asset])
        lows   = list(self.lows[asset])
        val, _ = ind.adx(highs, lows, closes)
        return val


# ── Trade Manager ─────────────────────────────────────────────────────────────

class TradeManager:
    def __init__(self):
        self.trades = {}

    def open(self, asset: str, entry: float, atr: float, direction: str = "LONG"):
        stop    = entry - STOP_ATR_MULT   * atr if direction == "LONG" else entry + STOP_ATR_MULT   * atr
        target  = entry + TARGET_ATR_MULT * atr if direction == "LONG" else entry - TARGET_ATR_MULT * atr
        be_trig = entry + STOP_ATR_MULT   * atr if direction == "LONG" else entry - STOP_ATR_MULT   * atr
        self.trades[asset] = {
            "entry": entry, "stop": stop, "target": target,
            "be_trigger": be_trig, "atr": atr,
            "direction": direction, "stage": 0,
        }
        print(f"  [SET] {asset} {direction} | entry=${entry:,.4f} | "
              f"stop=${stop:,.4f} | target=${target:,.4f} | R:R=2:1")

    def update_breakeven(self, asset: str, price: float):
        if asset not in self.trades:
            return
        t = self.trades[asset]
        if t["stage"] == 0:
            if ((t["direction"] == "LONG"  and price >= t["be_trigger"]) or
                    (t["direction"] == "SHORT" and price <= t["be_trigger"])):
                t["stop"]  = t["entry"]
                t["stage"] = 1
                print(f"  [BREAKEVEN] {asset} → free trade, stop=${t['entry']:,.4f}")

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
        return self.trades[asset]["stage"] >= 1 and adx_val < ADX_EXIT_LEVEL

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
    def __init__(self, logger: OvernightLogger):
        self.logger           = logger
        self.positions        = {}
        self.capital          = INITIAL_CAPITAL
        self.total_pnl        = 0.0
        self.trade_count      = 0
        self.wins             = 0
        self.losses           = 0
        self.cooldowns        = {}
        self.last_result      = {}   # asset → "WIN" or "LOSS"
        self.consecutive_loss = 0
        self.pause_until      = None

    def has(self, asset: str) -> bool:
        return asset in self.positions

    def qty(self, asset: str) -> float:
        return self.positions.get(asset, {}).get("qty", 0)

    def tick_cooldowns(self):
        for a in list(self.cooldowns):
            self.cooldowns[a] += 1

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
        return HALT_CAPITAL <= self.capital < SLOW_MODE_CAPITAL

    def can_open(self, asset: str) -> tuple[bool, str]:
        if self.is_paused():
            remaining = (self.pause_until - datetime.now(timezone.utc)).seconds // 60
            return False, f"paused ({remaining}min left)"
        if self.has(asset):
            return False, "in position"
        if len(self.positions) >= MAX_POSITIONS:
            return False, f"max {MAX_POSITIONS}"

        cd = self.cooldowns.get(asset, CANDLE_COOLDOWN)

        # FIX: longer cooldown after a loss on this asset
        if self.last_result.get(asset) == "LOSS" and cd < LOSS_COOLDOWN:
            return False, f"loss cooldown ({LOSS_COOLDOWN - cd} candles left)"

        # Normal cooldown
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

    def open(self, asset: str, qty: float, price: float, atr: float, direction: str):
        self.positions[asset] = {"qty": qty, "entry": price, "atr": atr, "direction": direction}
        self.cooldowns[asset] = 0
        self.logger.log_trade("OPEN", asset, qty, price)
        print(f"\n  [OPEN] {asset:<5} {direction:<6} | qty={qty} | "
              f"${price:,.4f} | open={len(self.positions)}/{MAX_POSITIONS}")

    def close(self, asset: str, price: float, reason: str) -> float:
        if not self.has(asset):
            return 0
        pos = self.positions.pop(asset)
        pnl = ((price - pos["entry"]) * pos["qty"] if pos["direction"] == "LONG"
               else (pos["entry"] - price) * pos["qty"])

        self.total_pnl   += pnl
        self.capital     += pnl
        self.trade_count += 1

        if pnl >= 0:
            self.wins             += 1
            self.consecutive_loss  = 0
            self.last_result[asset] = "WIN"
            # FIX: longer cooldown after a win — let the market breathe
            # prevents immediately re-entering same asset after profit
            self.cooldowns[asset] = WIN_COOLDOWN
        else:
            self.losses           += 1
            self.consecutive_loss += 1
            self.last_result[asset] = "LOSS"
            # FIX: reset cooldown to 0 so loss_cooldown check handles it
            self.cooldowns[asset] = 0

            if self.consecutive_loss >= CONSECUTIVE_LOSS_LIMIT:
                self.pause_until = (datetime.now(timezone.utc)
                                    + timedelta(minutes=PAUSE_MINUTES))
                msg = (f"{CONSECUTIVE_LOSS_LIMIT} consecutive losses — "
                       f"pausing {PAUSE_MINUTES}min until "
                       f"{self.pause_until.strftime('%H:%M UTC')}")
                print(f"\n  [PAUSE] {msg}")
                self.logger.log(f"PAUSE | {msg}")

        tag = "PROFIT" if pnl >= 0 else "LOSS  "
        print(f"\n  [CLOSE] {asset:<5} | entry=${pos['entry']:,.4f} → ${price:,.4f} | "
              f"{tag}: ${pnl:+.2f} | total: ${self.total_pnl:+.2f} | {reason}")

        self.logger.log_trade("CLOSE", asset, pos["qty"], price, pnl, reason)

        if self.trade_count % 5 == 0:
            self.dashboard()

        return pnl

    def max_drawdown_hit(self) -> bool:
        return self.capital < HALT_CAPITAL

    def dashboard(self):
        wr   = (self.wins / self.trade_count * 100) if self.trade_count else 0
        dd   = max(0, (INITIAL_CAPITAL - self.capital) / INITIAL_CAPITAL * 100)
        mode = " [SLOW MODE]" if self.in_slow_mode() else ""
        print("\n" + "═" * 68)
        print(f"  OVERNIGHT TRADER DASHBOARD{mode}")
        print("═" * 68)
        print(f"  Capital  : ${self.capital:>12,.2f}")
        print(f"  P&L      : ${self.total_pnl:>+12.2f}")
        print(f"  Drawdown : {dd:.2f}%")
        print(f"  Trades   : {self.trade_count} ({self.wins}W/{self.losses}L) Win: {wr:.1f}%")
        print(f"  Open     : {list(self.positions.keys()) or 'None'}")
        print(f"  Log file : {self.logger.filename}")
        print("═" * 68 + "\n")
        self.logger.log_summary(self.capital, self.total_pnl,
                                self.trade_count, self.wins, self.losses)


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
            tif = TimeInForce.DAY if asset in STOCK_ASSETS else TimeInForce.GTC

        # For SELL: get actual quantity from Alpaca to avoid insufficient balance
            if side == OrderSide.SELL:
                try:
                    position = self.client.get_open_position(to_alpaca_symbol(asset))
                    actual_qty = floor_qty(float(position.qty))
                    safe_qty = actual_qty
                except Exception:
                    safe_qty = floor_qty(qty)  # fallback to our estimate
            else:
                safe_qty = floor_qty(qty)

            order = MarketOrderRequest(
                symbol=to_alpaca_symbol(asset),
                qty=safe_qty,
                side=side,
                time_in_force=tif,
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


# ── Instant Warmup ────────────────────────────────────────────────────────────

def warmup_crypto(engine) -> int:
    print("Fetching crypto history from Kraken REST API...")
    warmed = 0
    for coin, pair in KRAKEN_REST_PAIRS.items():
        try:
            resp = requests.get(
                "https://api.kraken.com/0/public/OHLC",
                params={"pair": pair, "interval": 1}, timeout=10
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
            print(f"  ✓ {coin:<5} {len(three_min)} candles | ${three_min[-1]['close']:,.4f}")
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
    headers = {"APCA-API-KEY-ID": api_key, "APCA-API-SECRET-KEY": secret_key}
    for stock in STOCK_ASSETS:
        try:
            resp = requests.get(
                f"https://data.alpaca.markets/v2/stocks/{stock}/bars",
                headers=headers,
                params={"timeframe": "3Min", "limit": WARMUP_CANDLES,
                        "feed": "iex", "sort": "asc"},
                timeout=10,
            )
            bars = resp.json().get("bars", [])
            if not bars:
                print(f"  ✗ {stock}: no data (market closed?)")
                continue
            for b in bars:
                engine.add_candle(stock, {"open": b["o"], "high": b["h"],
                                          "low": b["l"], "close": b["c"],
                                          "volume": b["v"]})
            print(f"  ✓ {stock:<5} {len(bars)} bars | ${bars[-1]['c']:,.3f}")
            warmed += 1
        except Exception as e:
            print(f"  ✗ {stock}: {e}")
    return warmed


# ── Main Overnight Trader ─────────────────────────────────────────────────────

class OvernightTrader:
    def __init__(self):
        self.logger    = OvernightLogger()
        self.consumer  = Consumer(KAFKA_CONF)
        self.candles   = CandleBuilder()
        self.vwap      = VWAPTracker()
        self.engine    = SignalEngine()
        self.trades    = TradeManager()
        self.portfolio = Portfolio(self.logger)
        self.alpaca    = AlpacaExecutor()

        fg_val, fg_label = ai.get_fear_greed()
        ai_key = os.getenv("ANTHROPIC_API_KEY")
        print(f"  Fear & Greed    : {fg_val}/100 ({fg_label})")
        print(f"  Claude AI       : {'✓ enabled' if ai_key else '✗ not configured (optional)'}")
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
        if STOP_ATR_MULT * atr < 0.50:
            print(f"  [SKIP] {asset} — ATR too small (${atr:.4f})")
            return

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

        self.logger.log_signal(asset, direction, price,
                               signal_idict.get("adx") or 0,
                               signal_idict.get("vol_ratio") or 0,
                               confidence, ai_reason)

        if not execute or confidence < AI_CONFIDENCE_THRESHOLD:
            print(f"  [AI SKIP] {asset} {direction} | "
                  f"confidence={confidence}/10 '{ai_reason}'")
            return

        print(f"  [AI OK] {asset} {direction} | confidence={confidence}/10 '{ai_reason}'")

        qty = self.portfolio.position_qty(price, atr)
        if qty <= 0:
            return

        side = OrderSide.BUY if direction == "LONG" else OrderSide.SELL
        if self.alpaca._place(asset, qty, side):
            self.portfolio.open(asset, qty, price, atr, direction)
            self.trades.open(asset, price, atr, direction)

    def _close(self, asset: str, price: float, reason: str):
        if not self.portfolio.has(asset):
            return
        qty       = self.portfolio.qty(asset)
        direction = self.trades.direction(asset)
        self.portfolio.close(asset, price, reason)
        self.trades.clear(asset)
        side = OrderSide.SELL if direction == "LONG" else OrderSide.BUY
        self.alpaca._place(asset, qty, side)

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
        print("Overnight Trader | AI-Validated | Fear&Greed | Safety Features\n")
        self.logger.log("Trader started — overnight mode active")

        try:
            while True:
                if self.portfolio.max_drawdown_hit():
                    msg = f"Capital ${self.portfolio.capital:,.2f} < ${HALT_CAPITAL:,.2f} — HALTED"
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

                data   = json.loads(msg.value().decode("utf-8"))
                asset  = data.get("coin") or data.get("asset")
                price  = data.get("price")
                volume = data.get("volume", 0)

                if not asset or not price or asset not in ALL_ASSETS:
                    continue

                if asset in STOCK_ASSETS and not is_us_market_hours():
                    self.consumer.commit(asynchronous=False)
                    continue

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

                completed = self.candles.update(asset, price, volume)
                if completed is None:
                    self.consumer.commit(asynchronous=False)
                    continue

                self.engine.add_candle(asset, completed)
                vwap_val = self.vwap.update(asset, completed)
                self.portfolio.tick_cooldowns()
                self._close_stocks_at_market_close()

                if not self.engine.ready(asset):
                    n = self.engine.collected(asset)
                    print(f"  Warming {asset:<5} {n}/{CANDLES_NEEDED}")
                    self.consumer.commit(asynchronous=False)
                    continue

                if self.trades.has(asset):
                    adx_now = self.engine.adx_value(asset)
                    if self.trades.check_adx_exit(asset, adx_now):
                        print(f"\n  [ADX EXIT] {asset} | ADX={adx_now:.1f} trend dying")
                        self._close(asset, completed["close"], "ADX_EXIT")
                        self.consumer.commit(asynchronous=False)
                        continue

                signal, idict = self.engine.analyse(
                    asset, completed["close"],
                    vwap=vwap_val if asset in STOCK_ASSETS else None
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

                if signal == "BUY" and not self.portfolio.has(asset):
                    print(f"\n  *** LONG BREAKOUT *** {asset} | "
                          f"${completed['close']:,.3f} | ADX={adx_v:.1f} | vol={vol_r:.1f}x")
                    self._open(asset, completed["close"], atr_v, "LONG", idict)

                elif signal == "SHORT" and not self.portfolio.has(asset):
                    print(f"\n  *** SHORT BREAKDOWN *** {asset} | "
                          f"${completed['close']:,.3f} | ADX={adx_v:.1f} | vol={vol_r:.1f}x")
                    self._open(asset, completed["close"], atr_v, "SHORT", idict)

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