"""
trader.py — Final Momentum Breakout System

Fixes in this version:
  1. ADX bug fixed (values now correctly 0-100, not 0-800)
  2. Instant warmup — fetches 35 historical 3-min candles from:
       Kraken REST API  → crypto (free, no auth)
       Alpaca Data API  → stocks (uses your existing keys)
     No more waiting 75 minutes for warmup.
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

CANDLE_MINUTES  = 3
CANDLE_WINDOW   = 80
CANDLES_NEEDED  = 30      # need 30 so ADX(14) has 29 minimum (period*2+1)
WARMUP_CANDLES  = 35      # fetch 35 from API — extra buffer

DONCHIAN_PERIOD  = 15
VOLUME_PERIOD    = 15
VOLUME_THRESHOLD = 1.5

ADX_THRESHOLD  = 25
ADX_EXIT_LEVEL = 20

STOP_ATR_MULT   = 1.5
TARGET_ATR_MULT = 3.0

RISK_PER_TRADE  = 0.01
MAX_NOTIONAL    = 10_000.0
MAX_POSITIONS   = 3
MAX_DRAWDOWN    = 0.05
INITIAL_CAPITAL = 100_000.0
CANDLE_COOLDOWN = 3

# Kraken REST pair names (different from WebSocket names)
KRAKEN_REST_PAIRS = {
    "BTC":  "XBTUSD",
    "ETH":  "ETHUSD",
    "SOL":  "SOLUSD",
    "DOGE": "XDGUSD",
    "LTC":  "LTCUSD",
    "XRP":  "XRPUSD",
    "ADA":  "ADAUSD",
    "AVAX": "AVAXUSD",
    "LINK": "LINKUSD",
    "DOT":  "DOTUSD",
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


# ── Instant Warmup — Fetch Historical Candles from APIs ───────────────────────

def warmup_crypto(engine) -> int:
    """
    Fetch 1-min candles from Kraken and aggregate into 3-min candles.
    Kraken supports: 1, 5, 15, 30, 60 min — NOT 3 min directly.
    """
    print("Fetching crypto history from Kraken REST API...")
    warmed = 0

    for coin, pair in KRAKEN_REST_PAIRS.items():
        try:
            url    = "https://api.kraken.com/0/public/OHLC"
            params = {"pair": pair, "interval": 1}   # 1-min candles
            resp   = requests.get(url, params=params, timeout=10)
            data   = resp.json()

            if data.get("error") and data["error"]:
                print(f"  ✗ {coin}: {data['error']}")
                continue

            result_key  = [k for k in data["result"].keys() if k != "last"][0]
            raw_candles = data["result"][result_key]

            # Aggregate every 3 one-minute candles into one 3-minute candle
            three_min = []
            # Use most recent complete 1-min candles, skip last (may be incomplete)
            usable = raw_candles[:-1]
            # Align to 3-minute boundaries
            i = len(usable) - 1
            group = []
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

            for candle in three_min:
                engine.add_candle(coin, candle)

            print(f"  ✓ {coin:<5} loaded {len(three_min)} candles | "
                  f"last close=${three_min[-1]['close']:,.4f}")
            warmed += 1

        except Exception as e:
            print(f"  ✗ {coin}: {e}")

    return warmed

def warmup_stocks(engine) -> int:
    """
    Fetch last 35 three-minute bars from Alpaca Data API.
    Uses your existing API keys from .env
    Returns number of assets successfully warmed up.
    """
    api_key    = os.getenv("ALPACA_API_KEY")
    secret_key = os.getenv("ALPACA_SECRET_KEY")

    if not api_key or not secret_key:
        print("  ✗ Stocks: no API keys found in .env")
        return 0

    print("\nFetching stock history from Alpaca Data API...")
    warmed  = 0
    headers = {
        "APCA-API-KEY-ID":     api_key,
        "APCA-API-SECRET-KEY": secret_key,
    }

    for stock in STOCK_ASSETS:
        try:
            url    = f"https://data.alpaca.markets/v2/stocks/{stock}/bars"
            params = {
                "timeframe": "3Min",
                "limit":     WARMUP_CANDLES,
                "feed":      "iex",    # free IEX feed
                "sort":      "asc",
            }
            resp = requests.get(url, headers=headers, params=params, timeout=10)
            data = resp.json()
            bars = data.get("bars", [])

            if not bars:
                print(f"  ✗ {stock}: no data returned (market may be closed)")
                continue

            for bar in bars:
                engine.add_candle(stock, {
                    "open":   bar["o"],
                    "high":   bar["h"],
                    "low":    bar["l"],
                    "close":  bar["c"],
                    "volume": bar["v"],
                })

            print(f"  ✓ {stock:<5} loaded {len(bars)} bars | "
                  f"last close=${bars[-1]['c']:,.3f}")
            warmed += 1

        except Exception as e:
            print(f"  ✗ {stock}: {e}")

    return warmed


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

    def analyse(self, asset: str, current_price: float, vwap: float | None = None) -> tuple:
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
        breakout_up = bool(don_high and current_price > don_high)
        breakout_dn = bool(don_low  and current_price < don_low)
        vol_ok      = bool(vol_r and vol_r >= VOLUME_THRESHOLD)
        ema_up      = bool(e9 and e21 and e9 > e21)
        ema_dn      = bool(e9 and e21 and e9 < e21)

        above_vwap      = bool(vwap and current_price > vwap)
        below_vwap      = bool(vwap and current_price < vwap)
        vwap_check_buy  = (above_vwap if asset in STOCK_ASSETS else True)
        vwap_check_sell = (below_vwap if asset in STOCK_ASSETS else True)

        is_crypto = asset in CRYPTO_ASSETS

        if trend_ok and breakout_up and vol_ok and ema_up and vwap_check_buy:
            signal = "BUY"
        elif (not is_crypto and
              trend_ok and breakout_dn and vol_ok and ema_dn and vwap_check_sell):
            signal = "SHORT"
        else:
            signal = "HOLD"

        return signal, {
            "adx": adx_val, "adx_up": adx_rising,
            "don_high": don_high, "don_low": don_low,
            "vol_ratio": vol_r, "e9": e9, "e21": e21,
            "atr": atr_val,
            "trend_ok": trend_ok,
            "breakout": breakout_up or breakout_dn,
            "vol_ok":   vol_ok,
            "ema_ok":   ema_up or ema_dn,
            "vwap":     vwap,
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
              f"stop=${stop:,.4f} (risk ${abs(entry-stop):,.2f}) | "
              f"target=${target:,.4f} (reward ${abs(target-entry):,.2f}) | R:R=2:1")

    def update_breakeven(self, asset: str, price: float):
        if asset not in self.trades:
            return
        t = self.trades[asset]
        if t["stage"] == 0:
            if (t["direction"] == "LONG"  and price >= t["be_trigger"]) or \
               (t["direction"] == "SHORT" and price <= t["be_trigger"]):
                t["stop"]  = t["entry"]
                t["stage"] = 1
                print(f"  [BREAKEVEN] {asset} → stop at entry ${t['entry']:,.4f}")

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
            return False, f"max {MAX_POSITIONS}"
        cd = self.cooldowns.get(asset, CANDLE_COOLDOWN)
        if cd < CANDLE_COOLDOWN:
            return False, f"cooldown ({CANDLE_COOLDOWN - cd})"
        return True, "ok"

    def position_qty(self, price: float, atr: float) -> float:
        atr_safe = max(atr, price * 0.001)
        risk     = self.capital * RISK_PER_TRADE
        stop_d   = STOP_ATR_MULT * atr_safe
        qty      = risk / stop_d if stop_d > 0 else 0
        qty      = min(qty, MAX_NOTIONAL / price)
        return floor_qty(qty)

    def open(self, asset: str, qty: float, price: float, atr: float, direction: str):
        self.positions[asset] = {"qty": qty, "entry": price, "atr": atr, "direction": direction}
        self.cooldowns[asset] = 0
        print(f"\n  [OPEN] {asset:<5} {direction:<6} | qty={qty} | "
              f"price=${price:,.4f} | open={len(self.positions)}/{MAX_POSITIONS}")

    def close(self, asset: str, price: float, reason: str) -> float:
        if not self.has(asset):
            return 0
        pos = self.positions.pop(asset)
        pnl = (price - pos["entry"]) * pos["qty"] if pos["direction"] == "LONG" \
              else (pos["entry"] - price) * pos["qty"]
        self.total_pnl   += pnl
        self.capital     += pnl
        self.trade_count += 1
        self.wins   += 1 if pnl >= 0 else 0
        self.losses += 0 if pnl >= 0 else 1
        tag = "PROFIT" if pnl >= 0 else "LOSS  "
        print(f"\n  [CLOSE] {asset:<5} | entry=${pos['entry']:,.4f} → exit=${price:,.4f} | "
              f"{tag}: ${pnl:+.2f} | P&L: ${self.total_pnl:+.2f} | reason={reason}")
        self.cooldowns[asset] = 0
        if self.trade_count % 5 == 0:
            self.dashboard()
        return pnl

    def max_drawdown_hit(self) -> bool:
        return (INITIAL_CAPITAL - self.capital) / INITIAL_CAPITAL >= MAX_DRAWDOWN

    def dashboard(self):
        wr = (self.wins / self.trade_count * 100) if self.trade_count else 0
        dd = max(0, (INITIAL_CAPITAL - self.capital) / INITIAL_CAPITAL * 100)
        print("\n" + "═" * 65)
        print("  MOMENTUM TRADER — DASHBOARD")
        print("═" * 65)
        print(f"  Capital  : ${self.capital:>12,.2f}  (started ${INITIAL_CAPITAL:,.2f})")
        print(f"  P&L      : ${self.total_pnl:>+12.2f}")
        print(f"  Drawdown : {dd:.2f}%")
        print(f"  Trades   : {self.trade_count}  ({self.wins}W/{self.losses}L)  Win: {wr:.1f}%")
        print(f"  Open     : {list(self.positions.keys()) or 'None'}")
        print("═" * 65 + "\n")


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
            # Stocks: DAY order (required for fractional shares)
            # Crypto: GTC (markets never close)
            tif = TimeInForce.DAY if asset in STOCK_ASSETS else TimeInForce.GTC

            order = MarketOrderRequest(
                symbol=to_alpaca_symbol(asset),
                qty=floor_qty(qty),
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

    def _close(self, asset: str, price: float, reason: str):
        if not self.portfolio.has(asset):
            return
        qty       = self.portfolio.qty(asset)
        direction = self.trades.direction(asset)
        self.portfolio.close(asset, price, reason)
        self.trades.clear(asset)
        side = OrderSide.SELL if direction == "LONG" else OrderSide.BUY
        self.alpaca._place(asset, qty, side)

    def run(self):
        # ── Instant warmup from REST APIs ──────────────────────────────────
        print("=" * 60)
        print("  INSTANT WARMUP — fetching historical candles")
        print("=" * 60)

        crypto_warmed = warmup_crypto(self.engine)
        stock_warmed  = warmup_stocks(self.engine)

        print(f"\n  Warmed up: {crypto_warmed} crypto + {stock_warmed} stock assets")

        # Check which assets are ready
        ready_assets = [a for a in ALL_ASSETS if self.engine.ready(a)]
        not_ready    = [a for a in ALL_ASSETS if not self.engine.ready(a)]
        print(f"  Ready to trade: {ready_assets}")
        if not_ready:
            print(f"  Still warming:  {not_ready} (will warm from live data)")
        print()

        # ── Start live trading ─────────────────────────────────────────────
        self.consumer.subscribe(TOPICS, on_assign=self._on_assign)
        print("Final Momentum Trader | Live | ADX-Filtered Donchian | Long+Short\n")

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

                if asset in STOCK_ASSETS and not is_us_market_hours():
                    self.consumer.commit(asynchronous=False)
                    continue

                # ── Stop/target on every tick ──────────────────────────────

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

                # ── Build candle ───────────────────────────────────────────

                completed = self.candles.update(asset, price, volume)
                if completed is None:
                    self.consumer.commit(asynchronous=False)
                    continue

                self.engine.add_candle(asset, completed)
                vwap_val = self.vwap.update(asset, completed)
                self.portfolio.tick_cooldowns()

                if not self.engine.ready(asset):
                    n = self.engine.collected(asset)
                    print(f"  Warming {asset:<5} {n}/{CANDLES_NEEDED}")
                    self.consumer.commit(asynchronous=False)
                    continue

                # ── ADX exit check ─────────────────────────────────────────

                if self.trades.has(asset):
                    adx_now = self.engine.adx_value(asset)
                    if self.trades.check_adx_exit(asset, adx_now):
                        print(f"\n  [ADX EXIT] {asset} | ADX={adx_now:.1f} < {ADX_EXIT_LEVEL}")
                        self._close(asset, completed["close"], "ADX_EXIT")
                        self.consumer.commit(asynchronous=False)
                        continue

                # ── Signal ─────────────────────────────────────────────────

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
                    self._open(asset, completed["close"], atr_v, "LONG")

                elif signal == "SHORT" and not self.portfolio.has(asset):
                    print(f"\n  *** SHORT BREAKDOWN *** {asset} | "
                          f"${completed['close']:,.3f} | ADX={adx_v:.1f} | vol={vol_r:.1f}x")
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