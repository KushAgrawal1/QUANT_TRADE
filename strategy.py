"""
strategy.py - Pure signal logic, no I/O.

Extracted from trader.py so the backtester and the live trader run the
SAME code. If they run separate copies they drift, and then your backtest
is measuring a strategy you aren't trading. This module imports nothing
but indicators.py - no Kafka, no Alpaca, no network.
"""

from collections import deque
import indicators as ind


# Strategy parameters. Single source of truth for live and backtest.
CRYPTO_ASSETS = ["BTC", "ETH", "SOL", "LTC", "BCH", "AAVE", "XMR"]
STOCK_ASSETS  = ["AAPL", "NVDA", "TSLA", "AMZN"]
ALL_ASSETS    = CRYPTO_ASSETS + STOCK_ASSETS

CANDLE_WINDOW    = 80
CANDLES_NEEDED   = 30

DONCHIAN_PERIOD  = 15
VOLUME_PERIOD    = 15
VOLUME_THRESHOLD = 1.5
ADX_THRESHOLD    = 25
ADX_EXIT_LEVEL   = 20

STOP_ATR_MULT    = 1.5
TARGET_ATR_MULT  = 3.0

EMA_TOLERANCE    = 0.995


class SignalEngine:
    def __init__(self, assets=None):
        assets = assets or ALL_ASSETS
        self.assets  = list(assets)
        self.closes  = {a: deque(maxlen=CANDLE_WINDOW) for a in self.assets}
        self.highs   = {a: deque(maxlen=CANDLE_WINDOW) for a in self.assets}
        self.lows    = {a: deque(maxlen=CANDLE_WINDOW) for a in self.assets}
        self.volumes = {a: deque(maxlen=CANDLE_WINDOW) for a in self.assets}
        self.opens   = {a: deque(maxlen=CANDLE_WINDOW) for a in self.assets}

    def add_candle(self, asset: str, candle: dict):
        self.opens[asset].append(candle.get("open", candle["close"]))
        self.closes[asset].append(candle["close"])
        self.highs[asset].append(candle["high"])
        self.lows[asset].append(candle["low"])
        self.volumes[asset].append(candle.get("volume", 0))

    def ready(self, asset: str) -> bool:
        return len(self.closes[asset]) >= CANDLES_NEEDED

    def collected(self, asset: str) -> int:
        return len(self.closes[asset])

    def recent_candles(self, asset: str) -> list:
        """
        Last 5 OHLC bars. Previously this passed closes as 'open', feeding
        the AI validator fabricated candles - every bar looked like a doji.
        """
        opens  = list(self.opens[asset])[-5:]
        closes = list(self.closes[asset])[-5:]
        highs  = list(self.highs[asset])[-5:]
        lows   = list(self.lows[asset])[-5:]
        n = min(len(opens), len(closes), len(highs), len(lows))
        return [{"open": opens[i], "high": highs[i],
                 "low": lows[i], "close": closes[i]}
                for i in range(n)]

    def analyse(self, asset: str, price: float, vwap=None,
                allow_short: bool = None) -> tuple:
        closes  = list(self.closes[asset])
        highs   = list(self.highs[asset])
        lows    = list(self.lows[asset])
        volumes = list(self.volumes[asset])

        if allow_short is None:
            allow_short = asset in STOCK_ASSETS

        adx_val, adx_rising = ind.adx(highs, lows, closes)
        # Exclude the current candle from the channel so the breakout is
        # measured against prior range, not against itself.
        don_high, don_low = ind.donchian_channel(
            highs[:-1], lows[:-1], period=DONCHIAN_PERIOD
        )
        vol_r   = ind.volume_ratio(volumes, period=VOLUME_PERIOD)
        e9      = ind.ema(closes, 9)
        e21     = ind.ema(closes, 21)
        atr_val = ind.atr(highs, lows, closes)

        trend_ok    = bool(adx_val and adx_val > ADX_THRESHOLD and adx_rising)
        breakout_up = bool(don_high and price > don_high)
        breakout_dn = bool(don_low  and price < don_low)
        vol_ok      = bool(vol_r and vol_r >= VOLUME_THRESHOLD)
        ema_up      = bool(e9 and e21 and e9 >= e21 * EMA_TOLERANCE)
        ema_dn      = bool(e9 and e21 and e9 < e21)

        # Stocks must confirm against VWAP and fail closed when it is
        # unavailable. Crypto only applies VWAP if the caller supplies one.
        if asset in STOCK_ASSETS:
            vwap_buy  = bool(vwap and price > vwap)
            vwap_sell = bool(vwap and price < vwap)
        elif vwap is not None:
            vwap_buy  = price > vwap
            vwap_sell = price < vwap
        else:
            vwap_buy = vwap_sell = True

        if trend_ok and breakout_up and vol_ok and ema_up and vwap_buy:
            signal = "BUY"
        elif (allow_short and trend_ok and breakout_dn
              and vol_ok and ema_dn and vwap_sell):
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

    def adx_value(self, asset: str):
        val, _ = ind.adx(
            list(self.highs[asset]),
            list(self.lows[asset]),
            list(self.closes[asset]),
        )
        return val
