"""Deterministic candle builders shared by the strategy and backtest tests."""


def make_candles(direction="up", trend_len=12, last_volume=300.0, base_len=24):
    """
    A choppy sideways base followed by a strong one-directional trend whose
    final candle carries a volume spike. With the defaults this satisfies
    every entry gate (ADX > 25 and rising, Donchian breakout, volume >= 1.5x,
    EMA alignment) on the last candle.
    """
    candles = []
    price = 100.0
    for i in range(base_len):
        step = 0.4 if i % 2 == 0 else -0.4
        o, price = price, price + step
        candles.append({
            "open": o, "high": max(o, price) + 0.2, "low": min(o, price) - 0.2,
            "close": price, "volume": 100.0,
        })
    sign = 1 if direction == "up" else -1
    for i in range(trend_len):
        o, price = price, price + sign * 1.5
        vol = last_volume if i == trend_len - 1 else 100.0
        candles.append({
            "open": o, "high": max(o, price) + 0.2, "low": min(o, price) - 0.2,
            "close": price, "volume": vol,
        })
    return candles


def feed(engine, asset, candles):
    for c in candles:
        engine.add_candle(asset, c)
