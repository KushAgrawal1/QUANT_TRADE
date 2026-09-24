"""
indicators.py — Fixed ADX implementation.

Bug fixed: _wilder_smooth was accumulating raw sums instead of averages,
causing ADX values to explode to 100-800+.
Correct Wilder's formula seeds with sum/period (average), not raw sum.
"""



def _ema_series(prices: list, period: int) -> list:
    k = 2 / (period + 1)
    series = [prices[0]]
    for p in prices[1:]:
        series.append(p * k + series[-1] * (1 - k))
    return series


def _wilder_smooth(data: list, period: int) -> list:
    """
    Correct Wilder's smoothing.
    Seeds with AVERAGE of first N values (not raw sum).
    Then: next = (prev * (N-1) + current) / N
    This keeps values bounded — without the /period, values explode.
    """
    if len(data) < period:
        return []
    # Seed: average of first N values
    result = [sum(data[:period]) / period]
    for val in data[period:]:
        result.append((result[-1] * (period - 1) + val) / period)
    return result


def ema(prices: list, period: int) -> float | None:
    if len(prices) < period:
        return None
    return round(_ema_series(prices, period)[-1], 8)


def rsi(prices: list, period: int = 14) -> float | None:
    if len(prices) < period + 1:
        return None
    changes = [prices[i] - prices[i - 1] for i in range(1, len(prices))]
    gains   = [max(c,  0) for c in changes]
    losses  = [max(-c, 0) for c in changes]
    avg_g   = sum(gains[:period])  / period
    avg_l   = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        avg_g = (avg_g * (period - 1) + gains[i])  / period
        avg_l = (avg_l * (period - 1) + losses[i]) / period
    if avg_l == 0:
        return 100.0
    return round(100 - (100 / (1 + avg_g / avg_l)), 2)


def atr(highs: list, lows: list, closes: list, period: int = 14) -> float | None:
    if len(closes) < period + 1:
        return None
    trs = [max(
        highs[i] - lows[i],
        abs(highs[i] - closes[i - 1]),
        abs(lows[i]  - closes[i - 1]),
    ) for i in range(1, len(closes))]
    smoothed = _wilder_smooth(trs, period)
    return round(smoothed[-1], 8) if smoothed else None


def donchian_channel(highs: list, lows: list, period: int = 15) -> tuple:
    if len(highs) < period or len(lows) < period:
        return None, None
    return round(max(highs[-period:]), 6), round(min(lows[-period:]), 6)


def volume_ratio(volumes: list, period: int = 15) -> float | None:
    if len(volumes) < period + 1:
        return None
    avg = sum(volumes[-(period + 1):-1]) / period
    if avg == 0:
        return None
    return round(volumes[-1] / avg, 2)


def adx(highs: list, lows: list, closes: list, period: int = 14) -> tuple:
    """
    Average Directional Index — FIXED implementation.

    ADX range: 0-100
      > 25 AND rising = strong trend → take breakouts
      < 20            = ranging      → skip all signals

    The fix: _wilder_smooth now seeds with average (sum/n), not raw sum.
    Old code: result = [sum(data[:n])]       ← wrong, explodes to 100-800
    New code: result = [sum(data[:n]) / n]   ← correct, stays 0-100
    """
    min_len = period * 2 + 1
    if len(closes) < min_len:
        return None, None

    trs, dm_plus, dm_minus = [], [], []
    for i in range(1, len(closes)):
        high_move = highs[i] - highs[i - 1]
        low_move  = lows[i - 1] - lows[i]
        tr = max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i]  - closes[i - 1]),
        )
        trs.append(tr)
        dm_plus.append(high_move if high_move > low_move and high_move > 0 else 0)
        dm_minus.append(low_move  if low_move > high_move and low_move  > 0 else 0)

    atr_s = _wilder_smooth(trs,      period)
    dmp_s = _wilder_smooth(dm_plus,  period)
    dmm_s = _wilder_smooth(dm_minus, period)

    if not atr_s:
        return None, None

    di_plus  = [100 * dmp_s[i] / atr_s[i] if atr_s[i] > 0 else 0 for i in range(len(atr_s))]
    di_minus = [100 * dmm_s[i] / atr_s[i] if atr_s[i] > 0 else 0 for i in range(len(atr_s))]

    dx = []
    for i in range(len(di_plus)):
        total = di_plus[i] + di_minus[i]
        dx.append(100 * abs(di_plus[i] - di_minus[i]) / total if total > 0 else 0)

    adx_s = _wilder_smooth(dx, period)

    if len(adx_s) < 2:
        return None, None

    return round(adx_s[-1], 2), bool(adx_s[-1] > adx_s[-2])


def trend_direction(prices: list) -> str:
    if len(prices) < 21:
        return "UNKNOWN"
    e9  = _ema_series(prices, 9)[-1]
    e21 = _ema_series(prices, 21)[-1]
    return "UPTREND" if e9 > e21 else "DOWNTREND" if e9 < e21 else "RANGING"