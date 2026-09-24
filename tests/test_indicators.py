import random

import pytest

import indicators as ind


# ---- EMA ------------------------------------------------------------------

def test_ema_of_constant_series_is_the_constant():
    assert ind.ema([5.0] * 30, 9) == pytest.approx(5.0)


def test_ema_hand_computed():
    # period 2 -> k = 2/3. Series: 1, then 2*(2/3)+1*(1/3), then 3*(2/3)+prev*(1/3)
    assert ind.ema([1, 2, 3], 2) == pytest.approx(2.55555556, abs=1e-6)


def test_ema_needs_enough_data():
    assert ind.ema([1, 2, 3], 5) is None


# ---- RSI ------------------------------------------------------------------

def test_rsi_all_gains_is_100():
    assert ind.rsi(list(range(1, 40))) == 100.0


def test_rsi_all_losses_is_0():
    assert ind.rsi(list(range(40, 1, -1))) == 0.0


def test_rsi_needs_period_plus_one_points():
    assert ind.rsi([1.0] * 14) is None
    assert ind.rsi([1.0] * 15) is not None


def test_rsi_alternating_moves_is_near_50():
    prices = [100 + (1 if i % 2 else 0) for i in range(60)]
    assert ind.rsi(prices) == pytest.approx(50, abs=3)


# ---- Wilder smoothing / ATR ----------------------------------------------

def test_wilder_smooth_seeds_with_average_not_sum():
    # This is the original ADX bug: seeding with the raw sum inflated values.
    assert ind._wilder_smooth([2, 4, 6], 3) == [4.0]


def test_wilder_smooth_recurrence():
    out = ind._wilder_smooth([2, 4, 6, 8], 3)
    assert out == [4.0, pytest.approx((4.0 * 2 + 8) / 3)]


def test_wilder_smooth_short_input_returns_empty():
    assert ind._wilder_smooth([1, 2], 3) == []


def test_atr_constant_range():
    n = 30
    highs = [101.0] * n
    lows = [99.0] * n
    closes = [100.0] * n
    assert ind.atr(highs, lows, closes) == pytest.approx(2.0)


def test_atr_accounts_for_gaps_via_previous_close():
    # Bar 2 gaps up from a 10 close: true range must be |high - prev_close| = 10,
    # not just high - low = 1. With period=1 the ATR is the latest true range.
    assert ind.atr([10, 20], [9, 19], [10, 20], period=1) == pytest.approx(10.0)


def test_atr_needs_enough_data():
    assert ind.atr([1] * 5, [1] * 5, [1] * 5) is None


# ---- Donchian / volume ratio ---------------------------------------------

def test_donchian_uses_last_period_bars_only():
    highs = [100] + [10, 11, 12, 13, 14]
    lows = [0] + [5, 6, 7, 8, 9]
    assert ind.donchian_channel(highs, lows, period=5) == (14, 5)


def test_donchian_insufficient_data():
    assert ind.donchian_channel([1, 2], [1, 2], period=5) == (None, None)


def test_volume_ratio_excludes_current_bar_from_average():
    vols = [10.0] * 15 + [30.0]
    assert ind.volume_ratio(vols, period=15) == 3.0


def test_volume_ratio_zero_average_is_none():
    assert ind.volume_ratio([0.0] * 15 + [5.0], period=15) is None


def test_volume_ratio_insufficient_data():
    assert ind.volume_ratio([1.0] * 15, period=15) is None


# ---- ADX -----------------------------------------------------------------

def _random_walk(n, seed, drift=0.0, vol=1.0):
    rng = random.Random(seed)
    price, highs, lows, closes = 100.0, [], [], []
    for _ in range(n):
        price += drift + rng.gauss(0, vol)
        spread = abs(rng.gauss(0, vol * 0.5)) + 0.01
        highs.append(price + spread)
        lows.append(price - spread)
        closes.append(price)
    return highs, lows, closes


def test_adx_needs_enough_data():
    h, l, c = _random_walk(20, seed=1)
    assert ind.adx(h, l, c) == (None, None)


@pytest.mark.parametrize("seed", range(10))
def test_adx_stays_within_0_to_100(seed):
    """Regression: the old raw-sum seeding pushed ADX to 100-800+."""
    h, l, c = _random_walk(120, seed=seed, drift=0.3)
    value, rising = ind.adx(h, l, c)
    assert 0 <= value <= 100
    assert isinstance(rising, bool)


def test_adx_higher_in_trend_than_in_chop():
    h1, l1, c1 = _random_walk(150, seed=7, drift=1.5, vol=0.6)
    h2, l2, c2 = _random_walk(150, seed=7, drift=0.0, vol=1.5)
    trend, _ = ind.adx(h1, l1, c1)
    chop, _ = ind.adx(h2, l2, c2)
    assert trend > chop


# ---- Trend direction ------------------------------------------------------

def test_trend_direction():
    assert ind.trend_direction(list(range(1, 60))) == "UPTREND"
    assert ind.trend_direction(list(range(60, 1, -1))) == "DOWNTREND"
    assert ind.trend_direction([5.0] * 40) == "RANGING"
    assert ind.trend_direction([1.0] * 10) == "UNKNOWN"
