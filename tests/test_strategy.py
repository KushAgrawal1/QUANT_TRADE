import strategy as st
from tests.helpers import feed, make_candles


def _engine(asset="BTC", candles=None):
    eng = st.SignalEngine([asset])
    feed(eng, asset, candles if candles is not None else make_candles("up"))
    return eng


# ---- warmup ---------------------------------------------------------------

def test_not_ready_until_enough_candles():
    eng = st.SignalEngine(["BTC"])
    candles = make_candles("up")
    feed(eng, "BTC", candles[: st.CANDLES_NEEDED - 1])
    assert not eng.ready("BTC")
    assert eng.collected("BTC") == st.CANDLES_NEEDED - 1
    eng.add_candle("BTC", candles[st.CANDLES_NEEDED - 1])
    assert eng.ready("BTC")


def test_window_is_capped():
    eng = st.SignalEngine(["BTC"])
    feed(eng, "BTC", make_candles("up", base_len=200))
    assert eng.collected("BTC") == st.CANDLE_WINDOW


# ---- long entries ---------------------------------------------------------

def test_buy_on_full_breakout_setup():
    candles = make_candles("up")
    eng = _engine(candles=candles)
    signal, info = eng.analyse("BTC", candles[-1]["close"])
    assert signal == "BUY"
    assert info["trend_ok"] and info["breakout"] and info["vol_ok"] and info["ema_ok"]
    assert info["adx"] > st.ADX_THRESHOLD
    assert info["atr"] > 0


def test_hold_without_volume_confirmation():
    candles = make_candles("up", last_volume=100.0)
    eng = _engine(candles=candles)
    signal, info = eng.analyse("BTC", candles[-1]["close"])
    assert signal == "HOLD"
    assert not info["vol_ok"]


def test_hold_when_price_has_not_broken_the_channel():
    candles = make_candles("up")
    eng = _engine(candles=candles)
    _, info = eng.analyse("BTC", candles[-1]["close"])
    signal, _ = eng.analyse("BTC", info["don_high"] - 0.01)
    assert signal == "HOLD"


def test_hold_in_a_choppy_market():
    # Only the sideways base: ADX should be below threshold.
    candles = make_candles("up", trend_len=0, base_len=40)
    eng = _engine(candles=candles)
    signal, info = eng.analyse("BTC", candles[-1]["close"] + 5)
    assert signal == "HOLD"
    assert not info["trend_ok"]


def test_breakout_is_measured_against_prior_range_not_current_candle():
    """The channel excludes the current bar, otherwise price can never exceed it."""
    candles = make_candles("up")
    eng = _engine(candles=candles)
    _, info = eng.analyse("BTC", candles[-1]["close"])
    prior_highs = [c["high"] for c in candles[:-1]][-st.DONCHIAN_PERIOD:]
    assert info["don_high"] == round(max(prior_highs), 6)


# ---- shorts ---------------------------------------------------------------

def test_crypto_never_shorts_by_default():
    candles = make_candles("down")
    eng = _engine("BTC", candles)
    signal, _ = eng.analyse("BTC", candles[-1]["close"])
    assert signal == "HOLD"


def test_crypto_can_short_when_explicitly_allowed():
    candles = make_candles("down")
    eng = _engine("BTC", candles)
    signal, _ = eng.analyse("BTC", candles[-1]["close"], allow_short=True)
    assert signal == "SHORT"


def test_stocks_can_short_below_vwap():
    candles = make_candles("down")
    eng = _engine("AAPL", candles)
    price = candles[-1]["close"]
    signal, _ = eng.analyse("AAPL", price, vwap=price + 5)
    assert signal == "SHORT"


def test_stock_short_blocked_above_vwap():
    candles = make_candles("down")
    eng = _engine("AAPL", candles)
    price = candles[-1]["close"]
    signal, _ = eng.analyse("AAPL", price, vwap=price - 5)
    assert signal == "HOLD"


# ---- VWAP on longs --------------------------------------------------------

def test_stock_long_requires_price_above_vwap():
    candles = make_candles("up")
    eng = _engine("AAPL", candles)
    price = candles[-1]["close"]
    assert eng.analyse("AAPL", price, vwap=price - 5)[0] == "BUY"
    assert eng.analyse("AAPL", price, vwap=price + 5)[0] == "HOLD"


# ---- recent_candles -------------------------------------------------------

def test_recent_candles_keep_real_opens():
    """Regression: opens used to be filled with closes, so every bar looked
    like a doji when shown to the AI validator."""
    candles = make_candles("up")
    eng = _engine(candles=candles)
    recent = eng.recent_candles("BTC")
    assert len(recent) == 5
    assert [r["open"] for r in recent] == [c["open"] for c in candles[-5:]]
    assert any(r["open"] != r["close"] for r in recent)


def test_recent_candles_fall_back_to_close_when_open_missing():
    eng = st.SignalEngine(["BTC"])
    eng.add_candle("BTC", {"high": 2.0, "low": 1.0, "close": 1.5})
    assert eng.recent_candles("BTC")[0]["open"] == 1.5


def test_adx_value_matches_analyse():
    candles = make_candles("up")
    eng = _engine(candles=candles)
    _, info = eng.analyse("BTC", candles[-1]["close"])
    assert eng.adx_value("BTC") == info["adx"]


def test_stock_signals_fail_closed_when_vwap_unavailable():
    """README: stocks require price on the correct side of VWAP. No VWAP
    (e.g. no volume yet today) must mean no trade, not a free pass."""
    up = make_candles("up")
    assert _engine("AAPL", up).analyse("AAPL", up[-1]["close"], vwap=None)[0] == "HOLD"
    down = make_candles("down")
    eng = _engine("AAPL", down)
    assert eng.analyse("AAPL", down[-1]["close"], vwap=None)[0] == "HOLD"


def test_crypto_ignores_vwap_when_none_is_supplied():
    candles = make_candles("up")
    eng = _engine("BTC", candles)
    assert eng.analyse("BTC", candles[-1]["close"], vwap=None)[0] == "BUY"
