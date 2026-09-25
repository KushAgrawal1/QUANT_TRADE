from trader import CandleBuilder, VWAPTracker


# ---- CandleBuilder ---------------------------------------------------------

def test_first_tick_starts_a_candle_and_returns_none():
    cb = CandleBuilder()
    assert cb.update("BTC", 100.0, volume=1.0) is None


def test_same_bucket_ticks_update_ohlcv_without_completing():
    cb = CandleBuilder()
    cb.update("BTC", 100.0, volume=1.0)
    out = cb.update("BTC", 105.0, volume=2.0)
    assert out is None
    c = cb.current["BTC"]
    assert c["high"] == 105.0 and c["low"] == 100.0
    assert c["close"] == 105.0 and c["volume"] == 3.0


def test_new_bucket_returns_the_completed_prior_candle():
    cb = CandleBuilder()
    cb.update("BTC", 100.0, volume=1.0)
    cb.update("BTC", 90.0, volume=1.0)
    cb.current["BTC"]["key"] = "force-new-bucket"
    completed = cb.update("BTC", 200.0, volume=5.0)
    assert completed == {"open": 100.0, "high": 100.0, "low": 90.0,
                         "close": 90.0, "volume": 2.0}
    # the new candle has started fresh
    assert cb.current["BTC"]["open"] == 200.0 and cb.current["BTC"]["volume"] == 5.0


def test_assets_are_tracked_independently():
    cb = CandleBuilder()
    cb.update("BTC", 100.0)
    cb.update("ETH", 3000.0)
    assert cb.current["BTC"]["open"] == 100.0
    assert cb.current["ETH"]["open"] == 3000.0


# ---- VWAPTracker ------------------------------------------------------------

def _candle(h, l, c, v):
    return {"high": h, "low": l, "close": c, "volume": v}


def test_vwap_is_volume_weighted_typical_price():
    vt = VWAPTracker()
    vt.update("AAPL", _candle(102, 98, 100, 10))   # typical 100
    out = vt.update("AAPL", _candle(112, 108, 110, 30))  # typical 110
    # (100*10 + 110*30) / 40 = 107.5
    assert out == 107.5


def test_vwap_is_none_with_zero_cumulative_volume():
    vt = VWAPTracker()
    assert vt.update("AAPL", _candle(101, 99, 100, 0)) is None


def test_vwap_resets_on_a_new_day(monkeypatch):
    import trader
    vt = VWAPTracker()
    vt.update("AAPL", _candle(101, 99, 100, 10))

    class FakeDT:
        @staticmethod
        def now(tz):
            import datetime as real_dt
            return real_dt.datetime(2026, 1, 2, tzinfo=tz)

    monkeypatch.setattr(trader, "datetime", FakeDT)
    out = vt.update("AAPL", _candle(201, 199, 200, 5))
    assert out == 200.0   # only today's bar counted, not yesterday's
