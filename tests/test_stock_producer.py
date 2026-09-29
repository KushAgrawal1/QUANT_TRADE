from types import SimpleNamespace

import pytest

from stock_producer import STOCKS, TOPIC, build_stock_event


def _trade(symbol="AAPL", price=190.5, size=10, timestamp="2026-01-01T00:00:00Z"):
    return SimpleNamespace(symbol=symbol, price=price, size=size, timestamp=timestamp)


def test_build_stock_event_shape():
    event = build_stock_event(_trade())
    assert event == {
        "asset": "AAPL", "price": 190.5, "volume": 10.0,
        "timestamp": "2026-01-01T00:00:00Z", "asset_class": "stock",
    }


def test_build_stock_event_coerces_price_and_volume_to_float():
    event = build_stock_event(_trade(price="190.5", size="10"))
    assert event["price"] == 190.5 and isinstance(event["price"], float)
    assert event["volume"] == 10.0 and isinstance(event["volume"], float)


def test_build_stock_event_raises_on_missing_attribute():
    """The caller (handle_trade) is responsible for catching this - the
    pure function should fail loudly, not silently invent a value."""
    bad = SimpleNamespace(price=1.0, size=1)   # no .symbol
    with pytest.raises(AttributeError):
        build_stock_event(bad)


def test_importing_this_module_does_not_touch_the_network():
    """Regression: this module used to run its entire live stream loop at
    import time (no __main__ guard) - constructing a real StockDataStream
    with no credentials and hammering Kafka on localhost:29092 the moment
    anything imported it. Reaching this line at all is the test."""
    assert callable(build_stock_event)
    assert STOCKS == ["AAPL", "NVDA", "TSLA", "AMZN"]
    assert TOPIC == "stock-trades"

