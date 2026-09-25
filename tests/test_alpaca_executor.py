"""AlpacaExecutor tests using a fake client - no network, no real keys.

AlpacaExecutor.__init__ requires env vars and a live network call, so these
tests build the object with __new__ and set .client directly.
"""
from alpaca.trading.enums import OrderSide

import trader


class FakePosition:
    def __init__(self, qty):
        self.qty = qty


class FakeClient:
    def __init__(self, positions=None, fail=False):
        self.orders = []
        self.positions = positions or {}
        self.fail = fail

    def get_open_position(self, symbol):
        if symbol not in self.positions:
            raise Exception(f"no position for {symbol}")
        return FakePosition(self.positions[symbol])

    def submit_order(self, order):
        if self.fail:
            raise Exception("network down")
        self.orders.append(order)
        return type("R", (), {"id": "fake-order-id-1234"})()


def _executor(**kw):
    ex = trader.AlpacaExecutor.__new__(trader.AlpacaExecutor)
    ex.client = FakeClient(**kw)
    return ex


# ---- opening -----------------------------------------------------------

def test_open_long_places_a_buy_for_the_computed_qty():
    ex = _executor()
    assert ex.open_position("AAPL", 50.0, "LONG") is True
    assert [(o.side, o.qty) for o in ex.client.orders] == [(OrderSide.BUY, 50.0)]


def test_open_short_places_a_sell_for_the_computed_qty():
    """Regression: opening a short used to be routed through the
    close-a-position path, which found no existing position and silently
    skipped the order while still reporting success."""
    ex = _executor()
    assert ex.open_position("AAPL", 50.0, "SHORT") is True
    assert [(o.side, o.qty) for o in ex.client.orders] == [(OrderSide.SELL, 50.0)]


def test_open_zero_qty_is_rejected():
    ex = _executor()
    assert ex.open_position("AAPL", 0.0, "LONG") is False
    assert ex.client.orders == []


# ---- closing -------------------------------------------------------------

def test_close_long_fetches_real_qty_not_the_caller_qty():
    ex = _executor(positions={"AAPL": 78.6817})
    assert ex.close_position("AAPL", "LONG") is True
    order = ex.client.orders[0]
    assert order.side == OrderSide.SELL
    assert order.qty == 78.6817


def test_close_short_fetches_real_qty_and_buys():
    """Regression: closing a short passed a hardcoded qty=0 BUY order
    because the old code only special-cased SELL as 'the closing side'."""
    ex = _executor(positions={"AAPL": -50.0})
    assert ex.close_position("AAPL", "SHORT") is True
    order = ex.client.orders[0]
    assert order.side == OrderSide.BUY
    assert order.qty == 50.0   # magnitude, not the raw negative short qty


def test_close_with_no_open_position_skips_gracefully():
    ex = _executor()
    assert ex.close_position("AAPL", "LONG") is True
    assert ex.client.orders == []


def test_close_order_failure_is_reported_as_failure():
    ex = _executor(positions={"AAPL": 50.0}, fail=True)
    assert ex.close_position("AAPL", "LONG") is False


# ---- OvernightTrader._close keeps state on a real order failure ----------

def _bare_trader(alpaca):
    t = trader.OvernightTrader.__new__(trader.OvernightTrader)
    t.alpaca = alpaca
    logger = trader.OvernightLogger.__new__(trader.OvernightLogger)
    logger.log = lambda *a, **k: None
    logger.log_trade = lambda *a, **k: None
    t.portfolio = trader.Portfolio(logger, 100_000.0)
    t.trades = trader.TradeManager()
    return t


def test_close_keeps_internal_state_open_when_the_real_order_fails():
    """A failed close must not be booked internally - otherwise the bot
    loses track of a position Alpaca still holds and can pile a new entry
    on top of it."""
    t = _bare_trader(_executor(positions={"AAPL": 50.0}, fail=True))
    t.portfolio.open("AAPL", 50.0, 200.0, 2.0, "LONG")
    t.trades.open("AAPL", 200.0, 2.0, "LONG")

    t._close("AAPL", 205.0, "TARGET_HIT")

    assert t.portfolio.has("AAPL")
    assert t.trades.has("AAPL")


def test_close_clears_internal_state_when_the_real_order_succeeds():
    t = _bare_trader(_executor(positions={"AAPL": 50.0}))
    t.portfolio.open("AAPL", 50.0, 200.0, 2.0, "LONG")
    t.trades.open("AAPL", 200.0, 2.0, "LONG")

    t._close("AAPL", 205.0, "TARGET_HIT")

    assert not t.portfolio.has("AAPL")
    assert not t.trades.has("AAPL")
