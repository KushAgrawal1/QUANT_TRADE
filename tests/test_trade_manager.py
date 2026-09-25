import trader as tr
from trader import TradeManager


def test_open_long_computes_stop_target_and_breakeven_trigger():
    tm = TradeManager()
    tm.open("BTC", entry=100.0, atr=2.0, direction="LONG")
    assert tm.stop_price("BTC") == 100.0 - tr.STOP_ATR_MULT * 2.0
    assert tm.target_price("BTC") == 100.0 + tr.TARGET_ATR_MULT * 2.0
    assert tm.direction("BTC") == "LONG"
    assert tm.has("BTC")


def test_open_short_mirrors_the_direction():
    tm = TradeManager()
    tm.open("AAPL", entry=100.0, atr=2.0, direction="SHORT")
    assert tm.stop_price("AAPL") == 100.0 + tr.STOP_ATR_MULT * 2.0
    assert tm.target_price("AAPL") == 100.0 - tr.TARGET_ATR_MULT * 2.0


def test_check_tick_long_hits_target_and_stop():
    tm = TradeManager()
    tm.open("BTC", entry=100.0, atr=2.0, direction="LONG")
    assert tm.check_tick("BTC", tm.target_price("BTC")) == "TARGET"
    assert tm.check_tick("BTC", tm.stop_price("BTC")) == "STOP"
    assert tm.check_tick("BTC", 100.5) == "HOLD"


def test_check_tick_short_hits_target_and_stop():
    tm = TradeManager()
    tm.open("AAPL", entry=100.0, atr=2.0, direction="SHORT")
    assert tm.check_tick("AAPL", tm.target_price("AAPL")) == "TARGET"
    assert tm.check_tick("AAPL", tm.stop_price("AAPL")) == "STOP"


def test_check_tick_with_no_position_is_hold():
    assert TradeManager().check_tick("BTC", 100.0) == "HOLD"


def test_breakeven_moves_stop_to_entry_once_triggered():
    tm = TradeManager()
    tm.open("BTC", entry=100.0, atr=2.0, direction="LONG")
    be = 100.0 + tr.STOP_ATR_MULT * 2.0
    tm.update_breakeven("BTC", be - 0.01)
    assert tm.stop_price("BTC") != 100.0        # not yet
    tm.update_breakeven("BTC", be)
    assert tm.stop_price("BTC") == 100.0        # moved to entry


def test_breakeven_only_fires_once():
    tm = TradeManager()
    tm.open("BTC", entry=100.0, atr=2.0, direction="LONG")
    be = 100.0 + tr.STOP_ATR_MULT * 2.0
    tm.update_breakeven("BTC", be)
    tm.trades["BTC"]["stop"] = 55.0   # simulate a later trailing update
    tm.update_breakeven("BTC", be + 100)   # must not clobber it again
    assert tm.stop_price("BTC") == 55.0


def test_adx_exit_requires_breakeven_stage_and_low_adx():
    tm = TradeManager()
    tm.open("BTC", entry=100.0, atr=2.0, direction="LONG")
    assert not tm.check_adx_exit("BTC", tr.ADX_EXIT_LEVEL - 1)   # stage 0
    tm.update_breakeven("BTC", tm.trades["BTC"]["be_trigger"])
    assert not tm.check_adx_exit("BTC", tr.ADX_EXIT_LEVEL + 1)   # ADX still ok
    assert tm.check_adx_exit("BTC", tr.ADX_EXIT_LEVEL - 1)
    assert not tm.check_adx_exit("BTC", None)


def test_clear_removes_the_trade():
    tm = TradeManager()
    tm.open("BTC", entry=100.0, atr=2.0, direction="LONG")
    tm.clear("BTC")
    assert not tm.has("BTC")
    assert tm.direction("BTC") == "LONG"   # documented default for a missing asset
