import pytest

from datetime import timedelta, timezone, datetime as real_datetime

import trader as tr
from trader import Portfolio


def _logger():
    lg = tr.OvernightLogger.__new__(tr.OvernightLogger)
    lg.filename = "fake_test_log.txt"
    lg.calls = []
    lg.log = lambda msg: lg.calls.append(("log", msg))
    lg.log_trade = lambda *a, **k: lg.calls.append(("trade", a, k))
    lg.log_summary = lambda *a, **k: lg.calls.append(("summary", a, k))
    return lg


def _pf(capital=100_000.0):
    return Portfolio(_logger(), capital)


# ---- sizing -----------------------------------------------------------------

def test_sizing_is_risk_bound_when_notional_cap_does_not_bind():
    p = _pf()
    qty = p.position_qty(price=100.0, atr=50.0)   # small price*qty -> cap doesn't bind
    risked = qty * tr.STOP_ATR_MULT * 50.0
    assert risked == pytest.approx(p.capital * tr.RISK_PER_TRADE, rel=0.02)
    assert qty * 100.0 < tr.MAX_NOTIONAL


def test_sizing_is_notional_capped_for_a_cheap_low_atr_asset():
    p = _pf()
    qty = p.position_qty(price=1.0, atr=0.001)
    assert qty * 1.0 == pytest.approx(tr.MAX_NOTIONAL, abs=1.0)


def test_sizing_is_halved_in_slow_mode():
    # Same capital both times - only in_slow_mode() differs - so the /2
    # factor is isolated instead of being confounded by a capital delta.
    p = _pf()
    p.in_slow_mode = lambda: False
    full = p.position_qty(100.0, 50.0)
    p.in_slow_mode = lambda: True
    half = p.position_qty(100.0, 50.0)
    assert half == pytest.approx(full / 2, rel=1e-3)  # both independently floor_qty-rounded


def test_atr_floor_prevents_division_by_a_near_zero_atr():
    p = _pf()
    assert p.position_qty(price=100.0, atr=0.0) > 0


# ---- can_open gating ----------------------------------------------------------

def test_can_open_fresh_portfolio():
    assert _pf().can_open("BTC") == (True, "ok")


def test_can_open_blocks_when_already_in_position():
    p = _pf()
    p.open("BTC", 1.0, 100.0, 2.0, "LONG")
    assert p.can_open("BTC")[0] is False


def test_can_open_blocks_at_max_positions():
    p = _pf()
    for i, asset in enumerate(["BTC", "ETH", "SOL"][: tr.MAX_POSITIONS]):
        p.open(asset, 1.0, 100.0, 2.0, "LONG")
    ok, reason = p.can_open("AAVE")
    assert not ok and "max" in reason


def test_can_open_respects_the_default_candle_cooldown():
    p = _pf()
    ok, reason = p.can_open("BTC")
    assert ok    # no cooldown recorded yet -> defaults to CANDLE_COOLDOWN, already satisfied


def test_can_open_blocks_during_loss_cooldown():
    p = _pf()
    p.open("BTC", 1.0, 100.0, 2.0, "LONG")
    p.close("BTC", 90.0, "STOP")            # a loss
    ok, reason = p.can_open("BTC")
    assert not ok and "loss cooldown" in reason
    for _ in range(tr.LOSS_COOLDOWN):
        p.tick_cooldowns()
    assert p.can_open("BTC")[0] is True


def test_can_open_blocks_during_win_cooldown_then_clears():
    """Regression: close() used to set cooldowns[asset] = WIN_COOLDOWN
    directly, which is already >= CANDLE_COOLDOWN, so the generic gate
    never blocked and re-entry was allowed immediately after a win -
    contradicting the documented "7 candles, no re-entry after profit"."""
    p = _pf()
    p.open("BTC", 1.0, 100.0, 2.0, "LONG")
    p.close("BTC", 110.0, "TARGET")          # a win
    ok, reason = p.can_open("BTC")
    assert not ok and "win cooldown" in reason
    for _ in range(tr.WIN_COOLDOWN):
        p.tick_cooldowns()
    assert p.can_open("BTC")[0] is True


# ---- closing / streaks --------------------------------------------------------

def test_close_updates_capital_and_pnl():
    p = _pf()
    p.open("BTC", 2.0, 100.0, 2.0, "LONG")
    pnl = p.close("BTC", 110.0, "TARGET")
    assert pnl == pytest.approx(20.0)
    assert p.capital == pytest.approx(100_020.0)
    assert p.total_pnl == pytest.approx(20.0)
    assert p.wins == 1 and p.losses == 0
    assert not p.has("BTC")


def test_close_on_a_short_flips_the_pnl_sign():
    p = _pf()
    p.open("AAPL", 2.0, 100.0, 2.0, "SHORT")
    pnl = p.close("AAPL", 90.0, "TARGET")
    assert pnl == pytest.approx(20.0)


def test_close_with_no_position_is_a_noop():
    assert _pf().close("BTC", 100.0, "TARGET") == 0


def test_consecutive_losses_trigger_a_pause():
    p = _pf()
    for i in range(tr.CONSECUTIVE_LOSS_LIMIT):
        p.open(f"A{i}", 1.0, 100.0, 2.0, "LONG")
        p.close(f"A{i}", 90.0, "STOP")
    assert p.is_paused()
    assert p.consecutive_loss == tr.CONSECUTIVE_LOSS_LIMIT


def test_a_win_resets_the_consecutive_loss_streak():
    p = _pf()
    p.open("BTC", 1.0, 100.0, 2.0, "LONG")
    p.close("BTC", 90.0, "STOP")
    p.open("ETH", 1.0, 100.0, 2.0, "LONG")
    p.close("ETH", 110.0, "TARGET")
    assert p.consecutive_loss == 0


def test_can_open_reports_remaining_pause_minutes_correctly():
    """Regression: `.seconds` on a timedelta wraps at 24h; total_seconds()
    is needed even though PAUSE_MINUTES is small today, so it stays right
    if that constant ever grows."""
    p = _pf()
    p.pause_until = real_datetime.now(timezone.utc) + timedelta(days=1, minutes=10)
    ok, reason = p.can_open("BTC")
    assert not ok
    assert "9min" in reason or "10min" in reason   # not near-zero from wraparound


def test_pause_expires_and_resets_the_streak():
    p = _pf()
    p.consecutive_loss = 3
    p.pause_until = real_datetime.now(timezone.utc) - timedelta(seconds=1)
    assert p.is_paused() is False
    assert p.pause_until is None
    assert p.consecutive_loss == 0


# ---- drawdown modes -----------------------------------------------------------

def test_slow_mode_and_halt_thresholds():
    p = _pf()
    assert not p.in_slow_mode() and not p.max_drawdown_hit()
    p.capital = p.slow_mode_floor - 1
    assert p.in_slow_mode() and not p.max_drawdown_hit()
    p.capital = p.halt_floor - 1
    assert p.max_drawdown_hit()


def test_dashboard_runs_and_logs_a_summary():
    p = _pf()
    p.open("BTC", 1.0, 100.0, 2.0, "LONG")
    p.close("BTC", 110.0, "TARGET")
    p.dashboard()   # must not raise
    assert any(c[0] == "summary" for c in p.logger.calls)
