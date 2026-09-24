import pytest

from costs import (CRYPTO_COSTS, STOCK_COSTS, MIN_NET_RR, edge_gate, net_edge,
                   required_target_mult)


def test_round_trip_costs():
    # Crypto: 2 * (25 fee + 5 slippage) = 60 bps = 0.60%
    assert CRYPTO_COSTS.round_trip_bps == 60.0
    assert CRYPTO_COSTS.round_trip_pct() == pytest.approx(0.006)
    # Stocks: commission-free, 2 * 2 bps slippage = 4 bps
    assert STOCK_COSTS.round_trip_bps == 4.0


def test_entry_price_crosses_the_spread_against_you():
    m = CRYPTO_COSTS
    assert m.entry_price(100.0, is_buy=True) > 100.0
    assert m.entry_price(100.0, is_buy=False) < 100.0
    assert m.entry_price(100.0, is_buy=True) == pytest.approx(100.05)


def test_exit_price_crosses_the_spread_against_you():
    m = CRYPTO_COSTS
    assert m.exit_price(100.0, is_buy_to_close=True) > 100.0    # closing a short
    assert m.exit_price(100.0, is_buy_to_close=False) < 100.0   # closing a long


def test_fee_is_proportional_and_sign_agnostic():
    assert CRYPTO_COSTS.fee_on(10_000) == pytest.approx(25.0)
    assert CRYPTO_COSTS.fee_on(-10_000) == pytest.approx(25.0)
    assert STOCK_COSTS.fee_on(10_000) == 0.0


def test_docstring_worked_example_zero_net_win_at_0_20pct_atr():
    """The module docstring's headline example: 0.20% ATR crypto has no edge."""
    price = 100_000.0
    e = net_edge(price, price * 0.002, 1.5, 3.0, is_crypto=True)
    assert e["gross_win"] == pytest.approx(0.006)
    assert e["net_win"] == pytest.approx(0.0, abs=1e-12)
    assert e["gross_loss"] == pytest.approx(0.003)
    assert e["net_loss"] == pytest.approx(0.009)
    assert e["net_rr"] == pytest.approx(0.0, abs=1e-9)
    assert e["breakeven_wr"] == pytest.approx(1.0)


def test_costs_worsen_the_loss_and_shrink_the_win():
    e = net_edge(100.0, 1.0, 1.5, 3.0, is_crypto=True)
    assert e["net_win"] < e["gross_win"]
    assert e["net_loss"] > e["gross_loss"]


def test_net_edge_handles_nonpositive_price():
    e = net_edge(0.0, 1.0, 1.5, 3.0, is_crypto=True)
    assert e["atr_pct"] == 0.0


# ---- edge_gate ------------------------------------------------------------

def test_gate_blocks_small_atr_on_crypto():
    ok, reason, _ = edge_gate(100_000.0, 100.0, 1.5, 3.0, is_crypto=True)  # 0.10%
    assert not ok
    assert "floor" in reason


def test_gate_regression_dollar_floor_no_longer_passes_everything():
    """The old floor was $0.50 in price terms, so BTC at ATR $50 passed."""
    ok, _, _ = edge_gate(100_000.0, 50.0, 1.5, 3.0, is_crypto=True)
    assert not ok


def test_gate_allows_large_atr_on_crypto():
    ok, reason, e = edge_gate(100_000.0, 1_200.0, 1.5, 3.0, is_crypto=True)  # 1.2%
    assert ok, reason
    assert e["net_rr"] >= MIN_NET_RR


def test_gate_blocks_when_net_win_nonpositive_even_above_atr_floor():
    # ATR 0.45% clears the 0.40% floor, but a 1.2x target is only 0.54% gross,
    # which does not cover the 0.60% round-trip cost.
    ok, reason, e = edge_gate(100.0, 0.45, 1.0, 1.2, is_crypto=True)
    assert not ok
    assert e["net_win"] < 0
    assert "unprofitable" in reason


def test_gate_blocks_on_low_net_rr():
    # 0.5% ATR crypto: net win 0.9%, net loss 1.35% -> R:R ~0.67, below 1.2
    ok, reason, e = edge_gate(100.0, 0.5, 1.5, 3.0, is_crypto=True)
    assert not ok
    assert e["net_rr"] < MIN_NET_RR
    assert "R:R" in reason


def test_stocks_pass_at_much_smaller_atr_than_crypto():
    # Same 0.45% ATR: fails for crypto, passes for commission-free stocks.
    crypto_ok, _, _ = edge_gate(100.0, 0.45, 1.5, 3.0, is_crypto=True)
    stock_ok, _, _ = edge_gate(100.0, 0.45, 1.5, 3.0, is_crypto=False)
    assert not crypto_ok
    assert stock_ok


# ---- required_target_mult -------------------------------------------------

@pytest.mark.parametrize("atr_pct", [0.004, 0.008, 0.02])
def test_required_target_mult_achieves_requested_rr(atr_pct):
    mult = required_target_mult(atr_pct, 1.5, True, target_rr=1.2)
    price = 100.0
    e = net_edge(price, price * atr_pct, 1.5, mult, True)
    assert e["net_rr"] == pytest.approx(1.2)


def test_required_target_mult_zero_atr_is_infinite():
    assert required_target_mult(0.0, 1.5, True) == float("inf")
