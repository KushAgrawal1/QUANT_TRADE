"""
costs.py - Transaction cost model and pre-trade edge gate.

The strategy targets 3.0x ATR and stops at 1.5x ATR, which looks like a
2:1 reward:risk ratio. It is not. Every round trip pays fees twice and
crosses the spread twice, and those costs are subtracted from the win
while being ADDED to the loss. On small-ATR instruments this inverts the
edge completely.

Worked example at 0.20% ATR (typical for 3-minute BTC):
    gross win  = 3.0 * 0.20% = 0.60%   net win  = 0.60% - 0.60% = 0.00%
    gross loss = 1.5 * 0.20% = 0.30%   net loss = 0.30% + 0.60% = 0.90%
    -> real R:R = 0.00:1, breakeven win rate = 100%

No signal quality can rescue that. The only fixes are to trade instruments
whose ATR is large enough to clear costs, or to lower costs. This module
enforces the former as a hard pre-trade gate.
"""

from dataclasses import dataclass


# Alpaca crypto is taker-priced per side. Stocks are commission-free but
# still pay the spread. Slippage is the half-spread a market order crosses.
# Measure these against your own fills and update - do not trust defaults.
CRYPTO_FEE_BPS      = 25.0
CRYPTO_SLIPPAGE_BPS = 5.0
STOCK_FEE_BPS       = 0.0
STOCK_SLIPPAGE_BPS  = 2.0

# Minimum acceptable reward:risk AFTER costs. Below this, skip the trade.
MIN_NET_RR = 1.20

# Minimum ATR as a fraction of price. This replaces the old dollar-based
# floor (STOP_ATR_MULT * atr < 0.50), which was meaningless: $0.50 on a
# $100,000 BTC price is 0.0005% and passed literally every trade.
MIN_ATR_PCT = 0.0040   # 0.40%


@dataclass
class CostModel:
    fee_bps: float
    slippage_bps: float

    @property
    def per_side_bps(self) -> float:
        return self.fee_bps + self.slippage_bps

    @property
    def round_trip_bps(self) -> float:
        return 2.0 * self.per_side_bps

    def round_trip_pct(self) -> float:
        return self.round_trip_bps / 10_000.0

    def entry_price(self, mid: float, is_buy: bool) -> float:
        """Price actually paid after crossing the spread."""
        adj = self.slippage_bps / 10_000.0
        return mid * (1 + adj) if is_buy else mid * (1 - adj)

    def exit_price(self, mid: float, is_buy_to_close: bool) -> float:
        adj = self.slippage_bps / 10_000.0
        return mid * (1 + adj) if is_buy_to_close else mid * (1 - adj)

    def fee_on(self, notional: float) -> float:
        return abs(notional) * (self.fee_bps / 10_000.0)


CRYPTO_COSTS = CostModel(CRYPTO_FEE_BPS, CRYPTO_SLIPPAGE_BPS)
STOCK_COSTS  = CostModel(STOCK_FEE_BPS, STOCK_SLIPPAGE_BPS)


def costs_for(is_crypto: bool) -> CostModel:
    return CRYPTO_COSTS if is_crypto else STOCK_COSTS


def net_edge(price: float, atr: float, stop_mult: float, target_mult: float,
             is_crypto: bool) -> dict:
    """
    Compute the true post-cost economics of a proposed trade.

    Returns net win/loss as fractions of price, the real reward:risk, and
    the win rate required to break even.
    """
    model    = costs_for(is_crypto)
    rt       = model.round_trip_pct()
    atr_pct  = atr / price if price > 0 else 0.0

    gross_win  = target_mult * atr_pct
    gross_loss = stop_mult   * atr_pct

    net_win  = gross_win  - rt
    net_loss = gross_loss + rt

    rr = (net_win / net_loss) if net_loss > 0 else 0.0
    breakeven_wr = (net_loss / (net_win + net_loss)) if (net_win + net_loss) > 0 else 1.0

    return {
        "atr_pct":        atr_pct,
        "round_trip_pct": rt,
        "gross_win":      gross_win,
        "gross_loss":     gross_loss,
        "net_win":        net_win,
        "net_loss":       net_loss,
        "net_rr":         rr,
        "breakeven_wr":   breakeven_wr,
    }


def edge_gate(price: float, atr: float, stop_mult: float, target_mult: float,
              is_crypto: bool) -> tuple[bool, str, dict]:
    """
    Hard pre-trade gate. Returns (allowed, reason, economics).

    Rejects any trade whose post-cost reward:risk cannot clear MIN_NET_RR,
    or whose ATR is too small a fraction of price to be worth trading.
    This is the check that was missing entirely.
    """
    e = net_edge(price, atr, stop_mult, target_mult, is_crypto)

    if e["atr_pct"] < MIN_ATR_PCT:
        return False, (
            f"ATR {e['atr_pct']*100:.3f}% < {MIN_ATR_PCT*100:.2f}% floor "
            f"- too small to clear {e['round_trip_pct']*100:.2f}% costs"
        ), e

    if e["net_win"] <= 0:
        return False, (
            f"net win {e['net_win']*100:.3f}% <= 0 after "
            f"{e['round_trip_pct']*100:.2f}% costs - unprofitable by construction"
        ), e

    if e["net_rr"] < MIN_NET_RR:
        return False, (
            f"net R:R {e['net_rr']:.2f}:1 < {MIN_NET_RR:.2f}:1 "
            f"(needs {e['breakeven_wr']*100:.0f}% win rate to break even)"
        ), e

    return True, (
        f"net R:R {e['net_rr']:.2f}:1, breakeven WR "
        f"{e['breakeven_wr']*100:.0f}%"
    ), e


def required_target_mult(atr_pct: float, stop_mult: float, is_crypto: bool,
                         target_rr: float = MIN_NET_RR) -> float:
    """
    Solve for the target multiple needed to achieve target_rr after costs.
    Useful for asking 'what would this instrument need to pay me?'
    """
    rt = costs_for(is_crypto).round_trip_pct()
    if atr_pct <= 0:
        return float("inf")
    net_loss = stop_mult * atr_pct + rt
    return (target_rr * net_loss + rt) / atr_pct


if __name__ == "__main__":
    print(f"Crypto round trip: {CRYPTO_COSTS.round_trip_bps:.0f} bps "
          f"({CRYPTO_COSTS.round_trip_pct()*100:.2f}%)")
    print(f"Stock round trip:  {STOCK_COSTS.round_trip_bps:.0f} bps "
          f"({STOCK_COSTS.round_trip_pct()*100:.2f}%)\n")

    print(f"{'ATR%':>7} {'net R:R':>9} {'breakeven WR':>13} {'tgt mult for 1.2:1':>20} {'gate':>8}")
    for atr_pct in [0.001, 0.002, 0.003, 0.005, 0.008, 0.012, 0.020]:
        price = 100_000.0
        atr   = price * atr_pct
        ok, _, e = edge_gate(price, atr, 1.5, 3.0, True)
        need = required_target_mult(atr_pct, 1.5, True)
        print(f"{atr_pct*100:>6.2f}% {e['net_rr']:>8.2f}:1 "
              f"{e['breakeven_wr']*100:>12.1f}% {need:>19.1f}x "
              f"{'PASS' if ok else 'BLOCK':>8}")
