"""
backtest.py - Replay historical candles through the live strategy.

Usage:
    python fetch_history.py          # pull candles into ./data/
    python backtest.py               # run with current settings
    python backtest.py --grid        # sweep stop/target multiples
    python backtest.py --no-costs    # show the fantasy version

Design notes that matter for trusting the output:

  - Imports SignalEngine from strategy.py, the same module the live
    trader uses. No duplicated logic means no silent drift.

  - Signals are computed on CLOSED candles only, and the fill happens at
    the NEXT candle's open. Filling at the signal candle's close is
    look-ahead bias and is the single most common way backtests lie.

  - Stops and targets are checked against the following bars' high/low.
    When a bar's range spans both the stop and the target we assume the
    STOP hit first. That is pessimistic, and pessimistic is correct -
    you cannot tell intrabar order from OHLC data, and assuming the
    favourable one is how backtests manufacture fake profit.

  - Every fill pays fees and crosses the spread via costs.py.
"""

import os
import csv
import argparse
from dataclasses import dataclass, field

import strategy as st
from costs import costs_for, edge_gate


DATA_DIR = "data"


@dataclass
class Trade:
    asset: str
    direction: str
    entry_idx: int
    entry: float
    stop: float
    target: float
    qty: float
    exit_idx: int = 0
    exit_ts: float = 0.0   # bar timestamp of the exit (bar index if no time data)
    exit: float = 0.0
    pnl: float = 0.0
    fees: float = 0.0
    reason: str = ""


@dataclass
class Result:
    trades: list = field(default_factory=list)
    equity: list = field(default_factory=list)
    blocked: int = 0   # entries rejected by the post-cost edge gate

    def summary(self, starting_capital: float) -> dict:
        n = len(self.trades)
        if n == 0:
            return {"trades": 0, "net_pnl": 0.0, "win_rate": 0.0,
                    "profit_factor": 0.0, "max_dd_pct": 0.0,
                    "expectancy": 0.0, "fees": 0.0, "return_pct": 0.0}

        wins   = [t for t in self.trades if t.pnl > 0]
        losses = [t for t in self.trades if t.pnl <= 0]
        gross_w = sum(t.pnl for t in wins)
        gross_l = abs(sum(t.pnl for t in losses))
        net     = sum(t.pnl for t in self.trades)
        fees    = sum(t.fees for t in self.trades)

        peak, max_dd = starting_capital, 0.0
        for eq in self.equity:
            peak   = max(peak, eq)
            max_dd = max(max_dd, (peak - eq) / peak if peak > 0 else 0)

        return {
            "trades":        n,
            "net_pnl":       net,
            "return_pct":    net / starting_capital * 100,
            "win_rate":      len(wins) / n * 100,
            "avg_win":       gross_w / len(wins) if wins else 0.0,
            "avg_loss":      gross_l / len(losses) if losses else 0.0,
            "profit_factor": gross_w / gross_l if gross_l > 0 else float("inf"),
            "expectancy":    net / n,
            "max_dd_pct":    max_dd * 100,
            "fees":          fees,
        }


def load_candles(asset: str) -> list:
    path = os.path.join(DATA_DIR, f"{asset}.csv")
    if not os.path.exists(path):
        return []
    out = []
    with open(path) as f:
        for row in csv.DictReader(f):
            candle = {
                "open":   float(row["open"]),
                "high":   float(row["high"]),
                "low":    float(row["low"]),
                "close":  float(row["close"]),
                "volume": float(row["volume"]),
            }
            # Older CSVs have no time column; fall back gracefully.
            if row.get("time"):
                candle["time"] = int(row["time"])
            out.append(candle)
    return out


def run_asset(asset: str, candles: list, capital: float,
              stop_mult: float, target_mult: float,
              risk_per_trade: float = 0.01,
              max_notional: float = 10_000.0,
              apply_costs: bool = True,
              apply_edge_gate: bool = True,
              allow_short: bool = False) -> Result:

    is_crypto = asset in st.CRYPTO_ASSETS
    model     = costs_for(is_crypto)
    engine    = st.SignalEngine([asset])
    res       = Result()

    open_trade = None
    equity     = capital
    blocked_by_gate = 0

    for i, candle in enumerate(candles):
        engine.add_candle(asset, candle)

        # ---- manage an open position against THIS bar's range -----------
        if open_trade is not None:
            t = open_trade
            hit_stop = (candle["low"] <= t.stop if t.direction == "LONG"
                        else candle["high"] >= t.stop)
            hit_tgt  = (candle["high"] >= t.target if t.direction == "LONG"
                        else candle["low"] <= t.target)

            exit_px, reason = None, ""
            # Pessimistic: if both are touched in one bar, assume the stop.
            if hit_stop:
                # If the bar GAPPED through the stop, the stop price never
                # traded - you get the open, which is worse.
                gapped = (min(t.stop, candle["open"]) if t.direction == "LONG"
                          else max(t.stop, candle["open"]))
                exit_px, reason = gapped, "STOP"
            elif hit_tgt:
                exit_px, reason = t.target, "TARGET"

            if exit_px is not None:
                if apply_costs:
                    exit_px = model.exit_price(
                        exit_px, is_buy_to_close=(t.direction == "SHORT")
                    )
                gross = ((exit_px - t.entry) * t.qty if t.direction == "LONG"
                         else (t.entry - exit_px) * t.qty)
                fee = model.fee_on(t.entry * t.qty) + model.fee_on(exit_px * t.qty) \
                    if apply_costs else 0.0

                t.exit, t.exit_idx, t.reason = exit_px, i, reason
                t.exit_ts = candle.get("time", i)
                t.fees = fee
                t.pnl  = gross - fee
                equity += t.pnl
                res.trades.append(t)
                res.equity.append(equity)
                open_trade = None

        if not engine.ready(asset) or i + 1 >= len(candles):
            continue
        if open_trade is not None:
            continue

        signal, idict = engine.analyse(
            asset, candle["close"], vwap=None, allow_short=allow_short
        )
        if signal == "HOLD":
            continue

        atr = idict.get("atr") or 0.0
        if atr <= 0:
            continue

        # ---- fill at NEXT bar's open, never this bar's close ------------
        nxt      = candles[i + 1]
        mid      = nxt["open"]
        is_long  = signal == "BUY"

        if apply_edge_gate:
            ok, _reason, _e = edge_gate(
                mid, atr, stop_mult, target_mult, is_crypto
            )
            if not ok:
                blocked_by_gate += 1
                continue

        entry = model.entry_price(mid, is_buy=is_long) if apply_costs else mid

        risk   = equity * risk_per_trade
        stop_d = stop_mult * atr
        qty    = min(risk / stop_d, max_notional / entry) if stop_d > 0 else 0
        if qty <= 0:
            continue

        direction = "LONG" if is_long else "SHORT"
        stop   = (entry - stop_d if is_long else entry + stop_d)
        target = (entry + target_mult * atr if is_long
                  else entry - target_mult * atr)

        open_trade = Trade(
            asset=asset, direction=direction, entry_idx=i + 1,
            entry=entry, stop=stop, target=target, qty=qty,
        )

    res.blocked = blocked_by_gate
    return res


def combine_trades(trades: list, capital: float) -> Result:
    """
    Merge trades from every asset into ONE chronological equity curve.

    Trades are ordered by exit time. Concatenating per-asset lists instead
    (all BTC trades, then all ETH trades, ...) produces an equity curve that
    never existed, so the max drawdown computed from it is meaningless.
    """
    ordered = sorted(trades, key=lambda t: t.exit_ts)   # stable for ties
    equity, curve = capital, []
    for t in ordered:
        equity += t.pnl
        curve.append(equity)
    return Result(trades=ordered, equity=curve)


def run_all(stop_mult, target_mult, apply_costs=True, apply_edge_gate=True,
            capital=100_000.0, verbose=True) -> dict:
    all_trades = []
    per_asset = {}
    missing_time = False

    for asset in st.CRYPTO_ASSETS:
        candles = load_candles(asset)
        if len(candles) < st.CANDLES_NEEDED + 5:
            continue
        r = run_asset(asset, candles, capital, stop_mult, target_mult,
                      apply_costs=apply_costs, apply_edge_gate=apply_edge_gate)
        s = r.summary(capital)
        per_asset[asset] = s
        all_trades.extend(r.trades)
        missing_time = missing_time or "time" not in candles[0]
        if verbose and s["trades"]:
            print(f"  {asset:<5} {s['trades']:>4} trades | "
                  f"WR {s['win_rate']:>5.1f}% | "
                  f"PF {s['profit_factor']:>5.2f} | "
                  f"net ${s['net_pnl']:>+10,.2f} | "
                  f"fees ${s['fees']:>8,.2f} | "
                  f"blocked {r.blocked}")

    if verbose and missing_time:
        print("  NOTE: CSVs have no time column, so trades are ordered by bar "
              "index and combined drawdown is approximate.\n"
              "        Re-run fetch_history.py to fix.")
    combined = combine_trades(all_trades, capital)
    return {"combined": combined.summary(capital), "per_asset": per_asset}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--no-costs", action="store_true",
                    help="disable fees/slippage (shows the fantasy version)")
    ap.add_argument("--no-gate", action="store_true",
                    help="disable the post-cost edge gate")
    ap.add_argument("--grid", action="store_true",
                    help="sweep stop/target multiples")
    ap.add_argument("--capital", type=float, default=100_000.0)
    args = ap.parse_args()

    if not os.path.isdir(DATA_DIR):
        print(f"No {DATA_DIR}/ directory. Run: python fetch_history.py")
        return

    apply_costs = not args.no_costs
    apply_gate  = not args.no_gate

    if args.grid:
        print("Sweeping stop/target multiples (costs "
              f"{'ON' if apply_costs else 'OFF'}, gate "
              f"{'ON' if apply_gate else 'OFF'})\n")
        print(f"{'stop':>6} {'target':>7} {'trades':>7} {'WR%':>7} "
              f"{'PF':>7} {'net P&L':>13} {'maxDD%':>8}")
        rows = []
        for sm in [1.0, 1.5, 2.0, 2.5]:
            for tm in [2.0, 3.0, 4.0, 6.0, 8.0]:
                if tm <= sm:
                    continue
                out = run_all(sm, tm, apply_costs, apply_gate,
                              args.capital, verbose=False)
                c = out["combined"]
                rows.append((sm, tm, c))
                print(f"{sm:>6.1f} {tm:>7.1f} {c['trades']:>7} "
                      f"{c['win_rate']:>6.1f}% {c['profit_factor']:>7.2f} "
                      f"${c['net_pnl']:>+12,.2f} {c['max_dd_pct']:>7.1f}%")
        print("\nTreat the best cell with suspicion. Sweeping a grid and")
        print("picking the winner is overfitting unless it holds up on data")
        print("you did not sweep over. Re-run on a different date range.")
        return

    print(f"Backtest | stop {st.STOP_ATR_MULT}x  target {st.TARGET_ATR_MULT}x | "
          f"costs {'ON' if apply_costs else 'OFF'} | "
          f"gate {'ON' if apply_gate else 'OFF'}\n")
    out = run_all(st.STOP_ATR_MULT, st.TARGET_ATR_MULT,
                  apply_costs, apply_gate, args.capital)
    c = out["combined"]
    print(f"\n{'='*62}")
    print(f"  Trades        : {c['trades']}")
    print(f"  Win rate      : {c['win_rate']:.1f}%")
    print(f"  Profit factor : {c['profit_factor']:.2f}")
    print(f"  Expectancy    : ${c['expectancy']:+,.2f} per trade")
    print(f"  Net P&L       : ${c['net_pnl']:+,.2f}  ({c['return_pct']:+.2f}%)")
    print(f"  Fees paid     : ${c['fees']:,.2f}")
    print(f"  Max drawdown  : {c['max_dd_pct']:.2f}%")
    print(f"{'='*62}")
    if c["trades"] and c["net_pnl"] <= 0:
        print("\n  Negative expectancy. Do not trade this configuration.")


if __name__ == "__main__":
    main()
