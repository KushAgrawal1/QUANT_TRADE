import csv

import pytest

import backtest as bt
import indicators as ind
from tests.helpers import make_candles

CAPITAL = 100_000.0


def _setup():
    """Breakout base + derived numbers used to build follow-up candles."""
    base = make_candles("up")
    atr = ind.atr([c["high"] for c in base], [c["low"] for c in base],
                  [c["close"] for c in base])
    sig_close = base[-1]["close"]
    entry_open = sig_close + 3.0   # deliberately != signal close
    return base, atr, sig_close, entry_open


def _bar(o, h, l, c, v=100.0):
    return {"open": o, "high": h, "low": l, "close": c, "volume": v}


def _run(candles, capital=CAPITAL, costs=False, gate=False, **kw):
    return bt.run_asset("BTC", candles, capital, 1.5, 3.0,
                        apply_costs=costs, apply_edge_gate=gate, **kw)


# ---- entries --------------------------------------------------------------

def test_fill_happens_at_next_bar_open_not_signal_close():
    """Filling at the signal candle's close is look-ahead bias."""
    base, atr, sig_close, entry_open = _setup()
    fill_bar = _bar(entry_open, entry_open + .5, entry_open - .5, entry_open)
    target_bar = _bar(entry_open, entry_open + 3.1 * atr + 5, entry_open - .1,
                      entry_open + 3.1 * atr)
    r = _run(base + [fill_bar, target_bar])
    assert len(r.trades) == 1
    t = r.trades[0]
    assert t.entry == entry_open
    assert t.entry != sig_close
    assert t.entry_idx == len(base)


def test_no_trade_on_the_final_bar():
    """A signal on the last candle has no next open to fill at."""
    base, *_ = _setup()
    assert _run(base).trades == []


def test_stop_and_target_are_atr_multiples_of_entry():
    base, atr, _, entry_open = _setup()
    fill = _bar(entry_open, entry_open + .5, entry_open - .5, entry_open)
    t = _run(base + [fill, _bar(entry_open, entry_open + 100, entry_open - .1,
                                entry_open)]).trades[0]
    assert t.stop == pytest.approx(entry_open - 1.5 * atr)
    assert t.target == pytest.approx(entry_open + 3.0 * atr)


# ---- exits ----------------------------------------------------------------

def test_target_hit_is_a_win():
    base, atr, _, entry_open = _setup()
    fill = _bar(entry_open, entry_open + .5, entry_open - .5, entry_open)
    hit = _bar(entry_open, entry_open + 3.5 * atr, entry_open - .1, entry_open + 3 * atr)
    t = _run(base + [fill, hit]).trades[0]
    assert t.reason == "TARGET"
    assert t.exit == pytest.approx(entry_open + 3.0 * atr)
    assert t.pnl == pytest.approx((t.exit - t.entry) * t.qty)
    assert t.pnl > 0


def test_stop_hit_is_a_loss():
    base, atr, _, entry_open = _setup()
    fill = _bar(entry_open, entry_open + .5, entry_open - .5, entry_open)
    hit = _bar(entry_open, entry_open + .1, entry_open - 2 * atr, entry_open - atr)
    t = _run(base + [fill, hit]).trades[0]
    assert t.reason == "STOP"
    assert t.pnl < 0


def test_stop_gapped_through_fills_at_the_open_not_the_stop():
    """If a bar opens below the stop, the stop price never traded. The fill is
    the open. Filling at the stop flatters every gap-down loss."""
    base, atr, _, entry_open = _setup()
    stop = entry_open - 1.5 * atr
    fill = _bar(entry_open, entry_open + .5, entry_open - .5, entry_open)
    gap_open = stop - 5.0
    gap = _bar(gap_open, gap_open + .5, gap_open - 1.0, gap_open)
    t = _run(base + [fill, gap]).trades[0]
    assert t.reason == "STOP"
    assert t.exit == pytest.approx(gap_open)
    assert t.exit < t.stop
    assert t.pnl == pytest.approx((gap_open - t.entry) * t.qty)


def test_stop_not_gapped_still_fills_at_the_stop_price():
    base, atr, _, entry_open = _setup()
    stop = entry_open - 1.5 * atr
    fill = _bar(entry_open, entry_open + .5, entry_open - .5, entry_open)
    dip = _bar(entry_open, entry_open + .1, stop - 1.0, stop)
    t = _run(base + [fill, dip]).trades[0]
    assert t.exit == pytest.approx(stop)


def test_bar_touching_both_stop_and_target_assumes_stop_first():
    """OHLC can't tell intrabar order; assuming the good outcome manufactures profit."""
    base, atr, _, entry_open = _setup()
    fill = _bar(entry_open, entry_open + .5, entry_open - .5, entry_open)
    wide = _bar(entry_open, entry_open + 5 * atr, entry_open - 5 * atr, entry_open)
    t = _run(base + [fill, wide]).trades[0]
    assert t.reason == "STOP"


def test_only_one_position_open_at_a_time():
    base, atr, _, entry_open = _setup()
    quiet = [_bar(entry_open, entry_open + .3, entry_open - .3, entry_open)
             for _ in range(5)]
    r = _run(base + quiet)
    assert len(r.trades) == 0   # still open, never closed, never stacked


# ---- sizing & costs -------------------------------------------------------

def test_position_risk_is_one_percent_of_equity():
    base, atr, _, entry_open = _setup()
    capital = 10_000.0   # small enough that the notional cap does not bind
    fill = _bar(entry_open, entry_open + .5, entry_open - .5, entry_open)
    hit = _bar(entry_open, entry_open + 100, entry_open - .1, entry_open)
    t = _run(base + [fill, hit], capital=capital).trades[0]
    assert t.qty * (1.5 * atr) == pytest.approx(capital * 0.01)


def test_notional_cap_limits_quantity():
    base, atr, _, entry_open = _setup()
    fill = _bar(entry_open, entry_open + .5, entry_open - .5, entry_open)
    hit = _bar(entry_open, entry_open + 100, entry_open - .1, entry_open)
    t = _run(base + [fill, hit], capital=10_000_000.0).trades[0]
    assert t.qty * t.entry <= 10_000.0 + 1e-6


def test_costs_reduce_pnl_and_are_recorded():
    base, atr, _, entry_open = _setup()
    fill = _bar(entry_open, entry_open + .5, entry_open - .5, entry_open)
    hit = _bar(entry_open, entry_open + 3.5 * atr, entry_open - .1, entry_open + 3 * atr)
    free = _run(base + [fill, hit], costs=False).trades[0]
    paid = _run(base + [fill, hit], costs=True).trades[0]
    assert paid.fees > 0
    assert paid.pnl < free.pnl


def test_edge_gate_blocks_low_atr_crypto_and_counts_it():
    # Shift every price up by 100k: absolute moves (and so the signal) are
    # unchanged but ATR falls to ~0.001% of price, far below the cost floor.
    base, atr, _, entry_open = _setup()
    shift = 100_000.0
    shifted = [{k: (v + shift if k != "volume" else v) for k, v in c.items()}
               for c in base]
    eo = entry_open + shift
    fill = _bar(eo, eo + .5, eo - .5, eo)
    hit = _bar(eo, eo + 100, eo - .1, eo)
    candles = shifted + [fill, hit]

    ungated = _run(candles, gate=False)
    assert len(ungated.trades) == 1          # the setup does produce a trade...

    gated = _run(candles, gate=True)
    assert gated.trades == []                # ...and the gate is what stops it
    assert gated.blocked >= 1


# ---- summary statistics ---------------------------------------------------

def _trade(pnl, fees=0.0):
    return bt.Trade(asset="BTC", direction="LONG", entry_idx=0, entry=100, stop=99,
                    target=102, qty=1, pnl=pnl, fees=fees)


def test_summary_empty():
    s = bt.Result().summary(CAPITAL)
    assert s["trades"] == 0 and s["net_pnl"] == 0.0


def test_result_blocked_defaults_to_zero():
    assert bt.Result().blocked == 0


def test_summary_basic_stats():
    r = bt.Result(trades=[_trade(300), _trade(-100), _trade(-100), _trade(200, 5.0)],
                  equity=[100_300, 100_200, 100_100, 100_300])
    s = r.summary(CAPITAL)
    assert s["trades"] == 4
    assert s["net_pnl"] == 300
    assert s["win_rate"] == 50.0
    assert s["profit_factor"] == pytest.approx(500 / 200)
    assert s["avg_win"] == 250 and s["avg_loss"] == 100
    assert s["expectancy"] == 75
    assert s["fees"] == 5.0
    assert s["return_pct"] == pytest.approx(0.3)


def test_summary_max_drawdown_from_peak():
    r = bt.Result(trades=[_trade(1)] * 3, equity=[110_000, 99_000, 105_000])
    assert r.summary(100_000)["max_dd_pct"] == pytest.approx(10.0)


def test_summary_profit_factor_infinite_with_no_losses():
    r = bt.Result(trades=[_trade(10)], equity=[100_010])
    assert r.summary(CAPITAL)["profit_factor"] == float("inf")


# ---- loading data ---------------------------------------------------------

def test_load_candles_reads_csv(tmp_path, monkeypatch):
    monkeypatch.setattr(bt, "DATA_DIR", str(tmp_path))
    with open(tmp_path / "BTC.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["open", "high", "low", "close", "volume"])
        w.writeheader()
        w.writerow({"open": 1, "high": 2, "low": 0.5, "close": 1.5, "volume": 10})
    out = bt.load_candles("BTC")
    assert out == [{"open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "volume": 10.0}]


def test_load_candles_missing_file_is_empty(tmp_path, monkeypatch):
    monkeypatch.setattr(bt, "DATA_DIR", str(tmp_path))
    assert bt.load_candles("NOPE") == []


def test_load_candles_reads_optional_time_column(tmp_path, monkeypatch):
    monkeypatch.setattr(bt, "DATA_DIR", str(tmp_path))
    with open(tmp_path / "ETH.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["time", "open", "high", "low", "close", "volume"])
        w.writeheader()
        w.writerow({"time": 1700000000, "open": 1, "high": 2, "low": 0.5,
                    "close": 1.5, "volume": 10})
    assert bt.load_candles("ETH")[0]["time"] == 1700000000


def test_trade_exit_ts_uses_bar_time_when_available():
    base, atr, _, entry_open = _setup()
    for i, c in enumerate(base):
        c["time"] = 1_000_000 + 180 * i
    fill = _bar(entry_open, entry_open + .5, entry_open - .5, entry_open)
    hit = _bar(entry_open, entry_open + 100, entry_open - .1, entry_open)
    fill["time"] = 1_000_000 + 180 * len(base)
    hit["time"] = fill["time"] + 180
    t = _run(base + [fill, hit]).trades[0]
    assert t.exit_ts == hit["time"]


def test_trade_exit_ts_falls_back_to_bar_index_without_time():
    base, atr, _, entry_open = _setup()
    fill = _bar(entry_open, entry_open + .5, entry_open - .5, entry_open)
    hit = _bar(entry_open, entry_open + 100, entry_open - .1, entry_open)
    t = _run(base + [fill, hit]).trades[0]
    assert t.exit_ts == t.exit_idx


# ---- combining assets -----------------------------------------------------

def _timed(asset, ts, pnl):
    t = _trade(pnl)
    t.asset, t.exit_ts = asset, ts
    return t


def test_combine_trades_orders_by_exit_time_across_assets():
    a = [_timed("BTC", 1, 5000), _timed("BTC", 4, 5000)]
    b = [_timed("ETH", 2, -6000), _timed("ETH", 3, 6000)]

    combined = bt.combine_trades(a + b, 100_000.0)   # asset order in, time order out

    assert [t.exit_ts for t in combined.trades] == [1, 2, 3, 4]
    assert combined.equity == [105_000, 99_000, 105_000, 110_000]
    # Peak 105k -> trough 99k is a 5.71% drawdown. Summing in asset order
    # (BTC then ETH) would have reported 5.45% from a curve that never existed.
    assert combined.summary(100_000.0)["max_dd_pct"] == pytest.approx(6000 / 105_000 * 100)


def test_combine_trades_ends_at_starting_capital_plus_total_pnl():
    trades = [_timed("BTC", 3, 250), _timed("ETH", 1, -100), _timed("SOL", 2, 40)]
    combined = bt.combine_trades(trades, 1_000.0)
    assert combined.equity[-1] == pytest.approx(1_190.0)


def test_combine_trades_empty():
    c = bt.combine_trades([], 1_000.0)
    assert c.trades == [] and c.equity == []


def test_run_all_uses_chronological_combination(tmp_path, monkeypatch):
    """End to end: two assets whose trades interleave in time."""
    base, atr, _, entry_open = _setup()
    fill = _bar(entry_open, entry_open + .5, entry_open - .5, entry_open)
    win = _bar(entry_open, entry_open + 100, entry_open - .1, entry_open)
    loss = _bar(entry_open, entry_open + .1, entry_open - 5 * atr, entry_open)

    def series(exit_bar, t0):
        cs = [dict(c) for c in base] + [dict(fill), dict(exit_bar)]
        for i, c in enumerate(cs):
            c["time"] = t0 + 180 * i
        return cs

    data = {"BTC": series(win, 5_000), "ETH": series(loss, 1_000)}
    monkeypatch.setattr(bt, "load_candles", lambda a: data.get(a, []))

    out = bt.run_all(1.5, 3.0, apply_costs=False, apply_edge_gate=False,
                     capital=CAPITAL, verbose=False)
    assert out["combined"]["trades"] == 2
    assert set(out["per_asset"]) == {"BTC", "ETH"}

    # ETH's loss happens FIRST (t=1000..) and BTC's win later (t=5000..), so the
    # true drawdown is measured from the starting capital. Asset-order
    # summation (BTC first) would measure it from a higher, never-reached peak.
    eth_loss = -out["per_asset"]["ETH"]["net_pnl"]
    btc_win = out["per_asset"]["BTC"]["net_pnl"]
    assert eth_loss > 0 and btc_win > 0
    assert out["combined"]["max_dd_pct"] == pytest.approx(eth_loss / CAPITAL * 100)
    assert out["combined"]["max_dd_pct"] != pytest.approx(
        eth_loss / (CAPITAL + btc_win) * 100)
