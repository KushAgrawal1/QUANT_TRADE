import fetch_history as fh


class _Resp:
    def __init__(self, payload):
        self._p = payload

    def json(self):
        return self._p


def _row(t, o, h, l, c, v):
    # Kraken OHLC row: [time, open, high, low, close, vwap, volume, count]
    return [t, str(o), str(h), str(l), str(c), "0", str(v), 1]


def _mock_kraken(monkeypatch, rows):
    payload = {"error": [], "result": {"XBTUSD": rows, "last": rows[-1][0]}}
    seen = {}

    def fake_get(url, params=None, timeout=None):
        seen.update(params or {})
        return _Resp(payload)

    monkeypatch.setattr(fh.requests, "get", fake_get)
    return seen


def test_native_interval_passes_bars_through_with_time(monkeypatch):
    rows = [_row(1000 + 900 * i, 10 + i, 11 + i, 9 + i, 10.5 + i, 5) for i in range(4)]
    seen = _mock_kraken(monkeypatch, rows)
    bars = fh.fetch("XBTUSD", 15)
    assert seen["interval"] == 15
    assert len(bars) == 3                    # in-progress final bar dropped
    assert bars[0]["time"] == 1000
    assert bars[0]["open"] == 10.0 and bars[0]["volume"] == 5.0


def test_three_minute_bars_are_aggregated_from_one_minute(monkeypatch):
    rows = [
        _row(0,   10, 12, 9,  11, 1),
        _row(60,  11, 15, 10, 14, 2),
        _row(120, 14, 14, 8,  9,  3),
        _row(180, 9,  10, 7,  8,  4),
        _row(240, 8,  20, 8,  19, 5),
        _row(300, 19, 19, 18, 18, 6),
        _row(360, 1,  1,  1,  1,  1),        # in-progress bar, dropped
    ]
    seen = _mock_kraken(monkeypatch, rows)
    bars = fh.fetch("XBTUSD", 3)
    assert seen["interval"] == 1
    assert len(bars) == 2
    first = bars[0]
    assert first == {"time": 0, "open": 10.0, "high": 15.0, "low": 8.0,
                     "close": 9.0, "volume": 6.0}
    assert bars[1]["time"] == 180
    assert bars[1]["high"] == 20.0 and bars[1]["low"] == 7.0
