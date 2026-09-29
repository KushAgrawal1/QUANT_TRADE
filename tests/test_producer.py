
from producer import PAIR_TO_COIN, parse_kraken_message


def _trade_msg(pair="XBT/USD", trades=None):
    return [123, trades or [], "trade", pair]


def test_valid_trade_message_builds_one_event_per_trade():
    msg = _trade_msg(trades=[
        ["50000.1", "0.5", "1700000000.123", "b", "m", ""],
        ["50001.0", "0.2", "1700000000.456", "s", "l", ""],
    ])
    events = parse_kraken_message(msg)
    assert len(events) == 2
    assert events[0] == {
        "coin": "BTC", "price": 50000.1, "volume": 0.5,
        "timestamp": "1700000000.123", "side": "buy", "asset_class": "crypto",
    }
    assert events[1]["side"] == "sell"


def test_unknown_pair_falls_back_to_the_raw_pair_string():
    msg = _trade_msg(pair="SHIB/USD", trades=[["0.00001", "1000", "1700000000", "b", "m", ""]])
    assert parse_kraken_message(msg)[0]["coin"] == "SHIB/USD"


def test_every_configured_pair_maps_to_its_coin():
    for pair, coin in PAIR_TO_COIN.items():
        msg = _trade_msg(pair=pair, trades=[["1", "1", "1700000000", "b", "m", ""]])
        assert parse_kraken_message(msg)[0]["coin"] == coin


def test_non_trade_channel_returns_nothing():
    assert parse_kraken_message([123, {}, "ticker", "XBT/USD"]) == []


def test_wrong_shaped_list_returns_nothing():
    assert parse_kraken_message([123, "trade"]) == []
    assert parse_kraken_message([]) == []


def test_dict_message_returns_nothing():
    """subscriptionStatus/heartbeat messages are dicts, not lists - handled
    separately in on_message, not by the trade parser."""
    assert parse_kraken_message({"event": "heartbeat"}) == []


def test_one_malformed_trade_does_not_drop_the_rest_of_the_batch():
    """Regression: a single bad trade entry used to raise out of the whole
    on_message call, killing every other trade in the same WS frame and
    forcing a full reconnect over one bad record."""
    msg = _trade_msg(trades=[
        ["not-a-number", "0.5", "1700000000", "b", "m", ""],
        ["50000.0", "0.1", "1700000001", "b", "m", ""],
    ])
    events = parse_kraken_message(msg)
    assert len(events) == 1
    assert events[0]["price"] == 50000.0


def test_a_trade_missing_fields_is_skipped_not_raised():
    msg = _trade_msg(trades=[["50000.0"]])   # missing volume/timestamp/side
    assert parse_kraken_message(msg) == []
