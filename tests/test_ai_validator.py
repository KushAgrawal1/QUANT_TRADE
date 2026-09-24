import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

import ai_validator as ai
import strategy as st


# ---- helpers --------------------------------------------------------------

CANDLES = [{"open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5}] * 5


def _fake_anthropic(reply_text=None, raises=None, captured=None):
    """Build a stand-in for the `anthropic` module."""
    class Messages:
        def create(self, **kwargs):
            if captured is not None:
                captured.update(kwargs)
            if raises:
                raise raises
            return SimpleNamespace(content=[SimpleNamespace(text=reply_text)])

    class Client:
        def __init__(self, **_):
            self.messages = Messages()

    return SimpleNamespace(Anthropic=Client)


def _call(signal="LONG", price=100.0, atr=2.0):
    return ai.validate_with_claude(
        asset="BTC", signal=signal, price=price, adx=30.0, vol_ratio=2.0,
        atr=atr, recent_candles=CANDLES, fear_greed_val=50,
    )


@pytest.fixture
def configured(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
    monkeypatch.setattr(ai, "_ANTHROPIC_INSTALLED", True)


# ---- _extract_json --------------------------------------------------------

def test_extract_json_plain():
    assert ai._extract_json('{"execute": true, "confidence": 8}')["confidence"] == 8


def test_extract_json_strips_markdown_fences():
    text = '```json\n{"execute": false, "confidence": 3, "reason": "weak"}\n```'
    assert ai._extract_json(text)["execute"] is False


def test_extract_json_ignores_preamble():
    text = 'Sure! Here you go: {"execute": true, "confidence": 9, "reason": "ok"} Thanks.'
    assert ai._extract_json(text)["confidence"] == 9


def test_extract_json_raises_on_garbage():
    with pytest.raises(json.JSONDecodeError):
        ai._extract_json("no json here")


# ---- validate_with_claude -------------------------------------------------

def test_skipped_without_api_key(monkeypatch):
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    execute, confidence, reason = _call()
    assert execute is True and confidence == 7
    assert "skipped" in reason


def test_parses_a_valid_response(monkeypatch, configured):
    reply = json.dumps({"execute": True, "confidence": 9, "reason": "clean breakout"})
    monkeypatch.setattr(ai, "anthropic", _fake_anthropic(reply), raising=False)
    assert _call() == (True, 9, "clean breakout")


def test_respects_a_veto(monkeypatch, configured):
    reply = json.dumps({"execute": False, "confidence": 2, "reason": "fakeout"})
    monkeypatch.setattr(ai, "anthropic", _fake_anthropic(reply), raising=False)
    execute, confidence, _ = _call()
    assert execute is False and confidence == 2


def test_fails_closed_on_api_error(monkeypatch, configured):
    """An API outage must never be read as 'yes, take the trade'."""
    monkeypatch.setattr(
        ai, "anthropic", _fake_anthropic(raises=TimeoutError("boom")), raising=False
    )
    execute, confidence, reason = _call()
    assert execute is False
    assert confidence == 0
    assert "skipping" in reason


def test_fails_closed_on_unparseable_reply(monkeypatch, configured):
    monkeypatch.setattr(ai, "anthropic", _fake_anthropic("I think yes!"), raising=False)
    execute, confidence, _ = _call()
    assert execute is False and confidence == 0


# ---- fear & greed filter --------------------------------------------------

def _fg(monkeypatch, value):
    monkeypatch.setattr(ai, "get_fear_greed", lambda: (value, "label"))


@pytest.mark.parametrize("direction", ["LONG", "BUY"])
@pytest.mark.parametrize("value", [0, 24, 76, 100])
def test_fg_blocks_longs_at_extremes(monkeypatch, direction, value):
    """Regression: trader.py passes 'LONG' but the filter only checked 'BUY',
    so the extreme fear/greed filter silently never blocked a long."""
    _fg(monkeypatch, value)
    ok, reason = ai.fear_greed_filter(direction, is_crypto=True)
    assert not ok, reason


@pytest.mark.parametrize("direction", ["LONG", "BUY"])
@pytest.mark.parametrize("value", [25, 50, 75])
def test_fg_allows_longs_in_normal_range(monkeypatch, direction, value):
    _fg(monkeypatch, value)
    assert ai.fear_greed_filter(direction, is_crypto=True)[0]


def test_fg_blocks_short_in_extreme_fear_only(monkeypatch):
    _fg(monkeypatch, 10)
    assert not ai.fear_greed_filter("SHORT", is_crypto=True)[0]
    _fg(monkeypatch, 90)
    assert ai.fear_greed_filter("SHORT", is_crypto=True)[0]


def test_fg_does_not_filter_stocks(monkeypatch):
    _fg(monkeypatch, 0)
    assert ai.fear_greed_filter("LONG", is_crypto=False)[0]


# ---- fear & greed cache ---------------------------------------------------

class _FakeResp:
    def __init__(self, value, label):
        self._payload = {"data": [{"value": str(value), "value_classification": label}]}

    def json(self):
        return self._payload


@pytest.fixture
def fg_env(monkeypatch):
    calls = {"n": 0}

    def fake_get(url, timeout=None):
        calls["n"] += 1
        return _FakeResp(12, "Extreme Fear")

    monkeypatch.setattr(ai.requests, "get", fake_get)
    return calls


def _seed_cache(monkeypatch, age):
    monkeypatch.setattr(ai, "_fg_cache", {
        "value": 50, "label": "Neutral",
        "updated": datetime.now(timezone.utc) - age,
    })


def test_fg_cache_is_reused_while_fresh(monkeypatch, fg_env):
    _seed_cache(monkeypatch, timedelta(minutes=5))
    assert ai.get_fear_greed() == (50, "Neutral")
    assert fg_env["n"] == 0


def test_fg_cache_refreshes_after_30_minutes(monkeypatch, fg_env):
    _seed_cache(monkeypatch, timedelta(minutes=31))
    assert ai.get_fear_greed() == (12, "Extreme Fear")
    assert fg_env["n"] == 1


def test_fg_cache_refreshes_when_over_a_day_old(monkeypatch, fg_env):
    """Regression: timedelta.seconds wraps every 24h, so a cache aged
    1 day + 10 seconds looked 10 seconds old and was never refreshed."""
    _seed_cache(monkeypatch, timedelta(days=1, seconds=10))
    assert ai.get_fear_greed() == (12, "Extreme Fear")
    assert fg_env["n"] == 1


def test_fg_keeps_stale_value_if_refresh_fails(monkeypatch):
    def boom(url, timeout=None):
        raise ConnectionError("offline")

    monkeypatch.setattr(ai.requests, "get", boom)
    _seed_cache(monkeypatch, timedelta(hours=2))
    assert ai.get_fear_greed() == (50, "Neutral")


# ---- prompt contents ------------------------------------------------------

def _prompt_for(monkeypatch, signal, price=100.0, atr=2.0):
    captured = {}
    reply = json.dumps({"execute": True, "confidence": 8, "reason": "ok"})
    monkeypatch.setattr(
        ai, "anthropic", _fake_anthropic(reply, captured=captured), raising=False
    )
    _call(signal=signal, price=price, atr=atr)
    return captured["messages"][0]["content"]


def test_prompt_long_stop_below_and_target_above(monkeypatch, configured):
    prompt = _prompt_for(monkeypatch, "LONG")
    assert "Stop:   $97.0000" in prompt
    assert "Target: $106.0000" in prompt


def test_prompt_short_stop_above_and_target_below(monkeypatch, configured):
    prompt = _prompt_for(monkeypatch, "SHORT")
    assert "Stop:   $103.0000" in prompt
    assert "Target: $94.0000" in prompt


def test_prompt_follows_strategy_multiples(monkeypatch, configured):
    """The prompt used to hardcode 1.5x / 3.0x / '2:1', so retuning the
    strategy left the AI reviewing a stop and target that were never used."""
    monkeypatch.setattr(st, "STOP_ATR_MULT", 2.0)
    monkeypatch.setattr(st, "TARGET_ATR_MULT", 5.0)
    prompt = _prompt_for(monkeypatch, "LONG")
    assert "Stop:   $96.0000" in prompt
    assert "Target: $110.0000" in prompt
    assert "2.5:1" in prompt
