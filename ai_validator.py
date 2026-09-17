"""
ai_validator.py — AI-Powered Signal Validation Layer

Fix: max_tokens increased to 200 — stops Claude truncating JSON mid-string.
     "Unterminated string starting at" error is now eliminated.
"""

import os
import re
import json
import requests
from datetime import datetime, timezone

try:
    import anthropic
    _ANTHROPIC_INSTALLED = True
except ImportError:
    _ANTHROPIC_INSTALLED = False


# ── Fear & Greed Cache ────────────────────────────────────────────────────────

_fg_cache = {"value": 50, "label": "Neutral", "updated": None}


def get_fear_greed() -> tuple[int, str]:
    """
    Crypto Fear & Greed Index from alternative.me. Cached 30 minutes.
      0  - 24  : Extreme Fear  → skip crypto BUYs
      25 - 44  : Fear          → cautious but tradeable
      45 - 74  : Neutral/Greed → normal trading
      75 - 100 : Extreme Greed → skip crypto BUYs
    """
    global _fg_cache
    now = datetime.now(timezone.utc)
    if (_fg_cache["updated"] is None or
            (now - _fg_cache["updated"]).seconds > 1800):
        try:
            resp = requests.get(
                "https://api.alternative.me/fng/?limit=1", timeout=5
            )
            data = resp.json()
            _fg_cache = {
                "value":   int(data["data"][0]["value"]),
                "label":   data["data"][0]["value_classification"],
                "updated": now,
            }
        except Exception:
            pass
    return _fg_cache["value"], _fg_cache["label"]


def fear_greed_filter(signal: str, is_crypto: bool) -> tuple[bool, str]:
    if not is_crypto:
        return True, "stocks not F&G filtered"
    fg_val, fg_label = get_fear_greed()
    if signal == "BUY" and fg_val <= 24:
        return False, f"Extreme Fear ({fg_val}) — skip long"
    if signal == "BUY" and fg_val >= 76:
        return False, f"Extreme Greed ({fg_val}) — skip long"
    if signal == "SHORT" and fg_val <= 24:
        return False, f"Extreme Fear ({fg_val}) — skip short"
    return True, f"F&G={fg_val} ({fg_label})"


# ── JSON Extractor ────────────────────────────────────────────────────────────

def _extract_json(text: str) -> dict:
    """
    Robustly extract JSON from Claude response.
    Handles markdown fences and preamble text.
    """
    text = re.sub(r'```(?:json)?', '', text).strip()
    text = text.replace('```', '').strip()
    match = re.search(r'\{[^{}]*\}', text, re.DOTALL)
    if match:
        return json.loads(match.group())
    return json.loads(text)


# ── Claude Signal Validator ───────────────────────────────────────────────────

def validate_with_claude(
    asset: str,
    signal: str,
    price: float,
    adx: float,
    vol_ratio: float,
    atr: float,
    recent_candles: list,
    fear_greed_val: int,
) -> tuple[bool, int, str]:
    """
    Ask Claude to validate the signal. Returns (execute, confidence, reason).
    Only trade if execute=True AND confidence >= 7.
    """
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not _ANTHROPIC_INSTALLED or not api_key:
        return True, 7, "AI validation skipped (not configured)"

    try:
        client = anthropic.Anthropic(api_key=api_key, timeout=8.0, max_retries=1)

        candle_lines = "\n".join([
            f"  {i+1}. O={c['open']:.4f} H={c['high']:.4f} "
            f"L={c['low']:.4f} C={c['close']:.4f}"
            for i, c in enumerate(recent_candles[-5:])
        ])

        stop   = price - 1.5 * atr if signal == "LONG" else price + 1.5 * atr
        target = price + 3.0 * atr if signal == "LONG" else price - 3.0 * atr

        prompt = f"""You are a quantitative trading signal validator.

Asset:      {asset}
Direction:  {signal}
Price:      ${price:.4f}
ADX:        {adx:.1f}  ({'TRENDING' if adx > 25 else 'WEAK'})
Volume:     {vol_ratio:.1f}x average  ({'CONFIRMED' if vol_ratio >= 1.5 else 'WEAK'})
ATR:        ${atr:.4f}
Fear/Greed: {fear_greed_val}/100

Last 5 candles:
{candle_lines}

Stop:   ${stop:.4f}
Target: ${target:.4f}
R:R = 2:1

Is this a genuine breakout?

IMPORTANT: Respond with ONLY a raw JSON object. No markdown. No backticks. No text before or after.
Format: {{"execute": true, "confidence": 8, "reason": "brief reason"}}"""

        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=200,   # FIX: was 100, caused truncated JSON errors
            messages=[{"role": "user", "content": prompt}],
        )

        text   = response.content[0].text.strip()
        result = _extract_json(text)

        execute    = bool(result.get("execute", True))
        confidence = int(result.get("confidence", 7))
        reason     = str(result.get("reason", ""))

        return execute, confidence, reason

    except Exception as e:
        # FAIL CLOSED. This previously returned (True, 7, ...) which passes
        # the >= 7 confidence threshold, so any API timeout, rate limit or
        # network blip silently became "yes, take this trade". An unvalidated
        # signal during an outage is exactly when you least want to be filled.
        return False, 0, f"AI error - skipping trade ({str(e)[:40]})"