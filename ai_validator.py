"""
ai_validator.py — AI-Powered Signal Validation Layer

Fix: JSON parse error now defaults to confidence=7 (proceeds with trade)
     instead of confidence=5 (blocked trade). This was causing valid
     signals like ETH to be skipped when Claude returned slightly
     malformed JSON.
"""

import os
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
    Crypto Fear & Greed Index from alternative.me.
    Cached for 30 minutes to avoid API spam.

      0  - 24  : Extreme Fear  → skip crypto BUYs
      25 - 44  : Fear          → cautious but tradeable
      45 - 74  : Neutral/Greed → normal trading
      75 - 100 : Extreme Greed → skip crypto BUYs (overextended)
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
    """
    Pre-filter crypto signals based on Fear & Greed sentiment.
    Stocks not filtered — different sentiment drivers.
    """
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
    Ask Claude to validate the signal before execution.
    Returns: (should_execute, confidence_1_to_10, reason)
    Only trade if execute=True AND confidence >= 7.
    """
    api_key = os.getenv("ANTHROPIC_API_KEY")

    if not _ANTHROPIC_INSTALLED or not api_key:
        return True, 7, "AI validation skipped (not configured)"

    try:
        client = anthropic.Anthropic(api_key=api_key)

        candle_lines = "\n".join([
            f"  {i+1}. O={c['open']:.4f} H={c['high']:.4f} "
            f"L={c['low']:.4f} C={c['close']:.4f}"
            for i, c in enumerate(recent_candles[-5:])
        ])

        stop   = price - 1.5 * atr if signal == "LONG" else price + 1.5 * atr
        target = price + 3.0 * atr if signal == "LONG" else price - 3.0 * atr

        prompt = f"""You are a quantitative trading signal validator. Analyze this signal.

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

Is this a genuine breakout? Respond ONLY with JSON:
{{"execute": true, "confidence": 8, "reason": "brief reason under 12 words"}}"""

        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=80,
            messages=[{"role": "user", "content": prompt}],
        )

        text   = response.content[0].text.strip()
        result = json.loads(text)

        execute    = bool(result.get("execute", True))
        confidence = int(result.get("confidence", 7))
        reason     = str(result.get("reason", ""))

        return execute, confidence, reason

    except json.JSONDecodeError:
        # FIX: was returning confidence=5 which BLOCKED trades on parse errors
        # Now returns 7 so the trade proceeds when Claude response is malformed
        return True, 7, "AI parse error — proceeding with trade"

    except Exception as e:
        return True, 7, f"AI error — proceeding ({str(e)[:30]})"