"""
ai_validator.py — AI-Powered Signal Validation Layer

Research basis:
  "LLMs cannot predict prices, but excel at signal validation and
   sentiment analysis. Binary confirmation mechanism reduces false
   signals by 40-70%." — ScienceDirect, June 2026

  "LLM-guided RL agents outperformed unguided baselines in 4/6 stocks
   on Sharpe Ratio. All improvements came from LLM guidance alone,
   not from modifying the underlying architecture."
   — FLLM 2025 Research Paper

Tools used:
  1. Crypto Fear & Greed Index (alternative.me) — free, no auth
     Context: market-wide sentiment. Extreme fear = avoid longs.
     Extreme greed = avoid longs (overextended).

  2. Claude AI (claude-haiku) — signal validator
     Context: given technical setup, is this signal high quality?
     Responds YES/NO with confidence. Only trade on high confidence.
     Cost: ~$0.0003 per call. 100 calls overnight = ~$0.03 total.

How to enable Claude validation:
  1. Go to console.anthropic.com → create account → generate API key
  2. Add to .env: ANTHROPIC_API_KEY=sk-ant-...
  3. pip install anthropic

If no key is set, validation is skipped gracefully — system still runs.
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
    Updates every hour. We cache for 30 minutes to avoid API spam.

    Scale:
      0  - 24  : Extreme Fear  → institutions selling, avoid crypto BUYs
      25 - 44  : Fear          → cautious, still tradeable
      45 - 74  : Neutral/Greed → normal conditions
      75 - 100 : Extreme Greed → market overextended, avoid BUYs
    """
    global _fg_cache
    now = datetime.now(timezone.utc)

    # Refresh every 30 minutes
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
            pass  # keep using cached value on error

    return _fg_cache["value"], _fg_cache["label"]


def fear_greed_filter(signal: str, is_crypto: bool) -> tuple[bool, str]:
    """
    Apply Fear & Greed as a pre-filter for crypto signals.
    Stocks are not filtered — they have different sentiment drivers.

    Returns (allowed, reason)
    """
    if not is_crypto:
        return True, "stocks not F&G filtered"

    fg_val, fg_label = get_fear_greed()

    if signal == "BUY" and fg_val <= 24:
        return False, f"Extreme Fear ({fg_val}) — institutions selling, skip long"

    if signal == "BUY" and fg_val >= 76:
        return False, f"Extreme Greed ({fg_val}) — market overextended, skip long"

    if signal == "SHORT" and fg_val <= 24:
        return False, f"Extreme Fear ({fg_val}) — already crashing, skip short"

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

    Claude sees:
      - Asset + signal direction
      - Current ADX, volume, ATR values
      - Last 5 candles (OHLC)
      - Fear & Greed context
      - Proposed stop and target

    Claude decides: is this a genuine breakout or likely a false signal?

    Only trade if: execute=True AND confidence >= 7

    Uses claude-haiku (fastest + cheapest). Cost: ~$0.0003 per call.
    """
    api_key = os.getenv("ANTHROPIC_API_KEY")

    if not _ANTHROPIC_INSTALLED or not api_key:
        return True, 5, "AI validation skipped (not configured)"

    try:
        client = anthropic.Anthropic(api_key=api_key)

        candle_lines = "\n".join([
            f"  {i+1}. O={c['open']:.4f} H={c['high']:.4f} "
            f"L={c['low']:.4f} C={c['close']:.4f}"
            for i, c in enumerate(recent_candles[-5:])
        ])

        if signal == "LONG":
            stop   = price - 1.5 * atr
            target = price + 3.0 * atr
        else:
            stop   = price + 1.5 * atr
            target = price - 3.0 * atr

        prompt = f"""You are a quantitative trading signal validator. Analyze this signal.

Asset:      {asset}
Direction:  {signal}
Price:      ${price:.4f}
ADX:        {adx:.1f}  (>25=trending, currently {'TRENDING' if adx > 25 else 'WEAK'})
Volume:     {vol_ratio:.1f}x average  ({'CONFIRMED' if vol_ratio >= 1.5 else 'WEAK'})
ATR:        ${atr:.4f}
Fear/Greed: {fear_greed_val}/100

Last 5 candles:
{candle_lines}

Stop:   ${stop:.4f}  (risk: ${abs(price - stop):.4f})
Target: ${target:.4f}  (reward: ${abs(target - price):.4f})
R:R = 2:1

Is this a genuine breakout or a false signal? Does recent price action support it?

Respond ONLY with JSON — no other text:
{{"execute": true, "confidence": 8, "reason": "momentum confirmed by volume"}}"""

        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=80,
            messages=[{"role": "user", "content": prompt}],
        )

        result     = json.loads(response.content[0].text.strip())
        execute    = bool(result.get("execute", True))
        confidence = int(result.get("confidence", 5))
        reason     = str(result.get("reason", ""))

        return execute, confidence, reason

    except json.JSONDecodeError:
        return True, 5, "AI parse error — proceeding"
    except Exception as e:
        return True, 5, f"AI error — proceeding ({str(e)[:30]})"