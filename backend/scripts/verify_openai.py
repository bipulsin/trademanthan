#!/usr/bin/env python3
"""
Verify OPENAI_API_KEY is loaded and a minimal chat completion works (gpt-4o-mini).

Does not print the API key. Exit 0 on success, non-zero on failure.

  PYTHONPATH=. python backend/scripts/verify_openai.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


def main() -> int:
    import backend.env_bootstrap  # noqa: F401

    from backend.config import settings

    key = (settings.OPENAI_API_KEY or "").strip()
    if not key:
        print("OPENAI_API_KEY: NOT SET (empty or missing in environment / .env)")
        return 2

    print("OPENAI_API_KEY: SET (length=%s chars)" % len(key))

    try:
        from openai import OpenAI
    except ImportError:
        print("FAIL: openai package not installed (pip install 'openai>=1.54.0')")
        return 3

    client = OpenAI(api_key=key, timeout=45.0, max_retries=0)
    model = "gpt-4o-mini"
    try:
        resp = client.chat.completions.create(
            model=model,
            messages=[
                {
                    "role": "user",
                    "content": 'Reply with JSON only: {"ok": true, "echo": "pong"}',
                }
            ],
            response_format={"type": "json_object"},
            max_tokens=64,
            temperature=0,
        )
    except Exception as e:
        err = str(e)[:800]
        print("FAIL: OpenAI API request error:", type(e).__name__, err)
        if "insufficient_quota" in err or "429" in err:
            print(
                "HINT: Key is accepted but account has no quota / billing issue — "
                "see https://platform.openai.com/account/billing"
            )
        return 4

    msg = (resp.choices[0].message.content or "").strip()
    print("Model:", model)
    print("Raw response (truncated):", msg[:300] + ("…" if len(msg) > 300 else ""))

    try:
        data = json.loads(msg)
    except json.JSONDecodeError:
        print("WARN: response was not valid JSON; treating as partial success if HTTP succeeded")
        print("OK: HTTP completion succeeded (body not JSON)")
        return 0

    if data.get("ok") is True:
        print("OK: Parsed JSON with ok=true")
        return 0

    print("WARN: JSON parsed but ok!=true:", data)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
