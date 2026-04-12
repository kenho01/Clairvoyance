from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import time

import requests

from ingestion.investment.fx import get_rate_to_sgd
from ingestion.investment.models import Position

_BASE_URL = "https://api.gemini.com"


def _auth_headers(api_key: str, api_secret: str, endpoint: str, payload: dict) -> dict:
    payload["nonce"] = str(int(time.time() * 1000))
    payload["request"] = endpoint

    encoded = base64.b64encode(json.dumps(payload).encode()).decode()
    signature = hmac.new(  # type: ignore[attr-defined]
        api_secret.encode(),
        encoded.encode(),
        hashlib.sha384,
    ).hexdigest()

    return {
        "Content-Type": "text/plain",
        "X-GEMINI-APIKEY": api_key,
        "X-GEMINI-PAYLOAD": encoded,
        "X-GEMINI-SIGNATURE": signature,
        "Cache-Control": "no-cache",
    }


def _get_spot_price(symbol: str) -> float:
    """Fetch current mid price for a trading pair (e.g. 'BTCUSD')."""
    try:
        resp = requests.get(f"{_BASE_URL}/v1/pubticker/{symbol}USD", timeout=10)
        resp.raise_for_status()
        return float(resp.json().get("last", 0))
    except Exception:
        return 0.0


def fetch_positions(
    api_key: str | None = None,
    api_secret: str | None = None,
) -> list[Position]:
    api_key    = api_key    or os.environ["GEMINI_API_KEY"]
    api_secret = api_secret or os.environ["GEMINI_API_SECRET"]

    endpoint = "/v1/balances"
    headers  = _auth_headers(api_key, api_secret, endpoint, {"account": "primary"})

    print("Gemini: fetching balances...")
    resp = requests.post(f"{_BASE_URL}{endpoint}", headers=headers, timeout=15)
    resp.raise_for_status()
    balances = resp.json()

    positions = []
    for bal in balances:
        amount = float(bal.get("amount", 0))
        if amount <= 0:
            continue

        currency = bal.get("currency", "")

        _FIAT = {"USD", "SGD", "EUR", "GBP"}
        if currency in _FIAT:
            continue  # skip cash balances

        price = _get_spot_price(currency)
        market_value = amount * price

        fx_rate = get_rate_to_sgd("USD")
        positions.append(
            Position(
                source="gemini",
                symbol=currency,
                asset_class="crypto",
                quantity=amount,
                price=price,
                market_value=market_value,
                currency="USD",
                fx_rate_to_sgd=fx_rate,
                market_value_sgd=round(market_value * fx_rate, 2),
            )
        )

    print(f"Gemini: {len(positions)} balances fetched")
    return positions


if __name__ == "__main__":
    from dotenv import load_dotenv
    import pandas as pd
    load_dotenv()
    positions = fetch_positions()
    df = pd.DataFrame([p.to_dict() for p in positions])
    print(df.to_string(index=False))
