from __future__ import annotations

import requests

_BASE_URL = "https://open.er-api.com/v6/latest/SGD"
_cache: dict[str, float] = {}


def get_rate_to_sgd(currency: str) -> float:
    """Return how many SGD 1 unit of `currency` is worth. Returns 1.0 for SGD."""
    if currency == "SGD":
        return 1.0

    if not _cache:
        try:
            resp = requests.get(_BASE_URL, timeout=10)
            resp.raise_for_status()
            # rates are SGD per 1 unit of each currency — we need inverse
            rates = resp.json().get("rates", {})
            # rates["USD"] = how many USD per 1 SGD, so 1 USD = 1/rates["USD"] SGD
            for ccy, rate in rates.items():
                _cache[ccy] = round(1 / rate, 6) if rate else 0.0
            _cache["SGD"] = 1.0
        except Exception as e:
            print(f"WARNING: FX rate fetch failed — {e}")
            return 1.0

    return _cache.get(currency, 1.0)
