from __future__ import annotations

import os

import requests

_BASE_URL = "https://open.er-api.com/v6/latest/SGD"
_cache: dict[str, float] = {}


def _fallback_rates_from_bigquery(project_id: str) -> dict[str, float]:
    from google.cloud import bigquery
    client = bigquery.Client(project=project_id)
    query = f"""
        SELECT currency, fx_rate_to_sgd
        FROM `{project_id}.ods.ods_investment_positions_df`
        WHERE date = (SELECT MAX(date) FROM `{project_id}.ods.ods_investment_positions_df`)
        QUALIFY ROW_NUMBER() OVER (PARTITION BY currency ORDER BY etl_time DESC) = 1
    """
    rows = list(client.query(query).result())
    return {row.currency: row.fx_rate_to_sgd for row in rows}


def get_rate_to_sgd(currency: str) -> float:
    """Return how many SGD 1 unit of `currency` is worth. Returns 1.0 for SGD."""
    if currency == "SGD":
        return 1.0

    if not _cache:
        project_id = os.environ.get("GCP_PROJECT_ID", "")
        try:
            resp = requests.get(_BASE_URL, timeout=10)
            resp.raise_for_status()
            rates = resp.json().get("rates", {})
            for ccy, rate in rates.items():
                _cache[ccy] = round(1 / rate, 6) if rate else 0.0
            _cache["SGD"] = 1.0
        except Exception as e:
            print(f"WARNING: FX rate fetch failed ({e}) — falling back to latest BigQuery rates")
            if project_id:
                try:
                    bq_rates = _fallback_rates_from_bigquery(project_id)
                    _cache.update(bq_rates)
                    _cache["SGD"] = 1.0
                    print(f"FX: loaded {len(bq_rates)} rates from BigQuery fallback")
                except Exception as bq_err:
                    print(f"WARNING: FX BigQuery fallback also failed ({bq_err}) — non-SGD rates will be wrong")

    return _cache.get(currency, 1.0)
