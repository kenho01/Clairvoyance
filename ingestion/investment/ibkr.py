from __future__ import annotations

import io
import os
import time
import xml.etree.ElementTree as ET

import pandas as pd
import requests
from dotenv import load_dotenv
from google.cloud import bigquery

from ingestion.investment.fx import get_rate_to_sgd
from ingestion.investment.models import Position


_SEND_URL = "https://gdcdyn.interactivebrokers.com/Universal/servlet/FlexStatementService.SendRequest"
_GET_URL  = "https://gdcdyn.interactivebrokers.com/Universal/servlet/FlexStatementService.GetStatement"

_ASSET_CLASS_MAP = {
    "STK": "stock",
    "ETF": "etf",
    "OPT": "option",
    "FUT": "future",
    "CASH": "cash",
    "CRYPTO": "crypto",
}


def _request_report(token: str, query_id: str) -> str:
    """Submit the Flex Query and return the reference code."""
    resp = requests.get(
        _SEND_URL,
        params={"t": token, "q": query_id, "v": 3},
        timeout=60,
    )
    resp.raise_for_status()
    root = ET.fromstring(resp.text)
    status = root.findtext("Status")
    if status != "Success":
        raise RuntimeError(f"IBKR Flex Query failed: {root.findtext('ErrorMessage')}")
    return root.findtext("ReferenceCode")


def _download_report(token: str, reference_code: str, retries: int = 5) -> str:
    """Poll until the report is ready and return the raw CSV text."""
    for attempt in range(retries):
        resp = requests.get(
            _GET_URL,
            params={"t": token, "q": reference_code, "v": 3},
            timeout=60,
        )
        resp.raise_for_status()
        # CSV report is ready when it doesn't start with XML tags
        if not resp.text.strip().startswith("<"):
            return resp.text
        # Report not ready yet — wait and retry
        time.sleep(2 ** attempt)
    raise TimeoutError("IBKR report not ready after retries")


def _parse_positions(csv_text: str) -> list[Position]:
    """
    Parse IBKR CSV Flex Query output into Position objects.

    IBKR CSV files contain multiple sections with different headers.
    We extract only the rows that have position columns (Symbol, AssetClass etc).
    """
    positions = []
    position_columns = {"CurrencyPrimary", "AssetClass", "Symbol", "Quantity", "MarkPrice"}
    current_header = None

    for line in csv_text.splitlines():
        row = [col.strip().strip('"') for col in line.split(",")]

        # Detect a header row for the positions section
        if position_columns.issubset(set(row)):
            current_header = row
            continue

        # Parse a data row under the positions header
        if current_header and len(row) == len(current_header):
            record = dict(zip(current_header, row))
            asset_class_raw = record.get("AssetClass", "STK")

            try:
                quantity = float(record.get("Quantity", 0) or 0)
                price    = float(record.get("MarkPrice", 0) or 0)
            except ValueError:
                continue

            if quantity == 0:
                continue

            currency = record.get("CurrencyPrimary", "USD")
            market_value = round(quantity * price, 2)
            fx_rate = get_rate_to_sgd(currency)
            positions.append(
                Position(
                    source="ibkr",
                    symbol=record.get("Symbol", ""),
                    asset_class=_ASSET_CLASS_MAP.get(asset_class_raw, asset_class_raw.lower()),
                    quantity=quantity,
                    price=price,
                    market_value=market_value,
                    currency=currency,
                    fx_rate_to_sgd=fx_rate,
                    market_value_sgd=round(market_value * fx_rate, 2),
                )
            )

    return positions


def _fallback_from_bigquery(project_id: str) -> list[Position]:
    client = bigquery.Client(project=project_id)
    query = f"""
        SELECT symbol, asset_class, quantity, price, market_value,
               currency, fx_rate_to_sgd, market_value_sgd
        FROM `{project_id}.ods.ods_investment_positions_df`
        WHERE source = 'ibkr'
          AND date = (
              SELECT MAX(date)
              FROM `{project_id}.ods.ods_investment_positions_df`
              WHERE source = 'ibkr'
          )
        QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY etl_time DESC) = 1
    """
    rows = list(client.query(query).result())
    return [
        Position(
            source="ibkr",
            symbol=row.symbol,
            asset_class=row.asset_class,
            quantity=row.quantity,
            price=row.price,
            market_value=row.market_value,
            currency=row.currency,
            fx_rate_to_sgd=row.fx_rate_to_sgd,
            market_value_sgd=row.market_value_sgd,
        )
        for row in rows
    ]


def fetch_positions(
    token: str | None = None,
    query_id: str | None = None,
    project_id: str | None = None,
    _status: dict | None = None,
) -> list[Position]:
    token      = token      or os.environ["IBKR_FLEX_TOKEN"]
    query_id   = query_id   or os.environ["IBKR_FLEX_QUERY_ID"]
    project_id = project_id or os.environ.get("GCP_PROJECT_ID", "")

    try:
        print("IBKR: requesting Flex Query report...")
        ref_code = _request_report(token, query_id)

        print(f"IBKR: downloading report (ref={ref_code})...")
        csv_text = _download_report(token, ref_code)

        positions = _parse_positions(csv_text)
        print(f"IBKR: {len(positions)} positions fetched")
        if _status is not None:
            _status["status"] = "live"
            _status["row_count"] = len(positions)
        return positions
    except Exception as e:
        print(f"IBKR: fetch failed ({e}) — falling back to latest BigQuery data")
        if _status is not None:
            _status["status"] = "fallback"
        if not project_id:
            print("IBKR: GCP_PROJECT_ID not set, cannot fall back")
            if _status is not None:
                _status["status"] = "failed"
                _status["row_count"] = 0
            return []
        try:
            positions = _fallback_from_bigquery(project_id)
            print(f"IBKR: using {len(positions)} positions from latest history (fallback)")
            if _status is not None:
                _status["row_count"] = len(positions)
            return positions
        except Exception as bq_err:
            print(f"IBKR: BigQuery fallback also failed — {bq_err}")
            if _status is not None:
                _status["status"] = "failed"
                _status["row_count"] = 0
            return []


if __name__ == "__main__":
    load_dotenv()
    positions = fetch_positions()
    df = pd.DataFrame([p.to_dict() for p in positions])
    print(df.to_string(index=False))
