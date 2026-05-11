from __future__ import annotations

import os

from google.cloud import bigquery

from ingestion.investment.fx import get_rate_to_sgd
from ingestion.investment.models import Position


_ASSET_CLASS_MAP = {
    "STK": "stock",
    "ETF": "etf",
    "OPT": "option",
    "FUT": "future",
    "CASH": "cash",
    "CRYPTO": "crypto",
}


def _build_client():
    from tigeropen.common.consts import Language
    from tigeropen.tiger_open_config import TigerOpenClientConfig
    from tigeropen.trade.trade_client import TradeClient

    config = TigerOpenClientConfig(sandbox_debug=False)
    config.tiger_id = os.environ["TIGER_ID"]
    config.private_key = os.environ["TIGER_PRIVATE_KEY"]
    config.account = os.environ["TIGER_ACCOUNT"]
    config.language = Language.en_US

    return TradeClient(config)


def _fallback_from_bigquery(project_id: str) -> list[Position]:
    client = bigquery.Client(project=project_id)
    query = f"""
        SELECT symbol, asset_class, quantity, price, market_value,
               currency, fx_rate_to_sgd, market_value_sgd
        FROM `{project_id}.ods.ods_investment_positions_df`
        WHERE source = 'tiger'
          AND date = (
              SELECT MAX(date)
              FROM `{project_id}.ods.ods_investment_positions_df`
              WHERE source = 'tiger'
          )
        QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY etl_time DESC) = 1
    """
    rows = list(client.query(query).result())
    return [
        Position(
            source="tiger",
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


def fetch_positions(project_id: str | None = None) -> list[Position]:
    project_id = project_id or os.environ.get("GCP_PROJECT_ID", "")

    try:
        print("Tiger Brokers: fetching positions...")
        client = _build_client()
        raw_positions = client.get_positions(account=os.environ["TIGER_ACCOUNT"])

        positions = []
        for p in raw_positions:
            quantity     = float(p.position_qty or 0)
            price        = float(p.market_price or 0)
            market_value = float(p.market_value or quantity * price)

            currency = getattr(p.contract, "currency", "USD")
            fx_rate  = get_rate_to_sgd(currency)
            positions.append(
                Position(
                    source="tiger",
                    symbol=p.contract.symbol,
                    asset_class=_ASSET_CLASS_MAP.get(
                        getattr(p.contract, "sec_type", "STK"), "stock"
                    ),
                    quantity=quantity,
                    price=price,
                    market_value=market_value,
                    currency=currency,
                    fx_rate_to_sgd=fx_rate,
                    market_value_sgd=round(market_value * fx_rate, 2),
                )
            )

        print(f"Tiger Brokers: {len(positions)} positions fetched")
        return positions
    except Exception as e:
        print(f"Tiger: fetch failed ({e}) — falling back to latest BigQuery data")
        if not project_id:
            print("Tiger: GCP_PROJECT_ID not set, cannot fall back")
            return []
        try:
            positions = _fallback_from_bigquery(project_id)
            print(f"Tiger: using {len(positions)} positions from latest history (fallback)")
            return positions
        except Exception as bq_err:
            print(f"Tiger: BigQuery fallback also failed — {bq_err}")
            return []


if __name__ == "__main__":
    from dotenv import load_dotenv
    import pandas as pd
    load_dotenv()
    positions = fetch_positions()
    df = pd.DataFrame([p.to_dict() for p in positions])
    print(df.to_string(index=False))
