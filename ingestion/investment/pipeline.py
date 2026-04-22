from __future__ import annotations

import argparse
import io
import os
from datetime import datetime, timezone

import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery, storage

from ingestion.investment import gemini, ibkr, tiger
from ingestion.investment.models import Position

_SOURCE_MAP = {
    "ibkr":   ibkr.fetch_positions,
    "tiger":  tiger.fetch_positions,
    "gemini": gemini.fetch_positions,
}

_BQ_TABLE = "ods.ods_investment_positions_df"


def _fetch_all(sources: list[str]) -> list[Position]:
    positions = []
    for source in sources:
        try:
            positions.extend(_SOURCE_MAP[source]())
        except Exception as e:
            print(f"WARNING: {source} fetch failed — {e}")
    return positions


def _upload_parquet_to_gcs(df: pd.DataFrame, bucket_name: str, project_id: str) -> None:
    client = storage.Client(project=project_id)
    bucket = client.bucket(bucket_name)
    date   = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    path   = f"raw/investments/{date}/positions.parquet"
    blob   = bucket.blob(path)

    buf = io.BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow")
    buf.seek(0)
    blob.upload_from_file(buf, content_type="application/octet-stream")
    print(f"Uploaded to gs://{bucket_name}/{path}")


def _load_to_bigquery(df: pd.DataFrame, project_id: str) -> None:
    client    = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.{_BQ_TABLE}"

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=[
            bigquery.SchemaField("etl_time",         "TIMESTAMP"),
            bigquery.SchemaField("date",             "DATE"),
            bigquery.SchemaField("source",           "STRING"),
            bigquery.SchemaField("symbol",           "STRING"),
            bigquery.SchemaField("asset_class",      "STRING"),
            bigquery.SchemaField("quantity",         "FLOAT64"),
            bigquery.SchemaField("price",            "FLOAT64"),
            bigquery.SchemaField("market_value",     "FLOAT64"),
            bigquery.SchemaField("currency",         "STRING"),
            bigquery.SchemaField("fx_rate_to_sgd",   "FLOAT64"),
            bigquery.SchemaField("market_value_sgd", "FLOAT64"),
        ],
    )

    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()
    print(f"Loaded {len(df)} rows into {table_ref}")


def run(sources: list[str], bucket_name: str, project_id: str) -> pd.DataFrame:
    positions = _fetch_all(sources)
    if not positions:
        print("No positions fetched — skipping")
        return pd.DataFrame()

    df = pd.DataFrame([p.to_dict() for p in positions])
    df["etl_time"] = pd.Timestamp.now(tz="Asia/Singapore")
    df["date"]     = pd.Timestamp.now(tz="Asia/Singapore").date()

    print(f"\nCombined portfolio: {len(df)} positions across {df['source'].nunique()} source(s)")
    print(df[["source", "symbol", "asset_class", "quantity", "price", "market_value", "currency"]].to_string(index=False))

    if bucket_name:
        try:
            _upload_parquet_to_gcs(df, bucket_name, project_id)
        except Exception as e:
            print(f"ERROR: GCS upload failed — {e}")
            raise
    else:
        print("GCS_BUCKET_INVESTMENTS not set — skipping GCS upload")

    if project_id:
        try:
            _load_to_bigquery(df, project_id)
        except Exception as e:
            print(f"ERROR: BigQuery load failed — {e}")
            raise
    else:
        print("GCP_PROJECT_ID not set — skipping BigQuery load")

    return df


def main() -> None:
    load_dotenv()
    parser = argparse.ArgumentParser(description="Investment portfolio pipeline")
    parser.add_argument(
        "--sources",
        nargs="+",
        choices=list(_SOURCE_MAP.keys()),
        default=list(_SOURCE_MAP.keys()),
        help="Which broker sources to fetch (default: all)",
    )
    args = parser.parse_args()

    try:
        run(
            sources=args.sources,
            bucket_name=os.environ.get("GCS_BUCKET_INVESTMENTS", ""),
            project_id=os.environ.get("GCP_PROJECT_ID", ""),
        )
    except Exception as e:
        import traceback
        print(f"FATAL: pipeline failed — {e}")
        traceback.print_exc()
        raise


if __name__ == "__main__":
    main()
