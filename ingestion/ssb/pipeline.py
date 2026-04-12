from __future__ import annotations

import argparse
import io
import os
from datetime import date
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery, storage

_BQ_TABLE      = "ods.ods_ssb_holdings_df"
_GCS_CSV_PATH  = "config/ssb_holdings.csv"   # where the source-of-truth CSV lives in GCS


def _read_csv_from_gcs(bucket_name: str, project_id: str) -> pd.DataFrame:
    client  = storage.Client(project=project_id)
    bucket  = client.bucket(bucket_name)
    blob    = bucket.blob(_GCS_CSV_PATH)
    content = blob.download_as_text()
    return pd.read_csv(io.StringIO(content), parse_dates=["issue_date", "maturity_date"])


def _upload_parquet_to_gcs(df: pd.DataFrame, bucket_name: str, object_path: str, project_id: str) -> None:
    client = storage.Client(project=project_id)
    bucket = client.bucket(bucket_name)
    blob   = bucket.blob(object_path)
    buf    = io.BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow")
    buf.seek(0)
    blob.upload_from_file(buf, content_type="application/octet-stream")
    print(f"Uploaded snapshot to gs://{bucket_name}/{object_path}")


def _load_to_bigquery(df: pd.DataFrame, project_id: str) -> None:
    client    = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.{_BQ_TABLE}"

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=[
            bigquery.SchemaField("ingested_at",    "TIMESTAMP"),
            bigquery.SchemaField("snapshot_date",  "DATE"),
            bigquery.SchemaField("issue_code",     "STRING"),
            bigquery.SchemaField("issue_date",     "DATE"),
            bigquery.SchemaField("maturity_date",  "DATE"),
            bigquery.SchemaField("face_value",     "FLOAT64"),
            bigquery.SchemaField("currency",       "STRING"),
        ],
    )

    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()
    print(f"Loaded {len(df)} SSB holding(s) (total SGD {df['face_value'].sum():,.2f}) into {table_ref}")


def run(bucket_name: str, project_id: str, local_csv: Path | None = None) -> pd.DataFrame:
    if local_csv:
        print(f"Reading from local file: {local_csv.name}")
        df = pd.read_csv(local_csv, parse_dates=["issue_date", "maturity_date"])
    else:
        print(f"Reading from gs://{bucket_name}/{_GCS_CSV_PATH}")
        df = _read_csv_from_gcs(bucket_name, project_id)

    today = date.today()
    df["ingested_at"]   = pd.Timestamp.now(tz="UTC")
    df["snapshot_date"] = today

    print(f"  {len(df)} bond(s), total face value SGD {df['face_value'].sum():,.2f}")
    print(df[["issue_code", "issue_date", "maturity_date", "face_value"]].to_string(index=False))

    month_prefix = today.strftime("%Y-%m")
    gcs_path     = f"raw/ssb/{month_prefix}/ssb_{month_prefix}.parquet"

    if bucket_name:
        _upload_parquet_to_gcs(df, bucket_name, gcs_path, project_id)
    else:
        print("GCS_BUCKET_BANK not set — skipping GCS upload")

    if project_id:
        _load_to_bigquery(df, project_id)
    else:
        print("GCP_PROJECT_ID not set — skipping BigQuery load")

    return df


def main() -> None:
    load_dotenv()
    bucket     = os.environ.get("GCS_BUCKET_BANK", "")
    project_id = os.environ.get("GCP_PROJECT_ID", "")

    parser = argparse.ArgumentParser(description="SSB holdings pipeline")
    group  = parser.add_mutually_exclusive_group()
    group.add_argument("--csv",      help="Local path to ssb_holdings.csv (for local testing)")
    group.add_argument("--from-gcs", action="store_true", help="Read CSV from GCS (same as production)")
    args = parser.parse_args()

    if args.csv:
        run(bucket_name=bucket, project_id=project_id, local_csv=Path(args.csv))
    else:
        # default: read from GCS (used by Cloud Run scheduled job and --from-gcs flag)
        run(bucket_name=bucket, project_id=project_id)


if __name__ == "__main__":
    main()
