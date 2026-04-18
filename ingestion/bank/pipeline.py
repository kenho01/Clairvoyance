from __future__ import annotations

import argparse
import io
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery, storage

from ingestion.bank.categoriser import categorise
from ingestion.bank.pdf_parser import parse_pdf, transactions_to_dataframe

_BQ_TABLE          = "ods.ods_bank_transactions_df"
_BQ_BALANCES_TABLE = "ods.ods_account_balances_df"


def _download_pdf_from_gcs(bucket_name: str, object_path: str) -> Path:
    """Download a PDF from GCS inbox to a temp file and return its local path."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_path)

    tmp = tempfile.NamedTemporaryFile(suffix=".pdf", delete=False)
    blob.download_to_filename(tmp.name)
    return Path(tmp.name)


def _upload_parquet_to_gcs(df: pd.DataFrame, bucket_name: str, object_path: str, project_id: str) -> None:
    """Upload a DataFrame as Parquet to GCS."""
    client = storage.Client(project=project_id)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_path)

    buf = io.BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow")
    buf.seek(0)
    blob.upload_from_file(buf, content_type="application/octet-stream")
    print(f"Uploaded to gs://{bucket_name}/{object_path}")


def _load_to_bigquery(df: pd.DataFrame, project_id: str) -> None:
    """Append rows to ods.bank_transactions in BigQuery."""
    client = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.{_BQ_TABLE}"

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=[
            bigquery.SchemaField("etl_time",         "TIMESTAMP"),
            bigquery.SchemaField("source_file",      "STRING"),
            bigquery.SchemaField("transaction_type", "STRING"),
            bigquery.SchemaField("date",             "STRING"),
            bigquery.SchemaField("description",      "STRING"),
            bigquery.SchemaField("amount",           "FLOAT64"),
            bigquery.SchemaField("currency",         "STRING"),
            bigquery.SchemaField("category",         "STRING"),
        ],
    )

    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()
    print(f"Loaded {len(df)} rows into {table_ref}")


def _load_balance_to_bigquery(
    closing_balance: float,
    statement_date: str,
    source_file: str,
    project_id: str,
) -> None:
    """Store the savings account closing balance in ods.account_balances."""
    client    = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.{_BQ_BALANCES_TABLE}"

    df = pd.DataFrame([{
        "etl_time":       pd.Timestamp.now(tz="Asia/Singapore"),
        "source":         "uob_savings",
        "source_file":    source_file,
        "statement_date": statement_date,
        "balance":        closing_balance,
        "currency":       "SGD",
    }])

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=[
            bigquery.SchemaField("etl_time",       "TIMESTAMP"),
            bigquery.SchemaField("source",         "STRING"),
            bigquery.SchemaField("source_file",    "STRING"),
            bigquery.SchemaField("statement_date", "STRING"),
            bigquery.SchemaField("balance",        "FLOAT64"),
            bigquery.SchemaField("currency",       "STRING"),
        ],
    )

    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()
    print(f"Loaded closing balance SGD {closing_balance:,.2f} into {table_ref}")


def run(pdf_path: Path, bucket_name: str, project_id: str, source_filename: str | None = None) -> pd.DataFrame:
    """Parse, categorise, upload to GCS raw/, and load into BigQuery ODS."""
    # Use the original filename (passed from service) rather than a temp name
    original_name = source_filename or pdf_path.name

    print(f"Parsing {pdf_path.name}...")
    transactions, stmt_type, closing_balance = parse_pdf(pdf_path)
    print(f"  Found {len(transactions)} transactions ({stmt_type})")

    if not transactions:
        print("  No transactions found — skipping")
        return transactions_to_dataframe(transactions)

    # Derive month from the statement's own transactions, not today's date
    first_date = pd.to_datetime(transactions[0].date, format="%d %b %Y")
    month_prefix = first_date.strftime("%Y-%m")
    filename = f"uob_{stmt_type}_{month_prefix}.parquet"
    gcs_path = f"raw/{stmt_type}/{month_prefix}/{filename}"

    # Deduplication: check GCS parquet existence before the expensive categorisation step.
    # GCS is strongly consistent — once the first request writes the parquet, all
    # concurrent duplicates will see it and bail out before calling Claude.
    if bucket_name:
        gcs_client = storage.Client(project=project_id)
        if gcs_client.bucket(bucket_name).blob(gcs_path).exists():
            print(f"  Parquet already exists at gs://{bucket_name}/{gcs_path} — skipping")
            return transactions_to_dataframe(transactions)

    print("Categorising with Claude Haiku...")
    transactions = categorise(transactions)

    df = transactions_to_dataframe(transactions)
    df["etl_time"] = pd.Timestamp.now(tz="Asia/Singapore")
    df["source_file"] = original_name

    if bucket_name:
        _upload_parquet_to_gcs(df, bucket_name, gcs_path, project_id)
    else:
        print("GCS_BUCKET_BANK not set — skipping GCS upload")

    if project_id:
        _load_to_bigquery(df, project_id)
        if closing_balance is not None:
            _load_balance_to_bigquery(closing_balance, month_prefix, original_name, project_id)
    else:
        print("GCP_PROJECT_ID not set — skipping BigQuery load")

    return df


def main() -> None:
    load_dotenv()
    bucket     = os.environ.get("GCS_BUCKET_BANK", "")
    project_id = os.environ.get("GCP_PROJECT_ID", "")

    # When triggered by Eventarc, the GCS object path is in CE_SUBJECT
    ce_subject = os.environ.get("CE_SUBJECT", "")

    parser = argparse.ArgumentParser(description="Bank PDF pipeline")
    parser.add_argument("--pdf", help="Local path to PDF (for local runs)")
    args = parser.parse_args()

    if args.pdf:
        pdf_path = Path(args.pdf)
        run(pdf_path, bucket_name=bucket, project_id=project_id)
    elif ce_subject:
        # CE_SUBJECT looks like: /projects/_/buckets/<bucket>/objects/inbox/<filename>
        object_path = ce_subject.split("/objects/", 1)[-1]
        pdf_path = _download_pdf_from_gcs(bucket, object_path)
        run(pdf_path, bucket_name=bucket, project_id=project_id)
        Path(pdf_path).unlink(missing_ok=True)
    else:
        raise SystemExit("Provide --pdf or set CE_SUBJECT env var")


if __name__ == "__main__":
    main()
