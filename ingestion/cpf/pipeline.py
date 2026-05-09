from __future__ import annotations

import argparse
import io
import os
import tempfile
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery, storage

from ingestion.cpf.pdf_parser import parse_pdf

_BQ_TABLE = "ods.ods_cpf_balances_df"


def _download_pdf_from_gcs(bucket_name: str, object_path: str) -> Path:
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob   = bucket.blob(object_path)
    tmp    = tempfile.NamedTemporaryFile(suffix=".pdf", delete=False)
    blob.download_to_filename(tmp.name)
    return Path(tmp.name)


def _upload_parquet_to_gcs(df: pd.DataFrame, bucket_name: str, object_path: str, project_id: str) -> None:
    client = storage.Client(project=project_id)
    bucket = client.bucket(bucket_name)
    blob   = bucket.blob(object_path)
    buf    = io.BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow")
    buf.seek(0)
    blob.upload_from_file(buf, content_type="application/octet-stream")
    print(f"Uploaded to gs://{bucket_name}/{object_path}")


def _already_loaded(client: bigquery.Client, project_id: str, statement_date) -> bool:
    query = f"""
        SELECT COUNT(*) AS cnt
        FROM `{project_id}.{_BQ_TABLE}`
        WHERE statement_date = '{statement_date}'
    """
    result = client.query(query).result()
    return next(iter(result)).cnt > 0


def _load_to_bigquery(df: pd.DataFrame, project_id: str) -> None:
    client    = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.{_BQ_TABLE}"

    statement_date = df["statement_date"].iloc[0]
    if _already_loaded(client, project_id, statement_date):
        print(f"CPF snapshot for {statement_date} already exists — skipping")
        return

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=[
            bigquery.SchemaField("etl_time",          "TIMESTAMP"),
            bigquery.SchemaField("source_file",       "STRING"),
            bigquery.SchemaField("statement_date",    "DATE"),
            bigquery.SchemaField("ordinary_account",  "FLOAT64"),
            bigquery.SchemaField("special_account",   "FLOAT64"),
            bigquery.SchemaField("medisave_account",  "FLOAT64"),
            bigquery.SchemaField("total_cpf",         "FLOAT64"),
            bigquery.SchemaField("currency",          "STRING"),
        ],
    )

    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()
    print(f"Loaded CPF snapshot (SGD {df['total_cpf'].iloc[0]:,.2f}) into {table_ref}")


def run(pdf_path: Path, bucket_name: str, project_id: str) -> pd.DataFrame:
    print(f"Parsing {pdf_path.name}...")
    snapshot = parse_pdf(pdf_path)
    print(f"  Date: {snapshot.statement_date}")
    print(f"  OA: SGD {snapshot.ordinary_account:,.2f}")
    print(f"  SA: SGD {snapshot.special_account:,.2f}")
    print(f"  MA: SGD {snapshot.medisave_account:,.2f}")
    print(f"  Total CPF: SGD {snapshot.total:,.2f}")

    df = pd.DataFrame([{
        "etl_time":         pd.Timestamp.now(tz="Asia/Singapore"),
        "source_file":      pdf_path.name,
        "statement_date":   snapshot.statement_date,
        "ordinary_account": snapshot.ordinary_account,
        "special_account":  snapshot.special_account,
        "medisave_account": snapshot.medisave_account,
        "total_cpf":        snapshot.total,
        "currency":         "SGD",
    }])

    month_prefix = snapshot.statement_date.strftime("%Y-%m")
    gcs_path     = f"raw/cpf/{month_prefix}/cpf_{month_prefix}.parquet"

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

    ce_subject = os.environ.get("CE_SUBJECT", "")

    parser = argparse.ArgumentParser(description="CPF account balances pipeline")
    parser.add_argument("--pdf", help="Local path to CPF PDF (for local runs)")
    args = parser.parse_args()

    if args.pdf:
        run(Path(args.pdf), bucket_name=bucket, project_id=project_id)
    elif ce_subject:
        object_path = ce_subject.split("/objects/", 1)[-1]
        pdf_path    = _download_pdf_from_gcs(bucket, object_path)
        run(pdf_path, bucket_name=bucket, project_id=project_id)
        Path(pdf_path).unlink(missing_ok=True)
    else:
        raise SystemExit("Provide --pdf or set CE_SUBJECT env var")


if __name__ == "__main__":
    main()
