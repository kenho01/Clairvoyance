#!/bin/sh
set -e

case "$PIPELINE" in
  bank)
    exec python -m ingestion.bank.pipeline "$@"
    ;;
  bank-service)
    exec gunicorn --bind "0.0.0.0:${PORT:-8080}" --workers 1 --threads 8 ingestion.bank.service:app
    ;;
  cpf-service)
    exec gunicorn --bind "0.0.0.0:${PORT:-8080}" --workers 1 --threads 8 ingestion.cpf.service:app
    ;;
  investment)
    exec python -m ingestion.investment.pipeline "$@"
    ;;
  cpf)
    exec python -m ingestion.cpf.pipeline "$@"
    ;;
  dbt)
    exec dbt run \
      --project-dir /app/dbt/clairvoyance \
      --profiles-dir /app/dbt/clairvoyance \
      --target prod \
      "$@"
    ;;
  *)
    echo "ERROR: Unknown PIPELINE='$PIPELINE'. Valid values: bank | bank-service | investment | cpf | cpf-service | dbt" >&2
    exit 1
    ;;
esac
