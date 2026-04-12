FROM python:3.11-slim

WORKDIR /app

# System deps required by pdfplumber
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpango-1.0-0 \
    libpangoft2-1.0-0 \
    && rm -rf /var/lib/apt/lists/*

# Python dependencies (includes dbt-bigquery)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir dbt-bigquery==1.9.0

# Application code
COPY pyproject.toml .
COPY ingestion/ ingestion/
COPY dbt/ dbt/
RUN pip install --no-cache-dir -e .

COPY entrypoint.sh .
RUN chmod +x entrypoint.sh

# PIPELINE env var selects which pipeline to run:
#   bank | investment | cpf | ssb | dbt
ENV PIPELINE=bank

ENTRYPOINT ["./entrypoint.sh"]
