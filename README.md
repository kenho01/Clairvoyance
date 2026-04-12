# Clairvoyance

A personal finance data pipeline that automatically ingests bank statements, investment positions, CPF balances, and Singapore Savings Bonds into BigQuery — then transforms them into a net worth and spending dashboard via dbt.

## Architecture

![Architecture Diagram](images/Project%20-%20Clarity%20Architecture%20Diagram(1).png)

## Pipelines

| Pipeline | Trigger | What it does |
|---|---|---|
| `bank` | PDF dropped in GCS `inbox/` via Eventarc | Parses UOB savings & credit card PDFs, categorises transactions with Claude Haiku, loads to BQ |
| `investment` | Cloud Scheduler — daily 6am SGT | Fetches positions from Tiger Brokers, IBKR Flex, and Gemini; converts to SGD; loads to BQ |
| `cpf` | PDF dropped in GCS `inbox/` via Eventarc | Parses CPF Account Balances PDF (OA, SA, MA), loads to BQ |
| `ssb` | Cloud Scheduler — 1st of month 9am SGT | Reads `ssb_holdings.csv` from GCS `config/`, loads snapshot to BQ |
| `dbt` | Cloud Scheduler — daily 6:30am SGT | Runs dbt transformations across dwd → dws → ads layers |

## Data Layers (BigQuery)

```
ods   Raw ingested data (one table per source)
 │
dwd   Cleaned and typed (dedup, category overrides, date parsing)
 │
dws   Aggregated (net worth history, monthly spend, savings rate)
 │
ads   Dashboard-ready (pivoted net worth, MoM deltas, spend by category)
```

### Key tables

| Table | Description |
|---|---|
| `ods.ods_bank_transactions_df` | Raw bank transactions with LLM categories |
| `ods.ods_investment_positions_df` | Daily investment snapshots in SGD |
| `ods.ods_account_balances_df` | Monthly savings account closing balances |
| `ods.ods_cpf_balances_df` | CPF OA / SA / MA balances per statement |
| `ods.ods_ssb_holdings_df` | SSB holdings snapshot per run |
| `dwd.dwd_bank_transactions_df` | Cleaned transactions (transfers, income, reimbursements excluded) |
| `dws.dws_net_worth_history_df` | Monthly net worth by asset class |
| `ads.ads_net_worth_dashboard_df` | Pivoted: stocks, crypto, cash, SSB, CPF per month |
| `ads.ads_monthly_spend_dashboard_df` | Spend by category with MoM delta |

## Transaction Categories

The bank pipeline uses Claude Haiku to categorise transactions. Deterministic overrides in the dwd layer handle edge cases:

| Category | Filtered from spend dashboard |
|---|---|
| Transfer | Yes — credit card payments, inter-account |
| Income | Yes — salary, government credits, cashback |
| Reimbursement | Yes — incoming PayNow from named people |
| Groceries / Dining / Transport / etc. | No — shown in spend dashboard |

## Project Structure

```
ingestion/
  bank/
    pdf_parser.py      PDF → RawTransaction dataclass
    categoriser.py     Claude Haiku batch categorisation
    pipeline.py        Orchestrates parse → categorise → GCS → BQ
    service.py         Flask app — Eventarc target for PDF uploads
  investment/
    pipeline.py        Fetches Tiger, IBKR, Gemini positions
    tiger.py / ibkr.py / gemini.py
    fx.py              FX rates → SGD conversion
  cpf/
    pdf_parser.py      Extracts OA / SA / MA from CPF PDF
    pipeline.py
  ssb/
    pipeline.py        Reads ssb_holdings.csv from GCS

dbt/clairvoyance/
  models/
    dwd/               Clean layer
    dws/               Aggregation layer
    ads/               Dashboard layer
  profiles.yml         Uses GCP_PROJECT_ID env var (ADC auth)

terraform/
  main.tf              All GCP infrastructure
  variables.tf
  terraform.tfvars     ← gitignored, contains secrets

Dockerfile             Single image, PIPELINE env var routes entrypoint
entrypoint.sh          Routes: bank | bank-service | investment | cpf | ssb | dbt
```

## Infrastructure (Terraform)

All GCP resources are managed in `terraform/`:

- **Artifact Registry** — Docker image repository
- **GCS buckets** — bank and investments storage
- **BigQuery datasets** — ods, dwd, dws, ads
- **Secret Manager** — API keys for Anthropic, Tiger, IBKR, Gemini
- **Cloud Run Jobs** — bank, investment, cpf, ssb, dbt
- **Cloud Run Service** — bank-service (Eventarc target)
- **Eventarc trigger** — fires on GCS `object.finalized` in bank bucket
- **Cloud Scheduler** — investment (daily), dbt (daily), ssb (monthly)
- **Service Account** — `clairvoyance-pipeline` with least-privilege IAM roles

## Setup

### Prerequisites

- GCP project with billing enabled
- `gcloud` CLI authenticated
- `terraform` >= 1.5.7
- Docker with buildx
- Python 3.11+

### 1. Configure secrets

```bash
cp .env.example .env
# Fill in your API keys
```

```bash
cp terraform/terraform.tfvars.example terraform/terraform.tfvars
# Fill in project_id, secrets, and bucket names
```

### 2. Apply infrastructure

```bash
cd terraform
terraform init
terraform apply
```

### 3. Build and push the Docker image

```bash
# Authenticate Docker with Artifact Registry
gcloud auth configure-docker asia-southeast1-docker.pkg.dev

# Build for Cloud Run (linux/amd64)
docker buildx build --platform linux/amd64 \
  -t asia-southeast1-docker.pkg.dev/<PROJECT_ID>/clairvoyance/pipeline:latest \
  --push .
```

### 4. Update pipeline_image and re-apply

```hcl
# terraform/terraform.tfvars
pipeline_image = "asia-southeast1-docker.pkg.dev/<PROJECT_ID>/clairvoyance/pipeline:latest"
```

```bash
terraform apply
```

### 5. Trigger pipelines

**Bank statements**: Upload a PDF to `gs://<bucket>/inbox/` — Eventarc fires automatically.

**Investments**: Runs daily via Cloud Scheduler, or trigger manually:
```bash
gcloud run jobs execute clairvoyance-investment --region=asia-southeast1
```

**SSB**: Update `gs://<bucket>/config/ssb_holdings.csv` when you buy/redeem bonds:
```csv
issue_code,issue_date,maturity_date,face_value,currency
GX26010A,2026-01-01,2036-01-01,500,SGD
```

**dbt**: Runs daily via Cloud Scheduler, or trigger manually:
```bash
gcloud run jobs execute clairvoyance-dbt --region=asia-southeast1
```

## Local Development

```bash
# Install dependencies
pip install -e ".[dev]"

# Run bank pipeline locally
python -m ingestion.bank.pipeline --pdf path/to/statement.pdf

# Run investment pipeline locally
python -m ingestion.investment.pipeline

# Run tests
pytest
```

## Dashboard

Built in Google Looker Studio, connected directly to BigQuery.

Key data sources:
- `ads.ads_net_worth_dashboard_df` — net worth scorecards and time series
- `ads.ads_monthly_spend_dashboard_df` — spending by category with MoM delta
- `dwd.dwd_bank_transactions_df` — top merchants table

A static HTML mockup is available at `docs/dashboard_mockup.html`.
