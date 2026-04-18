# BigQuery Schema

## Layer Overview

```
ODS  → raw ingested data, no transformation
DWD  → cleaned, typed, category overrides applied
DWS  → aggregated metrics (monthly spend, net worth history, savings rate)
ADS  → dashboard-ready views (pivoted, carry-forward, MoM deltas)
```

---

## ODS — Operational Data Store

### `ods.ods_bank_transactions_df`
Raw rows parsed from UOB PDF statements.

| Column | Type | Notes |
|---|---|---|
| etl_time | TIMESTAMP | Pipeline load time |
| source_file | STRING | Original PDF filename |
| transaction_type | STRING | e.g. `"Credit Card"`, `"FAST Payment"` |
| date | STRING | Raw date string, e.g. `"07 FEB 2026"` |
| description | STRING | Raw merchant / description text |
| amount | FLOAT64 | Negative = debit, positive = credit |
| currency | STRING | Always `"SGD"` for UOB statements |
| category | STRING | Assigned by Claude Haiku categoriser |

### `ods.ods_investment_positions_df`
Daily position snapshots from all brokers.

| Column | Type | Notes |
|---|---|---|
| etl_time | TIMESTAMP | Pipeline load time |
| date | DATE | Snapshot date |
| source | STRING | `"ibkr"` \| `"tiger"` \| `"gemini"` |
| symbol | STRING | Ticker or crypto symbol, e.g. `"VWRA"`, `"BTC"` |
| asset_class | STRING | `"stock"` \| `"etf"` \| `"crypto"` |
| quantity | FLOAT64 | Units held |
| price | FLOAT64 | Price per unit in native currency |
| market_value | FLOAT64 | `quantity × price` in native currency |
| currency | STRING | Native currency, e.g. `"USD"`, `"HKD"` |
| fx_rate_to_sgd | FLOAT64 | Exchange rate used |
| market_value_sgd | FLOAT64 | `market_value × fx_rate_to_sgd` |

### `ods.ods_account_balances_df`
Monthly UOB savings account closing balance from PDF statement.

| Column | Type | Notes |
|---|---|---|
| etl_time | TIMESTAMP | Pipeline load time |
| source_file | STRING | Original PDF filename |
| statement_date | STRING | `"YYYY-MM"` |
| balance | FLOAT64 | Closing balance in SGD |
| currency | STRING | `"SGD"` |

### `ods.ods_cpf_balances_df`
CPF account balances extracted from the CPF Account Balances PDF.

| Column | Type | Notes |
|---|---|---|
| etl_time | TIMESTAMP | Pipeline load time |
| source_file | STRING | Original PDF filename |
| statement_date | DATE | Statement date |
| ordinary_account | FLOAT64 | OA balance in SGD |
| special_account | FLOAT64 | SA balance in SGD |
| medisave_account | FLOAT64 | MA balance in SGD |

### `ods.ods_ssb_holdings_df`
SSB holdings snapshot loaded from `config/ssb_holdings.csv`.

| Column | Type | Notes |
|---|---|---|
| date | DATE | Date the snapshot was taken |
| issue_code | STRING | e.g. `"GX26010A"` |
| issue_date | DATE | |
| maturity_date | DATE | |
| face_value | FLOAT64 | SGD face value held |
| currency | STRING | `"SGD"` |

---

## DWD — Data Warehouse Detail

### `dwd.dwd_bank_transactions_df`
Cleaned and enriched transactions. Transfers, income, and reimbursements are excluded.

| Column | Type | Notes |
|---|---|---|
| transaction_id | STRING | MD5 hash of `(source_file, date, description, amount)` |
| etl_time | TIMESTAMP | |
| source_file | STRING | |
| transaction_type | STRING | |
| transaction_date | DATE | Parsed from raw date string |
| year_month | STRING | `"YYYY-MM"` — partition/filter helper |
| description | STRING | |
| amount | FLOAT64 | Negative = debit, positive = credit |
| currency | STRING | |
| category | STRING | Post-override category |

**Category overrides applied (deterministic rules take precedence over LLM):**
- Credit card payment rows → `Transfer`
- Salary / payroll credits → `Income`
- Government credits (GST voucher, CDC, SkillsFuture, Workfare) → `Income`
- Cashback / rebates → `Income`
- Incoming PayNow from named individuals → `Reimbursement`

**Rows excluded from this table:** `Transfer`, `Income`, `Reimbursement`

### `dwd.dwd_investment_positions_df`
Cleaned investment positions with SGD normalisation.

| Column | Type | Notes |
|---|---|---|
| etl_time | TIMESTAMP | |
| date | DATE | Snapshot date |
| source | STRING | Broker name |
| symbol | STRING | |
| asset_class | STRING | |
| quantity | FLOAT64 | |
| price | FLOAT64 | Native currency |
| market_value | FLOAT64 | Native currency |
| currency | STRING | |
| fx_rate_to_sgd | FLOAT64 | |
| market_value_sgd | FLOAT64 | |

---

## DWS — Data Warehouse Summary

### `dws.dws_net_worth_history_df`
Monthly net worth broken down by asset class and source. Each row is one asset_class/source combination per month.

| Column | Type | Notes |
|---|---|---|
| date | DATE | First day of the month |
| source | STRING | e.g. `"ibkr"`, `"tiger"`, `"cpf"`, `"uob_savings"`, `"ssb"` |
| asset_class | STRING | `"stock"` \| `"etf"` \| `"crypto"` \| `"cash"` \| `"cpf_oa"` \| `"cpf_sa"` \| `"cpf_ma"` \| `"ssb"` |
| market_value_sgd | FLOAT64 | SGD value for this row |

### `dws.dws_monthly_spend_df`
Monthly spending aggregated by category.

| Column | Type | Notes |
|---|---|---|
| year_month | STRING | `"YYYY-MM"` |
| category | STRING | |
| total_debit | FLOAT64 | Sum of debits (positive) |
| total_credit | FLOAT64 | Sum of credits / refunds |
| net_spend | FLOAT64 | `total_debit − total_credit` |
| tx_count | INT64 | Number of transactions |

### `dws.dws_monthly_savings_rate_df`
Monthly income vs spend summary.

| Column | Type | Notes |
|---|---|---|
| year_month | STRING | `"YYYY-MM"` |
| total_income | FLOAT64 | Sum of all positive amounts |
| total_spend | FLOAT64 | Sum of all debits |
| net_savings | FLOAT64 | `total_income − total_spend` |
| savings_rate_pct | FLOAT64 | `net_savings / total_income × 100` |

---

## ADS — Application Data Store

### `ads.ads_net_worth_dashboard_df`
Monthly pivoted net worth. Non-daily assets (cash, CPF, SSB) are carried forward so every month has a complete row.

| Column | Type | Notes |
|---|---|---|
| date | DATE | First day of the month |
| stocks_sgd | FLOAT64 | Stocks + ETFs |
| crypto_sgd | FLOAT64 | Crypto positions |
| cash_sgd | FLOAT64 | UOB savings balance (carried forward) |
| ssb_sgd | FLOAT64 | SSB face value (carried forward) |
| cpf_oa_sgd | FLOAT64 | CPF Ordinary Account (carried forward) |
| cpf_sa_sgd | FLOAT64 | CPF Special Account (carried forward) |
| cpf_ma_sgd | FLOAT64 | CPF MediSave Account (carried forward) |
| total_cpf_sgd | FLOAT64 | `cpf_oa + cpf_sa + cpf_ma` |
| total_net_worth_excl_cpf_sgd | FLOAT64 | Stocks + crypto + cash + SSB |
| total_net_worth_sgd | FLOAT64 | All assets including CPF |

### `ads.ads_net_worth_daily_df`
Daily pivoted net worth (investment positions updated daily; cash/CPF/SSB carried forward).

Same columns as `ads_net_worth_dashboard_df`, plus:

| Column | Type | Notes |
|---|---|---|
| day_over_day_change_sgd | FLOAT64 | `total_net_worth_sgd − previous day` |

### `ads.ads_monthly_spend_dashboard_df`
Monthly spend by category with month-over-month comparison.

| Column | Type | Notes |
|---|---|---|
| year_month | STRING | `"YYYY-MM"` |
| category | STRING | |
| total_debit | FLOAT64 | |
| total_credit | FLOAT64 | |
| net_spend | FLOAT64 | |
| tx_count | INT64 | |
| prev_month_spend | FLOAT64 | Previous month's `net_spend` for this category |
| mom_delta | FLOAT64 | `net_spend − prev_month_spend` |
| mom_pct | FLOAT64 | % change month-over-month |
