# BigQuery Schema Design

## Layer Overview

```
ODS  → raw ingested data, no transformation
DWD  → cleaned, enriched, typed
DWS  → aggregated metrics
ADS  → dashboard-ready views
```

---

## ODS — Operational Data Store

### `ods.bank_transactions`
Raw rows as parsed from UOB PDF statements.

| Column           | Type      | Notes                                      |
|------------------|-----------|--------------------------------------------|
| ingested_at      | TIMESTAMP | Load timestamp (pipeline adds this)        |
| source_file      | STRING    | Original PDF filename                      |
| transaction_type | STRING    | E.g. "Credit Card", "FAST Payment"         |
| date             | STRING    | Raw date string, e.g. "07 FEB 2026"        |
| description      | STRING    | Raw merchant/description text              |
| amount           | FLOAT64   | Negative = debit, positive = credit        |
| currency         | STRING    | Always "SGD" for UOB statements            |

### `ods.investment_positions`
Raw position snapshots from all brokers.

| Column       | Type      | Notes                                          |
|--------------|-----------|------------------------------------------------|
| ingested_at  | TIMESTAMP | Load timestamp                                 |
| source       | STRING    | "ibkr" \| "tiger" \| "gemini"                  |
| symbol       | STRING    | Ticker or crypto symbol, e.g. "VWRA", "BTC"    |
| asset_class  | STRING    | "stock" \| "etf" \| "crypto" \| "cash"         |
| quantity     | FLOAT64   | Number of units held                           |
| price        | FLOAT64   | Price per unit in native currency              |
| market_value | FLOAT64   | quantity × price in native currency            |
| currency     | STRING    | Native currency, e.g. "USD", "HKD"             |

---

## DWD — Data Warehouse Detail

### `dwd.bank_transactions`
Cleaned and enriched bank transactions.

| Column           | Type      | Notes                                        |
|------------------|-----------|----------------------------------------------|
| transaction_id   | STRING    | SHA256 of (source_file + date + description + amount) |
| source_file      | STRING    | Carried from ODS                             |
| transaction_type | STRING    | Carried from ODS                             |
| transaction_date | DATE      | Parsed from raw date string                  |
| description      | STRING    | Trimmed                                      |
| amount           | FLOAT64   | Negative = debit, positive = credit          |
| currency         | STRING    | "SGD"                                        |
| category         | STRING    | Assigned by Claude Haiku categoriser         |
| year_month       | STRING    | "2026-02" (partition/filter helper)          |

### `dwd.investment_positions`
Cleaned positions with SGD-normalised values.

| Column            | Type      | Notes                                      |
|-------------------|-----------|--------------------------------------------|
| snapshot_date     | DATE      | Date the snapshot was taken                |
| source            | STRING    | Broker name                                |
| symbol            | STRING    | Ticker or crypto symbol                    |
| asset_class       | STRING    | Standardised asset class                   |
| quantity          | FLOAT64   |                                            |
| price             | FLOAT64   | In native currency                         |
| market_value      | FLOAT64   | In native currency                         |
| currency          | STRING    | Native currency                            |
| fx_rate_to_sgd    | FLOAT64   | Exchange rate used (USD→SGD, HKD→SGD etc.) |
| market_value_sgd  | FLOAT64   | market_value × fx_rate_to_sgd              |

---

## DWS — Data Warehouse Summary

### `dws.monthly_spend`
Monthly spending aggregated by category.

| Column        | Type    | Notes                              |
|---------------|---------|------------------------------------|
| year_month    | STRING  | "2026-02"                          |
| category      | STRING  | Spending category                  |
| total_debit   | FLOAT64 | Sum of debits (positive value)     |
| total_credit  | FLOAT64 | Sum of credits (refunds/payments)  |
| net_spend     | FLOAT64 | total_debit - total_credit         |
| tx_count      | INT64   | Number of transactions             |

### `dws.net_worth_history`
Net worth snapshot per day.

| Column               | Type    | Notes                              |
|----------------------|---------|------------------------------------|
| snapshot_date        | DATE    |                                    |
| total_investments_sgd| FLOAT64 | Sum of all position market values  |
| asset_class          | STRING  | Breakdown by asset class           |
| source               | STRING  | Breakdown by broker                |
| market_value_sgd     | FLOAT64 | Market value for this row          |

---

## ADS — Application Data Store

### `ads.monthly_spend_dashboard`
Flattened for Looker Studio. Combines spend categories with month-over-month delta.

| Column        | Type    | Notes                                  |
|---------------|---------|----------------------------------------|
| year_month    | STRING  |                                        |
| category      | STRING  |                                        |
| net_spend     | FLOAT64 |                                        |
| prev_month    | FLOAT64 | net_spend from previous month          |
| mom_delta     | FLOAT64 | net_spend - prev_month                 |
| mom_pct       | FLOAT64 | % change month-over-month              |

### `ads.net_worth_dashboard`
Daily net worth trend for charting.

| Column               | Type    | Notes                              |
|----------------------|---------|------------------------------------|
| snapshot_date        | DATE    |                                    |
| total_net_worth_sgd  | FLOAT64 | Total across all brokers           |
| stocks_sgd           | FLOAT64 | Stocks + ETFs                      |
| crypto_sgd           | FLOAT64 | Crypto positions                   |
| cash_sgd             | FLOAT64 | Cash holdings                      |
