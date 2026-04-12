with raw as (
    select * from `{{ env_var('GCP_PROJECT_ID') }}.ods.ods_bank_transactions_df`
),

-- Deterministic category overrides (take precedence over LLM output)
overridden as (
    select
        * replace(
            case
                -- Credit card bill payments
                when transaction_type = 'Credit Card'
                     and amount > 0
                     and upper(description) like '%PAYMT%'
                then 'Transfer'
                -- Salary / payroll
                when amount > 0
                     and (
                         upper(description) like '%SALA%'
                         or upper(description) like '%SALARY%'
                         or upper(description) like '%PAYROLL%'
                     )
                then 'Income'
                -- Government credits
                when amount > 0
                     and (
                         upper(description) like '%GOV |%'
                         or upper(description) like '%GST VOUCHER%'
                         or upper(description) like '%CDC VOUCHER%'
                         or upper(description) like '%SKILLSFUTURE%'
                         or upper(description) like '%WORKFARE%'
                     )
                then 'Income'
                -- Cashback / rebates
                when amount > 0
                     and (
                         upper(description) like '%CASH REBATE%'
                         or upper(description) like '%CASHBACK%'
                         or upper(description) like '%REBATE%'
                     )
                then 'Income'
                -- Incoming PayNow from a named person (UOB format: "PAYNOW OTHR | NAME | ...")
                when amount > 0
                     and transaction_type = ''
                     and upper(description) like 'PAYNOW OTHR |%'
                then 'Reimbursement'
                else category
            end as category
        )
    from raw
),

cleaned as (
    select
        to_hex(md5(concat(
            coalesce(source_file, ''),
            coalesce(date, ''),
            coalesce(description, ''),
            coalesce(cast(amount as string), '')
        ))) as transaction_id,

        cast(ingested_at as timestamp) as ingested_at,
        source_file,
        transaction_type,
        parse_date('%d %b %Y', date) as transaction_date,
        format_date('%Y-%m', parse_date('%d %b %Y', date)) as year_month,
        description,
        amount,
        currency,
        category

    from overridden
    where category not in ('Transfer', 'Income', 'Reimbursement')   -- exclude payments, transfers, income, and friend reimbursements
)

select * from cleaned
