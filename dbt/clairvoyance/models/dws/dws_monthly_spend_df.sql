with base as (
    select * from {{ ref('dwd_bank_transactions_df') }}
),

aggregated as (
    select
        year_month,
        category,
        round(sum(case when amount < 0 then abs(amount) else 0 end), 2) as total_debit,
        round(sum(case when amount > 0 then amount       else 0 end), 2) as total_credit,
        round(sum(case when amount < 0 then abs(amount) else 0 end)
            - sum(case when amount > 0 then amount       else 0 end), 2) as net_spend,
        count(*) as tx_count
    from base
    group by 1, 2
)

select * from aggregated
order by year_month, category
