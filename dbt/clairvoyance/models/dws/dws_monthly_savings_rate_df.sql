with base as (
    select * from {{ ref('dwd_bank_transactions_df') }}
),

monthly as (
    select
        year_month,
        round(sum(case when amount > 0 then amount else 0 end), 2) as total_income,
        round(sum(case when amount < 0 then abs(amount) else 0 end), 2) as total_spend
    from base
    group by 1
),

with_rate as (
    select
        year_month,
        total_income,
        total_spend,
        round(total_income - total_spend, 2) as net_savings,
        case
            when total_income > 0
            then round((total_income - total_spend) / total_income * 100, 1)
            else null
        end as savings_rate_pct
    from monthly
)

select * from with_rate
order by year_month
