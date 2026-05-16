with base as (
    select * from {{ ref('dws_monthly_spend_df') }}
),

with_mom as (
    select
        year_month,
        category,
        total_debit,
        total_credit,
        net_spend,
        tx_count,
        year_month = format_date('%Y-%m', date_sub(date_trunc(current_date(), month), interval 1 month)) as is_previous_month,
        lag(net_spend) over (partition by category order by year_month) as prev_month_spend,
        round(
            net_spend - lag(net_spend) over (partition by category order by year_month),
            2
        ) as mom_delta,
        round(
            safe_divide(
                net_spend - lag(net_spend) over (partition by category order by year_month),
                nullif(lag(net_spend) over (partition by category order by year_month), 0)
            ) * 100,
            1
        ) as mom_pct
    from base
)

select * from with_mom
order by year_month, category
