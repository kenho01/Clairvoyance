with investments as (
    select
        date,
        source,
        asset_class,
        market_value_sgd
    from {{ ref('dwd_investment_positions_df') }}
),

bank_balances as (
    select
        cast(concat(statement_date, '-01') as date)  as date,
        'uob_savings' as source,
        'cash' as asset_class,
        round(sum(balance), 2) as market_value_sgd
    from `{{ env_var('GCP_PROJECT_ID') }}.ods.ods_account_balances_df`
    group by date, source, asset_class
),

cpf_latest as (
    select *
    from (
        select *,
            row_number() over (
                partition by date_trunc(statement_date, month)
                order by statement_date desc
            ) as rn
        from `{{ env_var('GCP_PROJECT_ID') }}.ods.ods_cpf_balances_df`
    )
    where rn = 1
),

cpf_balances as (
    select date_trunc(statement_date, month) as date, 'cpf' as source, 'cpf_oa' as asset_class, round(ordinary_account, 2) as market_value_sgd from cpf_latest
    union all
    select date_trunc(statement_date, month), 'cpf', 'cpf_sa', round(special_account, 2)   from cpf_latest
    union all
    select date_trunc(statement_date, month), 'cpf', 'cpf_ma', round(medisave_account, 2)  from cpf_latest
),

investment_daily as (
    select
        date_trunc(date, month) as date,
        source,
        asset_class,
        -- use the latest daily snapshot within each month
        round(sum(market_value_sgd), 2) as market_value_sgd
    from investments
    where date = (
        select max(i2.date)
        from {{ ref('dwd_investment_positions_df') }} i2
        where date_trunc(i2.date, month) = date_trunc(investments.date, month)
    )
    group by date_trunc(date, month), source, asset_class
),

endowus_monthly as (
    select
        date_trunc(parse_date('%d %b %Y', date), month) as month,
        round(sum(abs(amount)), 2) as monthly_sgd
    from `{{ env_var('GCP_PROJECT_ID') }}.ods.ods_bank_transactions_df`
    where category = 'Endowus'
      and amount < 0
    group by 1
),

endowus_cumulative as (
    select
        month as date,
        'endowus' as source,
        'endowus' as asset_class,
        round(sum(monthly_sgd) over (order by month rows between unbounded preceding and current row), 2) as market_value_sgd
    from endowus_monthly
),

combined as (
    select date, source, asset_class, market_value_sgd from investment_daily
    union all
    select date, source, asset_class, market_value_sgd from bank_balances
    union all
    select date, source, asset_class, market_value_sgd from cpf_balances
    union all
    select date, source, asset_class, market_value_sgd from endowus_cumulative
)

select * from combined
