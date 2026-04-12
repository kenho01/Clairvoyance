with investments as (
    select
        snapshot_date,
        source,
        asset_class,
        market_value_sgd
    from {{ ref('dwd_investment_positions_df') }}
),

bank_balances as (
    select
        cast(concat(statement_date, '-01') as date)  as snapshot_date,
        'uob_savings'                 as source,
        'cash'                        as asset_class,
        round(sum(balance), 2)        as market_value_sgd
    from `{{ env_var('GCP_PROJECT_ID') }}.ods.ods_account_balances_df`
    group by snapshot_date, source, asset_class
),

cpf_balances as (
    select date_trunc(statement_date, month) as snapshot_date, 'cpf' as source, 'cpf_oa' as asset_class, round(ordinary_account, 2) as market_value_sgd from `{{ env_var('GCP_PROJECT_ID') }}.ods.ods_cpf_balances_df`
    union all
    select date_trunc(statement_date, month),                          'cpf',            'cpf_sa',                                  round(special_account, 2)   from `{{ env_var('GCP_PROJECT_ID') }}.ods.ods_cpf_balances_df`
    union all
    select date_trunc(statement_date, month),                          'cpf',            'cpf_ma',                                  round(medisave_account, 2)  from `{{ env_var('GCP_PROJECT_ID') }}.ods.ods_cpf_balances_df`
),

investment_daily as (
    select
        date_trunc(snapshot_date, month) as snapshot_date,
        source,
        asset_class,
        -- use the latest daily snapshot within each month
        round(sum(market_value_sgd), 2) as market_value_sgd
    from investments
    where snapshot_date = (
        select max(i2.snapshot_date)
        from {{ ref('dwd_investment_positions_df') }} i2
        where date_trunc(i2.snapshot_date, month) = date_trunc(investments.snapshot_date, month)
    )
    group by date_trunc(snapshot_date, month), source, asset_class
),

ssb_latest as (
    -- keep only the most recent snapshot per calendar month
    select *
    from (
        select *,
            row_number() over (
                partition by date_trunc(snapshot_date, month)
                order by snapshot_date desc
            ) as rn
        from `{{ env_var('GCP_PROJECT_ID') }}.ods.ods_ssb_holdings_df`
    )
    where rn = 1
),

ssb_balances as (
    select
        date_trunc(snapshot_date, month)  as snapshot_date,
        'ssb'                             as source,
        'ssb'                             as asset_class,
        round(sum(face_value), 2)         as market_value_sgd
    from ssb_latest
    group by snapshot_date, source, asset_class
),

combined as (
    select snapshot_date, source, asset_class, market_value_sgd from investment_daily
    union all
    select snapshot_date, source, asset_class, market_value_sgd from bank_balances
    union all
    select snapshot_date, source, asset_class, market_value_sgd from cpf_balances
    union all
    select snapshot_date, source, asset_class, market_value_sgd from ssb_balances
)

select * from combined
