with base as (
    select * from {{ ref('dws_net_worth_history_df') }}
),

pivoted as (
    select
        date,
        round(sum(case when asset_class in ('stock', 'etf') then market_value_sgd else 0 end), 2) as stocks_sgd,
        round(sum(case when asset_class = 'crypto' then market_value_sgd else 0 end), 2) as crypto_sgd,
        -- null if no statement for this date (will be filled forward below)
        nullif(round(sum(case when asset_class = 'cash' then market_value_sgd else 0 end), 2), 0) as cash_sgd,
        nullif(round(sum(case when asset_class = 'cpf_oa' then market_value_sgd else 0 end), 2), 0) as cpf_oa_sgd,
        nullif(round(sum(case when asset_class = 'cpf_sa' then market_value_sgd else 0 end), 2), 0) as cpf_sa_sgd,
        nullif(round(sum(case when asset_class = 'cpf_ma' then market_value_sgd else 0 end), 2), 0) as cpf_ma_sgd,
        nullif(round(sum(case when asset_class = 'endowus' then market_value_sgd else 0 end), 2), 0) as endowus_principal_sgd
    from base
    group by date
),

with_carried_forward as (
    select
        date,
        stocks_sgd,
        crypto_sgd,
        last_value(cash_sgd ignore nulls) over (order by date rows between unbounded preceding and current row) as cash_sgd,
        last_value(cpf_oa_sgd ignore nulls) over (order by date rows between unbounded preceding and current row) as cpf_oa_sgd,
        last_value(cpf_sa_sgd ignore nulls) over (order by date rows between unbounded preceding and current row) as cpf_sa_sgd,
        last_value(cpf_ma_sgd ignore nulls) over (order by date rows between unbounded preceding and current row) as cpf_ma_sgd,
        last_value(endowus_principal_sgd ignore nulls) over (order by date rows between unbounded preceding and current row) as endowus_principal_sgd
    from pivoted
),

final as (
    select
        date,
        stocks_sgd,
        crypto_sgd,
        coalesce(cash_sgd, 0) as cash_sgd,
        coalesce(endowus_principal_sgd, 0) as endowus_principal_sgd,
        coalesce(cpf_oa_sgd, 0) as cpf_oa_sgd,
        coalesce(cpf_sa_sgd, 0) as cpf_sa_sgd,
        coalesce(cpf_ma_sgd, 0) as cpf_ma_sgd,
        round(
            coalesce(cpf_oa_sgd, 0) + coalesce(cpf_sa_sgd, 0) + coalesce(cpf_ma_sgd, 0),
            2
        ) as total_cpf_sgd,
        round(
            stocks_sgd + crypto_sgd + coalesce(cash_sgd, 0) + coalesce(endowus_principal_sgd, 0),
            2
        ) as total_net_worth_excl_cpf_sgd,
        round(
            stocks_sgd + crypto_sgd
            + coalesce(cash_sgd,   0)
            + coalesce(endowus_principal_sgd, 0)
            + coalesce(cpf_oa_sgd, 0)
            + coalesce(cpf_sa_sgd, 0)
            + coalesce(cpf_ma_sgd, 0),
            2
        ) as total_net_worth_sgd
    from with_carried_forward
)

select * from final
order by date
