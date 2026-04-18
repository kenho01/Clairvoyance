with investment_daily as (
    select
        date,
        round(sum(case when asset_class in ('stock', 'etf') then market_value_sgd else 0 end), 2) as stocks_sgd,
        round(sum(case when asset_class = 'crypto' then market_value_sgd else 0 end), 2) as crypto_sgd
    from {{ ref('dwd_investment_positions_df') }}
    group by date
),

bank_balances as (
    select
        cast(concat(statement_date, '-01') as date)  as date,
        round(sum(balance), 2) as cash_sgd
    from `{{ env_var('GCP_PROJECT_ID') }}.ods.ods_account_balances_df`
    group by date
),

cpf_balances as (
    select
        date_trunc(statement_date, month)  as date,
        round(ordinary_account, 2) as cpf_oa_sgd,
        round(special_account, 2) as cpf_sa_sgd,
        round(medisave_account, 2) as cpf_ma_sgd
    from `{{ env_var('GCP_PROJECT_ID') }}.ods.ods_cpf_balances_df`
),

ssb_latest as (
    select *
    from (
        select *,
            row_number() over (
                partition by date_trunc(date, month)
                order by date desc
            ) as rn
        from `{{ env_var('GCP_PROJECT_ID') }}.ods.ods_ssb_holdings_df`
    )
    where rn = 1
),

ssb_balances as (
    select
        date_trunc(date, month) as date,
        round(sum(face_value), 2) as ssb_sgd
    from ssb_latest
    group by date_trunc(date, month)
),

joined as (
    select
        i.date,
        i.stocks_sgd,
        i.crypto_sgd,
        b.cash_sgd,
        c.cpf_oa_sgd,
        c.cpf_sa_sgd,
        c.cpf_ma_sgd,
        s.ssb_sgd
    from investment_daily i
    left join bank_balances b on b.date = date_trunc(i.date, month)
    left join cpf_balances c on c.date = date_trunc(i.date, month)
    left join ssb_balances s on s.date = date_trunc(i.date, month)
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
        last_value(ssb_sgd ignore nulls) over (order by date rows between unbounded preceding and current row) as ssb_sgd
    from joined
),

final as (
    select
        date,
        stocks_sgd,
        crypto_sgd,
        coalesce(cash_sgd, 0)  as cash_sgd,
        coalesce(ssb_sgd, 0)  as ssb_sgd,
        coalesce(cpf_oa_sgd, 0)  as cpf_oa_sgd,
        coalesce(cpf_sa_sgd, 0)  as cpf_sa_sgd,
        coalesce(cpf_ma_sgd, 0)  as cpf_ma_sgd,
        round(
            coalesce(cpf_oa_sgd, 0) + coalesce(cpf_sa_sgd, 0) + coalesce(cpf_ma_sgd, 0),
            2
        ) as total_cpf_sgd,
        round(
            stocks_sgd + crypto_sgd + coalesce(cash_sgd, 0) + coalesce(ssb_sgd, 0),
            2
        ) as total_net_worth_excl_cpf_sgd,
        round(
            stocks_sgd + crypto_sgd
            + coalesce(cash_sgd,   0)
            + coalesce(ssb_sgd,    0)
            + coalesce(cpf_oa_sgd, 0)
            + coalesce(cpf_sa_sgd, 0)
            + coalesce(cpf_ma_sgd, 0),
            2
        ) as total_net_worth_sgd,
        round(
            stocks_sgd + crypto_sgd
            + coalesce(cash_sgd,   0)
            + coalesce(ssb_sgd,    0)
            + coalesce(cpf_oa_sgd, 0)
            + coalesce(cpf_sa_sgd, 0)
            + coalesce(cpf_ma_sgd, 0)
            - lag(
                stocks_sgd + crypto_sgd
                + coalesce(cash_sgd,   0)
                + coalesce(ssb_sgd,    0)
                + coalesce(cpf_oa_sgd, 0)
                + coalesce(cpf_sa_sgd, 0)
                + coalesce(cpf_ma_sgd, 0)
            ) over (order by date),
            2
        ) as day_over_day_change_sgd
    from with_carried_forward
)

select * from final
order by date
