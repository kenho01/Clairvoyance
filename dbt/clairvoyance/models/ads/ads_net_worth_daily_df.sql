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

bank_balances_ranged as (
    select
        date,
        cash_sgd,
        lead(date, 1, date '9999-12-31') over (order by date) as next_date
    from bank_balances
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
    select
        date_trunc(statement_date, month)  as date,
        round(ordinary_account, 2) as cpf_oa_sgd,
        round(special_account, 2) as cpf_sa_sgd,
        round(medisave_account, 2) as cpf_ma_sgd
    from cpf_latest
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
        round(sum(monthly_sgd) over (order by month rows between unbounded preceding and current row), 2) as endowus_principal_sgd
    from endowus_monthly
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
        e.endowus_principal_sgd
    from investment_daily i
    left join bank_balances_ranged b
        on date_trunc(i.date, month) >= b.date
        and date_trunc(i.date, month) < b.next_date
    left join cpf_balances c on c.date = date_trunc(i.date, month)
    left join endowus_cumulative e on e.date = date_trunc(i.date, month)
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
    from joined
),

final as (
    select
        date,
        stocks_sgd,
        crypto_sgd,
        round(stocks_sgd + crypto_sgd, 2) as investment_sgd,
        coalesce(cash_sgd, 0)  as cash_sgd,
        coalesce(endowus_principal_sgd, 0)  as endowus_principal_sgd,
        coalesce(cpf_oa_sgd, 0)  as cpf_oa_sgd,
        coalesce(cpf_sa_sgd, 0)  as cpf_sa_sgd,
        coalesce(cpf_ma_sgd, 0)  as cpf_ma_sgd,
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
            + coalesce(cash_sgd,                0)
            + coalesce(endowus_principal_sgd,   0)
            + coalesce(cpf_oa_sgd,              0)
            + coalesce(cpf_sa_sgd,              0)
            + coalesce(cpf_ma_sgd,              0),
            2
        ) as total_net_worth_sgd,
        round(
            stocks_sgd + crypto_sgd
            + coalesce(cash_sgd,                0)
            + coalesce(endowus_principal_sgd,   0)
            + coalesce(cpf_oa_sgd,              0)
            + coalesce(cpf_sa_sgd,              0)
            + coalesce(cpf_ma_sgd,              0)
            - lag(
                stocks_sgd + crypto_sgd
                + coalesce(cash_sgd,                0)
                + coalesce(endowus_principal_sgd,   0)
                + coalesce(cpf_oa_sgd,              0)
                + coalesce(cpf_sa_sgd,              0)
                + coalesce(cpf_ma_sgd,              0)
            ) over (order by date),
            2
        ) as day_over_day_change_sgd
    from with_carried_forward
)

select * from final
order by date
