with raw as (
    select * from `{{ env_var('GCP_PROJECT_ID') }}.ods.ods_investment_positions_df`
),

deduped as (
    select *,
        row_number() over (
            partition by date, source, symbol
            order by etl_time desc
        ) as rn
    from raw
),

cleaned as (
    select
        cast(etl_time as timestamp)  as etl_time,
        cast(date as date) as date,
        source,
        symbol,
        asset_class,
        quantity,
        price,
        market_value,
        currency,
        fx_rate_to_sgd,
        market_value_sgd
    from deduped
    where rn = 1
)

select * from cleaned
