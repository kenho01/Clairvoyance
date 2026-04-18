with raw as (
    select * from `{{ env_var('GCP_PROJECT_ID') }}.ods.ods_investment_positions_df`
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
    from raw
)

select * from cleaned
