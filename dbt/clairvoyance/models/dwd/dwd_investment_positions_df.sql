with raw as (
    select * from `{{ env_var('GCP_PROJECT_ID') }}.ods.ods_investment_positions_df`
),

cleaned as (
    select
        cast(ingested_at as timestamp)  as ingested_at,
        cast(snapshot_date as date)     as snapshot_date,
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
