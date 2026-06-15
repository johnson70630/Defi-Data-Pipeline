{{ config(
    materialized='view'
) }}

SELECT
    swap_id,
    timestamp AS event_timestamp,
    event_time,
    event_date,

    sender,
    recipient,

    amount0,
    amount1,
    amount_usd,

    transaction_id,
    block_number,
    pool_id,

    token0_id,
    token0_symbol,
    token0_name,

    token1_id,
    token1_symbol,
    token1_name

FROM {{ source('raw', 'raw_uniswap_swaps') }}
WHERE swap_id IS NOT NULL