{{ config(materialized='table') }}

SELECT
    pool_id,
    token0_symbol,
    token1_symbol,
    COUNT(*) AS swap_count,
    SUM(amount_usd) AS total_volume_usd,
    AVG(amount_usd) AS avg_swap_usd,
    MAX(amount_usd) AS max_swap_usd

FROM {{ ref('stg_uniswap_swaps') }}

GROUP BY pool_id, token0_symbol, token1_symbol