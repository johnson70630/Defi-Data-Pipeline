{{ config(materialized='table') }}

SELECT
    event_date,
    COUNT(*) AS swap_count,
    COUNT(DISTINCT sender) AS active_senders,
    SUM(amount_usd) AS total_volume_usd,
    AVG(amount_usd) AS avg_swap_usd,
    MAX(amount_usd) AS max_swap_usd

FROM {{ ref('stg_uniswap_swaps') }}

GROUP BY event_date