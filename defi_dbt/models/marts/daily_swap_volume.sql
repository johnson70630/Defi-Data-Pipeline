{{ config(
    materialized='incremental',
    unique_key='event_date'
) }}

WITH daily AS (
    SELECT
        event_date,
        COUNT(*) AS swap_count,
        COUNT(DISTINCT sender) AS active_senders,
        SUM(amount_usd) AS total_volume_usd,
        AVG(amount_usd) AS avg_swap_usd,
        MAX(amount_usd) AS max_swap_usd

    FROM {{ ref('stg_uniswap_swaps') }}

    {% if is_incremental() %}
        WHERE event_date >= (
            SELECT COALESCE(MAX(event_date), '1900-01-01')
            FROM {{ this }}
        )
    {% endif %}

    GROUP BY event_date
)

SELECT *
FROM daily