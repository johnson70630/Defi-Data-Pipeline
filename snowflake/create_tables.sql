USE WAREHOUSE DEFI_WH;
USE DATABASE DEFI_DB;
USE SCHEMA RAW;

CREATE OR REPLACE TABLE RAW_UNISWAP_SWAPS (
    swap_id STRING,
    timestamp NUMBER,
    sender STRING,
    recipient STRING,
    amount0 FLOAT,
    amount1 FLOAT,
    amount_usd FLOAT,
    transaction_id STRING,
    block_number NUMBER,
    pool_id STRING,
    token0_id STRING,
    token0_symbol STRING,
    token0_name STRING,
    token1_id STRING,
    token1_symbol STRING,
    token1_name STRING,
    event_time TIMESTAMP_NTZ,
    event_date DATE
);