import os
from pathlib import Path

import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from cryptography.hazmat.primitives import serialization
from dotenv import load_dotenv

load_dotenv()

CSV_PATH = Path("data/processed/uniswap/uniswap_swaps_processed.csv")

SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT", "JC01541.us-east-2.aws")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER", "DBT_USER")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE", "DBT_ROLE")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "DEFI_WH")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "DEFI_DB")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "RAW")

PRIVATE_KEY_PATH = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")
PRIVATE_KEY_PASSPHRASE = os.getenv("SNOWFLAKE_KEY_PASSPHRASE")


def load_private_key():
    if not PRIVATE_KEY_PATH:
        raise ValueError("SNOWFLAKE_PRIVATE_KEY_PATH is missing")

    if not PRIVATE_KEY_PASSPHRASE:
        raise ValueError("SNOWFLAKE_KEY_PASSPHRASE is missing")

    with open(PRIVATE_KEY_PATH, "rb") as key_file:
        private_key = serialization.load_pem_private_key(
            key_file.read(),
            password=PRIVATE_KEY_PASSPHRASE.encode(),
        )

    return private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def main():
    if not CSV_PATH.exists():
        raise FileNotFoundError(f"Processed CSV not found: {CSV_PATH}")

    df = pd.read_csv(CSV_PATH)
    df = df.reset_index(drop=True)

    conn = snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        private_key=load_private_key(),
        role=SNOWFLAKE_ROLE,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
    )

    try:
        cur = conn.cursor()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS RAW_UNISWAP_SWAPS (
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
        """)

        cur.execute("CREATE TEMP TABLE TEMP_UNISWAP_SWAPS LIKE RAW_UNISWAP_SWAPS;")

        success, nchunks, nrows, _ = write_pandas(
            conn,
            df,
            "TEMP_UNISWAP_SWAPS",
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA,
            quote_identifiers=False,
        )

        if not success:
            raise RuntimeError("write_pandas failed")

        print(f"Loaded {nrows} rows into Snowflake temp table")

        cur.execute("""
            MERGE INTO RAW_UNISWAP_SWAPS AS target
            USING TEMP_UNISWAP_SWAPS AS source
            ON target.swap_id = source.swap_id
            WHEN NOT MATCHED THEN INSERT (
                swap_id,
                timestamp,
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
                token1_name,
                event_time,
                event_date
            )
            VALUES (
                source.swap_id,
                source.timestamp,
                source.sender,
                source.recipient,
                source.amount0,
                source.amount1,
                source.amount_usd,
                source.transaction_id,
                source.block_number,
                source.pool_id,
                source.token0_id,
                source.token0_symbol,
                source.token0_name,
                source.token1_id,
                source.token1_symbol,
                source.token1_name,
                source.event_time,
                source.event_date
            );
        """)

        print("Snowflake RAW load completed")

    finally:
        conn.close()


if __name__ == "__main__":
    main()