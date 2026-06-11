import logging
import os
from pathlib import Path


import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

PROCESSED_FILE = Path("data/processed/uniswap/uniswap_swaps_processed.csv")

POSTGRES_URL = os.getenv(
    "POSTGRES_URL",
    "postgresql+psycopg2://defi_user:defi_password@defi-postgres:5432/defi_db",
)

TARGET_TABLE = "uniswap_swaps"
STAGING_TABLE = "stg_uniswap_swaps"


def main() -> None:
    if not PROCESSED_FILE.exists():
        raise FileNotFoundError(f"File not found: {PROCESSED_FILE}")

    logging.info("Reading processed file: %s", PROCESSED_FILE)
    df = pd.read_csv(PROCESSED_FILE)

    engine = create_engine(POSTGRES_URL)

    logging.info("Loading %s rows into staging table: %s", len(df), STAGING_TABLE)

    df.to_sql(
        name=STAGING_TABLE,
        con=engine,
        if_exists="replace",
        index=False,
    )

    with engine.begin() as conn:
        logging.info("Creating target table if not exists.")

        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {TARGET_TABLE} AS
            SELECT *
            FROM {STAGING_TABLE}
            WHERE 1 = 0;
        """))

        conn.execute(text(f"""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_{TARGET_TABLE}_swap_id
            ON {TARGET_TABLE} (swap_id);
        """))

        logging.info("Upserting staging data into target table.")

        result = conn.execute(text(f"""
            INSERT INTO {TARGET_TABLE}
            SELECT *
            FROM {STAGING_TABLE}
            ON CONFLICT (swap_id) DO NOTHING;
        """))

        logging.info("Inserted %s new rows.", result.rowcount)

    logging.info("Incremental load completed.")


if __name__ == "__main__":
    main()