import logging
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

PROCESSED_FILE = Path("data/processed/uniswap/uniswap_swaps_processed.csv")

POSTGRES_URL = (
    "postgresql+psycopg2://defi_user:defi_password@defi-postgres:5432/defi_db"
)


def main() -> None:
    if not PROCESSED_FILE.exists():
        raise FileNotFoundError(f"File not found: {PROCESSED_FILE}")

    logging.info("Reading processed file: %s", PROCESSED_FILE)
    df = pd.read_csv(PROCESSED_FILE)

    engine = create_engine(POSTGRES_URL)

    logging.info("Loading %s rows into PostgreSQL", len(df))

    df.to_sql(
        name="uniswap_swaps",
        con=engine,
        if_exists="replace",
        index=False,
    )

    logging.info("Loaded data into table: uniswap_swaps")


if __name__ == "__main__":
    main()