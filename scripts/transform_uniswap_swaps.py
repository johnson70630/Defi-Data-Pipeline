import json
import logging
from pathlib import Path

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

RAW_DIR = Path("data/raw/uniswap")
PROCESSED_DIR = Path("data/processed/uniswap")


def get_latest_raw_file() -> Path:
    files = sorted(RAW_DIR.glob("uniswap_swaps_*.json"))

    if not files:
        raise FileNotFoundError("No raw Uniswap swap files found.")

    return files[-1]


def transform_swaps(raw_file: Path) -> pd.DataFrame:
    with raw_file.open("r", encoding="utf-8") as file:
        payload = json.load(file)

    swaps = payload["data"]["swaps"]

    rows = []

    for swap in swaps:
        row = {
            "swap_id": swap["id"],
            "timestamp": int(swap["timestamp"]),
            "sender": swap["sender"],
            "recipient": swap["recipient"],
            "amount0": float(swap["amount0"]),
            "amount1": float(swap["amount1"]),
            "amount_usd": float(swap["amountUSD"]),
            "transaction_id": swap["transaction"]["id"],
            "block_number": int(swap["transaction"]["blockNumber"]),
            "pool_id": swap["pool"]["id"],
            "token0_id": swap["pool"]["token0"]["id"],
            "token0_symbol": swap["pool"]["token0"]["symbol"],
            "token0_name": swap["pool"]["token0"]["name"],
            "token1_id": swap["pool"]["token1"]["id"],
            "token1_symbol": swap["pool"]["token1"]["symbol"],
            "token1_name": swap["pool"]["token1"]["name"],
        }

        rows.append(row)

    df = pd.DataFrame(rows)

    df["event_time"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
    df["event_date"] = df["event_time"].dt.date

    return df


def save_processed_data(df: pd.DataFrame) -> Path:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    output_path = PROCESSED_DIR / "uniswap_swaps_processed.csv"

    df.to_csv(output_path, index=False)

    return output_path


def main() -> None:
    raw_file = get_latest_raw_file()

    logging.info("Transforming raw file: %s", raw_file)

    df = transform_swaps(raw_file)
    output_path = save_processed_data(df)

    logging.info("Processed %s rows", len(df))
    logging.info("Saved processed data to %s", output_path)


if __name__ == "__main__":
    main()