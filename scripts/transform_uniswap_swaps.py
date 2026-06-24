import json
import logging
from pathlib import Path
from typing import Any

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


def load_valid_swaps(raw_file: Path) -> list[dict[str, Any]]:
    try:
        with raw_file.open("r", encoding="utf-8") as file:
            payload = json.load(file)

        swaps = payload["data"]["swaps"]
        logging.info("Loaded %s swaps from %s", len(swaps), raw_file)
        return swaps

    except Exception as exc:
        logging.warning("Skipping invalid raw file %s: %s", raw_file, exc)
        return []


def swap_to_row(swap: dict[str, Any]) -> dict[str, Any]:
    return {
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


def transform_swaps(raw_files: list[Path]) -> pd.DataFrame:
    rows = []

    for raw_file in raw_files:
        swaps = load_valid_swaps(raw_file)

        for swap in swaps:
            try:
                rows.append(swap_to_row(swap))
            except Exception as exc:
                logging.warning("Skipping invalid swap in %s: %s", raw_file, exc)

    df = pd.DataFrame(rows)

    if df.empty:
        raise ValueError("No valid swaps found in raw files.")

    before_dedup = len(df)
    df = df.drop_duplicates(subset=["swap_id"], keep="last")
    after_dedup = len(df)

    logging.info("Deduplicated swaps: %s -> %s", before_dedup, after_dedup)

    df["event_time"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
    df["event_date"] = df["event_time"].dt.date

    df = df.sort_values("timestamp")

    return df


def save_processed_data(df: pd.DataFrame) -> Path:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    output_path = PROCESSED_DIR / "uniswap_swaps_processed.csv"
    df.to_csv(output_path, index=False)

    return output_path


def main() -> None:
    raw_file = get_latest_raw_file()

    logging.info("Transforming latest raw file: %s", raw_file)

    df = transform_swaps([raw_file])
    output_path = save_processed_data(df)

    logging.info("Processed %s deduplicated rows", len(df))
    logging.info("Saved processed data to %s", output_path)


if __name__ == "__main__":
    main()