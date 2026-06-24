import json
import logging
import os
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import requests
import snowflake.connector
from cryptography.hazmat.primitives import serialization
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

API_KEY = os.getenv("GRAPH_API_KEY", "").strip()

SUBGRAPH_ID = "5zvR82QoaXYFyDEKLZ9t6v9adgnptxYpKpSbxtgVENFV"
GRAPH_URL = f"https://gateway.thegraph.com/api/{API_KEY}/subgraphs/id/{SUBGRAPH_ID}"

OUTPUT_DIR = Path("data/raw/uniswap")
CHECKPOINT_PATH = OUTPUT_DIR / "extract_checkpoint.json"

BATCH_SIZE = 1000
MAX_RECORDS: int | None = None
REQUEST_SLEEP_SECONDS = 0.2
BOOTSTRAP_LOOKBACK_DAYS = int(os.getenv("BOOTSTRAP_LOOKBACK_DAYS", "30"))
OVERLAP_SECONDS = 600
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT", "JC01541.us-east-2.aws")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER", "DBT_USER")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE", "DBT_ROLE")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "DEFI_WH")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "DEFI_DB")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "RAW")
SNOWFLAKE_RAW_TABLE = os.getenv("SNOWFLAKE_RAW_TABLE", "RAW_UNISWAP_SWAPS")
SNOWFLAKE_PRIVATE_KEY_PATH = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")
SNOWFLAKE_KEY_PASSPHRASE = os.getenv("SNOWFLAKE_KEY_PASSPHRASE")




# --- Snowflake checkpointing ---

def get_bootstrap_timestamp() -> int:
    bootstrap_start = datetime.now(UTC) - timedelta(days=BOOTSTRAP_LOOKBACK_DAYS)
    return int(bootstrap_start.timestamp())

def read_local_checkpoint() -> int | None:
    if not CHECKPOINT_PATH.exists():
        return None

    try:
        with CHECKPOINT_PATH.open("r", encoding="utf-8") as file:
            checkpoint = json.load(file)

        latest_timestamp = checkpoint.get("latest_timestamp")

        if latest_timestamp is None:
            return None

        return int(latest_timestamp)

    except Exception as exc:
        logging.warning("Could not read local extraction checkpoint: %s", exc)
        return None


def write_local_checkpoint(latest_timestamp: int, records_fetched: int) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    checkpoint = {
        "latest_timestamp": latest_timestamp,
        "latest_time_utc": datetime.fromtimestamp(latest_timestamp, UTC).isoformat(),
        "records_fetched": records_fetched,
        "updated_at": datetime.now(UTC).isoformat(),
    }

    with CHECKPOINT_PATH.open("w", encoding="utf-8") as file:
        json.dump(checkpoint, file, indent=2)


def load_private_key() -> bytes:
    if not SNOWFLAKE_PRIVATE_KEY_PATH:
        raise ValueError("SNOWFLAKE_PRIVATE_KEY_PATH is missing.")

    if not SNOWFLAKE_KEY_PASSPHRASE:
        raise ValueError("SNOWFLAKE_KEY_PASSPHRASE is missing.")

    with open(SNOWFLAKE_PRIVATE_KEY_PATH, "rb") as key_file:
        private_key = serialization.load_pem_private_key(
            key_file.read(),
            password=SNOWFLAKE_KEY_PASSPHRASE.encode(),
        )

    return private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def get_latest_timestamp() -> int | None:
    try:
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
            with conn.cursor() as cur:
                cur.execute(f"SELECT MAX(timestamp) FROM {SNOWFLAKE_RAW_TABLE};")
                result = cur.fetchone()[0]

                if result is None:
                    bootstrap_timestamp = get_bootstrap_timestamp()
                    logging.info(
                        "Snowflake RAW is empty. Bootstrapping from the last %s days with timestamp >= %s.",
                        BOOTSTRAP_LOOKBACK_DAYS,
                        bootstrap_timestamp,
                    )
                    return bootstrap_timestamp

                latest_timestamp = int(result)
                checkpoint_timestamp = max(latest_timestamp - OVERLAP_SECONDS, 0)

                logging.info(
                    "Latest Snowflake timestamp=%s. Using overlapped checkpoint=%s.",
                    latest_timestamp,
                    checkpoint_timestamp,
                )

                return checkpoint_timestamp

        finally:
            conn.close()

    except Exception as exc:
        bootstrap_timestamp = get_bootstrap_timestamp()
        logging.warning(
            "Could not read latest timestamp from Snowflake. Bootstrapping from the last %s days with timestamp >= %s. Error: %s",
            BOOTSTRAP_LOOKBACK_DAYS,
            bootstrap_timestamp,
            exc,
        )
        return bootstrap_timestamp


def build_query(first: int, skip: int, latest_timestamp: int | None) -> str:
    where_clause = ""
    order_direction = "asc"

    if latest_timestamp is not None:
        where_clause = f"where: {{ timestamp_gte: {latest_timestamp} }},"

    return f"""
    {{
      swaps(
        first: {first},
        skip: {skip},
        {where_clause}
        orderBy: timestamp,
        orderDirection: {order_direction}
      ) {{
        id
        timestamp
        sender
        recipient
        amount0
        amount1
        amountUSD
        transaction {{
          id
          blockNumber
        }}
        pool {{
          id
          token0 {{
            id
            symbol
            name
          }}
          token1 {{
            id
            symbol
            name
          }}
        }}
      }}
    }}
    """


def fetch_swaps() -> list[dict[str, Any]]:
    if not API_KEY:
        raise ValueError("GRAPH_API_KEY is missing. Add it to your .env file.")

    snowflake_timestamp = get_latest_timestamp()
    local_checkpoint_timestamp = read_local_checkpoint()

    if snowflake_timestamp is not None and local_checkpoint_timestamp is not None:
        latest_timestamp = max(snowflake_timestamp, local_checkpoint_timestamp - OVERLAP_SECONDS)
    elif snowflake_timestamp is not None:
        latest_timestamp = snowflake_timestamp
    elif local_checkpoint_timestamp is not None:
        latest_timestamp = max(local_checkpoint_timestamp - OVERLAP_SECONDS, 0)
    else:
        latest_timestamp = None

    if latest_timestamp:
        logging.info("Running extraction from timestamp >= %s", latest_timestamp)
    else:
        logging.info("Running extraction without timestamp checkpoint.")

    all_swaps: list[dict[str, Any]] = []
    skip = 0

    extracted_at = datetime.now(UTC)
    partial_output_path = OUTPUT_DIR / f"uniswap_swaps_{extracted_at.strftime('%Y%m%d_%H%M%S')}_partial.json"

    while MAX_RECORDS is None or len(all_swaps) < MAX_RECORDS:
        query = build_query(
            first=BATCH_SIZE,
            skip=skip,
            latest_timestamp=latest_timestamp,
        )

        logging.info("Fetching swaps: skip=%s, batch_size=%s", skip, BATCH_SIZE)

        response = requests.post(
            GRAPH_URL,
            json={"query": query},
            timeout=60,
        )

        response.raise_for_status()
        payload = response.json()

        if "errors" in payload:
            raise RuntimeError(f"GraphQL error: {payload['errors']}")

        swaps = payload.get("data", {}).get("swaps", [])

        if not swaps:
            logging.info("No more swaps returned.")
            break

        all_swaps.extend(swaps)
        logging.info("Total swaps fetched: %s", len(all_swaps))

        latest_batch_timestamp = int(swaps[-1]["timestamp"])
        latest_batch_time = datetime.fromtimestamp(latest_batch_timestamp, UTC)
        logging.info(
            "Latest fetched swap timestamp=%s (%s)",
            latest_batch_timestamp,
            latest_batch_time.isoformat(),
        )

        save_raw_data(all_swaps, output_path=partial_output_path)
        write_local_checkpoint(latest_batch_timestamp, len(all_swaps))
        logging.info("Saved partial extraction to %s", partial_output_path)
        logging.info("Updated local checkpoint at %s", CHECKPOINT_PATH)

        if MAX_RECORDS is not None and len(all_swaps) >= MAX_RECORDS:
            logging.info("Reached MAX_RECORDS limit: %s", MAX_RECORDS)
            break

        skip += BATCH_SIZE
        time.sleep(REQUEST_SLEEP_SECONDS)

    if MAX_RECORDS is not None:
        return all_swaps[:MAX_RECORDS]

    return all_swaps


def save_raw_data(swaps: list[dict[str, Any]], output_path: Path | None = None) -> Path:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    extracted_at = datetime.now(UTC)

    if output_path is None:
        file_name = f"uniswap_swaps_{extracted_at.strftime('%Y%m%d_%H%M%S')}.json"
        output_path = OUTPUT_DIR / file_name

    output = {
        "source": "the_graph_uniswap_v3",
        "subgraph_id": SUBGRAPH_ID,
        "extracted_at": extracted_at.isoformat(),
        "record_count": len(swaps),
        "checkpoint_strategy": "bootstrap_last_n_days_then_snowflake_max_timestamp_with_overlap",
        "bootstrap_lookback_days": BOOTSTRAP_LOOKBACK_DAYS,
        "overlap_seconds": OVERLAP_SECONDS,
        "data": {
            "swaps": swaps,
        },
    }

    with output_path.open("w", encoding="utf-8") as file:
        json.dump(output, file, indent=2)

    return output_path


def main() -> None:
    logging.info("Starting Uniswap swap extraction.")

    swaps = fetch_swaps()
    output_path = save_raw_data(swaps)

    if swaps:
        latest_timestamp = int(swaps[-1]["timestamp"])
        write_local_checkpoint(latest_timestamp, len(swaps))

    logging.info("Extraction completed.")
    logging.info("Saved %s records to %s", len(swaps), output_path)


if __name__ == "__main__":
    main()