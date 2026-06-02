import json
import logging
import os
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import requests
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
BATCH_SIZE = 1000
MAX_RECORDS = 10_000
REQUEST_SLEEP_SECONDS = 0.2


def build_query(first: int, skip: int) -> str:
    return f"""
    {{
      swaps(
        first: {first},
        skip: {skip},
        orderBy: timestamp,
        orderDirection: desc
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

    all_swaps: list[dict[str, Any]] = []
    skip = 0

    while len(all_swaps) < MAX_RECORDS:
        query = build_query(first=BATCH_SIZE, skip=skip)

        logging.info("Fetching swaps: skip=%s, batch_size=%s", skip, BATCH_SIZE)

        response = requests.post(
            GRAPH_URL,
            json={"query": query},
            timeout=30,
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

        skip += BATCH_SIZE
        time.sleep(REQUEST_SLEEP_SECONDS)

    return all_swaps[:MAX_RECORDS]


def save_raw_data(swaps: list[dict[str, Any]]) -> Path:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    extracted_at = datetime.now(UTC)
    file_name = f"uniswap_swaps_{extracted_at.strftime('%Y%m%d_%H%M%S')}.json"
    output_path = OUTPUT_DIR / file_name

    output = {
        "source": "the_graph_uniswap_v3",
        "subgraph_id": SUBGRAPH_ID,
        "extracted_at": extracted_at.isoformat(),
        "record_count": len(swaps),
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

    logging.info("Extraction completed.")
    logging.info("Saved %s records to %s", len(swaps), output_path)


if __name__ == "__main__":
    main()