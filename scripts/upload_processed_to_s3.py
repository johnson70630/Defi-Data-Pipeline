import logging
import os
from pathlib import Path

import boto3
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

PROCESSED_FILE = Path("data/processed/uniswap/uniswap_swaps_processed.csv")

S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
AWS_REGION = os.getenv("AWS_REGION", "us-west-2")


def upload_file_to_s3(file_path: Path) -> str:
    if not S3_BUCKET_NAME:
        raise ValueError("S3_BUCKET_NAME is missing from .env")

    if not file_path.exists():
        raise FileNotFoundError(f"Processed file not found: {file_path}")

    s3_client = boto3.client("s3", region_name=AWS_REGION)

    s3_key = f"processed/uniswap/{file_path.name}"

    logging.info("Uploading %s to s3://%s/%s", file_path, S3_BUCKET_NAME, s3_key)

    s3_client.upload_file(
        Filename=str(file_path),
        Bucket=S3_BUCKET_NAME,
        Key=s3_key,
    )

    return s3_key


def main() -> None:
    s3_key = upload_file_to_s3(PROCESSED_FILE)

    logging.info("Upload completed: s3://%s/%s", S3_BUCKET_NAME, s3_key)


if __name__ == "__main__":
    main()