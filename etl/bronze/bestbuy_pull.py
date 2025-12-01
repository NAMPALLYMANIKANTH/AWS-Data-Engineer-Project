import boto3
import json
import requests
import datetime as dt
from typing import List, Dict, Tuple, Optional

from etl.utils.secret_manager import get_bestbuy_api_key
from etl.utils.sns_alerts import notify_etl_failure, notify_etl_success

BRONZE_BUCKET = "store-ops-dev-bronze"   # your bucket name
BRONZE_PREFIX = "bestbuy/catalog"        # logical folder under bucket
AWS_REGION = "us-east-2"                 # region of your S3 bucket
PAGE_SIZE = 100                          # BestBuy page size
MAX_PAGES = 50


def fetch_products_page(api_key: str, page: int, page_size: int = PAGE_SIZE) -> Dict:
    """
    Fetch a single page of products from BestBuy API.
    Returns the raw JSON dict.
    """
    url = "https://api.bestbuy.com/v1/products"

    params = {
        "format": "json",
        "show": "sku,name,regularPrice,salePrice,categoryPath.name",
        "pageSize": page_size,
        "page": page,
        "apiKey": api_key,
    }

    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def fetch_all_products(api_key: str, max_pages: int = MAX_PAGES) -> List[Dict]:
    """
    Fetch multiple pages from BestBuy API.
    Stops when:
      - no products in a page
      - HTTP error (e.g. Over Quota)
      - we've hit max_pages
    """
    products: List[Dict] = []

    for page in range(1, max_pages + 1):
        print(f"Fetching page {page}...")
        try:
            data = fetch_products_page(api_key, page=page)
        except requests.exceptions.HTTPError as e:
            print(f"  HTTP error on page {page}: {e}")
            print("  Stopping further page fetches, using collected products.")
            break

        page_products = data.get("products", [])
        print(f"  page {page}: {len(page_products)} products")

        if not page_products:
            print("  No products returned, stopping.")
            break

        products.extend(page_products)

        total_pages = data.get("totalPages")
        if total_pages is not None and page >= total_pages:
            print("  Reached totalPages from API, stopping.")
            break

    print(f"Total products collected: {len(products)}")
    return products


def build_s3_key(prefix: str) -> str:
    """
    Build an S3 key with date partition, e.g.
    bestbuy/catalog/dt=2025-11-15/bestbuy_products_2025-11-15.jsonl
    """
    today = dt.date.today().strftime("%Y-%m-%d")
    key = f"{prefix}/dt={today}/bestbuy_products_{today}.jsonl"
    return key


def upload_to_s3_jsonl(records: List[Dict], bucket: str, prefix: str) -> str:
    """
    Writes the records as a JSONL (NDJSON) file to S3 and returns the S3 key.
    """
    if not records:
        raise ValueError("No records to upload to S3")

    s3 = boto3.client("s3", region_name=AWS_REGION)

    key = build_s3_key(prefix)
    body = "\n".join(json.dumps(r) for r in records)

    print(f"Uploading {len(records)} records to s3://{bucket}/{key}")
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/x-ndjson",
    )

    return key


def main() -> Tuple[int, Optional[str]]:
    """
    Core Bronze BestBuy ingestion logic.
    Returns:
        (total_products, s3_key or None if nothing uploaded)
    """
    print("Getting BestBuy API key from Secrets Manager...")
    api_key = get_bestbuy_api_key()
    print("API key retrieved ✅")

    print("Fetching products from BestBuy API...")
    products = fetch_all_products(api_key=api_key)

    if not products:
        print("No products returned, exiting.")
        return 0, None

    print("Uploading data to S3 Bronze...")
    key = upload_to_s3_jsonl(products, BRONZE_BUCKET, BRONZE_PREFIX)
    print(f"Upload complete ✅ s3://{BRONZE_BUCKET}/{key}")

    return len(products), key


def run_bronze_bestbuy_ingest() -> None:
    """
    Wrapper used by Airflow (and CLI) that:
      - runs the Bronze ingest
      - sends SNS notifications for success/failure
    """
    job_name = "bronze_bestbuy_ingest"

    try:
        total, s3_key = main()

        if total == 0:
            details = "Pipeline ran successfully but BestBuy API returned 0 products."
        else:
            today = dt.date.today().strftime("%Y-%m-%d")
            details = (
                f"Bronze BestBuy ingest for {today} completed.\n"
                f"Fetched {total} products and uploaded to "
                f"s3://{BRONZE_BUCKET}/{s3_key}"
            )

        notify_etl_success(job_name, details)

    except Exception as e:
        notify_etl_failure(job_name, e)
        raise


if __name__ == "__main__":
    # CLI entrypoint → same behavior as Airflow
    run_bronze_bestbuy_ingest()
