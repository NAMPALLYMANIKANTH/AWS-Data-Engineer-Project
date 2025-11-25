from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    LongType, IntegerType, StringType
)
import datetime as dt
import random
import sys

# ---- CONFIG ----
ICEBERG_WAREHOUSE = "s3://store-ops-dev-silver/iceberg/"
PRODUCTS_ICEBERG_TABLE = "glue_catalog.store_ops.silver_products_ic"
INVENTORY_ICEBERG_TABLE = "glue_catalog.store_ops.silver_inventory_ic"
NUM_STORES = 5        # number of stores
NUM_DAYS = 30         # days of history
APP_NAME = "SimulateInventoryIceberg"
# -----------------


def simulate_inventory(spark):
    # 1) Read SKUs from Iceberg silver_products_ic
    df_products = spark.read.table(PRODUCTS_ICEBERG_TABLE)

    sku_rows = (
        df_products
        .select("sku")
        .where(F.col("sku").isNotNull())
        .distinct()
        .collect()
    )
    sku_list = [row["sku"] for row in sku_rows]
    print(f"Found {len(sku_list)} unique SKUs in {PRODUCTS_ICEBERG_TABLE}")

    all_rows = []
    today = dt.date.today()

    # 2) Build rows (sku, store_id, stock_on_hand, stock_reserved, dt_str)
    for day_offset in range(NUM_DAYS):
        snapshot_date = today - dt.timedelta(days=day_offset)
        dt_str = snapshot_date.isoformat()

        for sku in sku_list:
            for store_id in range(1, NUM_STORES + 1):
                stock_on_hand = random.randint(5, 50)
                stock_reserved = random.randint(0, max(0, stock_on_hand // 3))

                all_rows.append(
                    (sku, store_id, stock_on_hand, stock_reserved, dt_str)
                )

    schema = StructType(
        [
            StructField("sku", LongType(), True),
            StructField("store_id", IntegerType(), True),
            StructField("stock_on_hand", IntegerType(), True),
            StructField("stock_reserved", IntegerType(), True),
            StructField("dt_str", StringType(), True),
        ]
    )

    df_inventory = spark.createDataFrame(all_rows, schema)

    df_inventory = (
        df_inventory
        .withColumn("dt", F.to_date("dt_str"))
        .drop("dt_str")
        .withColumn("ingested_at", F.current_timestamp())
    )

    return df_inventory


def main():
    args = getResolvedOptions(sys.argv, ["JOB_NAME"])

    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    spark.sparkContext.setLogLevel("WARN")

    # --- ICEBERG CONFIG ---
    spark.conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
    spark.conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", ICEBERG_WAREHOUSE)
    spark.conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    # ----------------------

    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)

    # Create Iceberg table if not exists
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {INVENTORY_ICEBERG_TABLE} (
            sku BIGINT,
            store_id INT,
            stock_on_hand INT,
            stock_reserved INT,
            ingested_at TIMESTAMP,
            dt DATE
        )
        USING iceberg
        PARTITIONED BY (dt)
    """)

    df_inventory = simulate_inventory(spark)
    print(f"Generated rows: {df_inventory.count()}")

    (
        df_inventory
        .writeTo(INVENTORY_ICEBERG_TABLE)
        .overwritePartitions()
    )

    print(f"✅ Inventory written to Iceberg table {INVENTORY_ICEBERG_TABLE}")

    job.commit()


if __name__ == "__main__":
    main()
