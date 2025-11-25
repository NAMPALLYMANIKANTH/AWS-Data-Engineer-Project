from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
import sys

# ---- CONFIG ----
BRONZE_PATH = "s3://store-ops-dev-bronze/bestbuy/catalog/"
ICEBERG_WAREHOUSE = "s3://store-ops-dev-silver/iceberg/"
ICEBERG_TABLE = "glue_catalog.store_ops.silver_products_ic"
APP_NAME = "CurateBestBuyCatalog"
# -----------------


def curate_catalog(spark):
    # 1) Read ALL Bronze partitions (dt=YYYY-MM-DD/)
    df_raw = spark.read.json(f"{BRONZE_PATH}*/*.jsonl")

    # 2) Extract dt from path + simple category flatten
    df = (
        df_raw
        .withColumn(
            "dt_str",
            F.regexp_extract(F.input_file_name(), r"dt=([0-9-]+)", 1)
        )
        .withColumn("dt", F.to_date("dt_str"))
        .drop("dt_str")
        # take 2nd level of categoryPath as category (if exists)
        .withColumn("category", F.col("categoryPath").getItem(1)["name"])
    )

    # 3) Curate final Silver DataFrame (matches Silver DDL)
    df_silver = (
        df.select(
            F.col("sku").cast("bigint").alias("sku"),
            F.col("name").alias("name"),
            F.col("regularPrice").cast("double").alias("regular_price"),
            F.col("salePrice").cast("double").alias("sale_price"),
            F.col("category"),
            F.col("dt")
        )
        # fields not present in Bronze -> NULL placeholders
        .withColumn("brand", F.lit(None).cast("string"))
        .withColumn("review_average", F.lit(None).cast("double"))
        .withColumn("online_availability", F.lit(None).cast("boolean"))

        .withColumn("on_sale", F.col("sale_price") < F.col("regular_price"))
        .withColumn(
            "discount_pct",
            F.when(
                F.col("regular_price") > 0,
                (F.col("regular_price") - F.col("sale_price")) / F.col("regular_price") * 100.0
            ).otherwise(F.lit(0.0))
        )
        .withColumn("ingested_at", F.current_timestamp())
    )

    return df_silver


def main():
    args = getResolvedOptions(sys.argv, ["JOB_NAME"])

    # Create Spark / GlueContext ONCE here
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

    # Create table if not exists (once)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {ICEBERG_TABLE} (
            sku BIGINT,
            name STRING,
            brand STRING,
            regular_price DOUBLE,
            sale_price DOUBLE,
            on_sale BOOLEAN,
            category STRING,
            review_average DOUBLE,
            online_availability BOOLEAN,
            discount_pct DOUBLE,
            ingested_at TIMESTAMP,
            dt DATE
        )
        USING iceberg
        PARTITIONED BY (dt)
    """)

    df_silver = curate_catalog(spark)
    print(f"Rows curated: {df_silver.count()}")

    # Write into Iceberg table (partition-aware)
    (
        df_silver
        .writeTo(ICEBERG_TABLE)
        .overwritePartitions()
    )

    print(f"✅ Silver written to Iceberg table {ICEBERG_TABLE}")

    job.commit()


if __name__ == "__main__":
    main()
