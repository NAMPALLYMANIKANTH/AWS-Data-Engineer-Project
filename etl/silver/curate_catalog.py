from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
import sys

# ---- CONFIG (edit if needed) ----
BRONZE_PATH = "s3://store-ops-dev-bronze/bestbuy/catalog/"
SILVER_PATH = "s3://store-ops-dev-silver/silver_products/"
APP_NAME = "CurateBestBuyCatalog"
# ---------------------------------


def curate_catalog(spark):
    # 1) Read raw Bronze JSON
    df_raw = (
        spark.read
        .json(f"{BRONZE_PATH}*/")   # dt=YYYY-MM-DD/*.jsonl
    )

    # 2) Flatten categoryPath
    df = (
        df_raw
        .withColumn("department", F.col("categoryPath").getItem(0)["name"])
        .withColumn("category",   F.col("categoryPath").getItem(1)["name"])
        .withColumn("subcategory", F.col("categoryPath").getItem(2)["name"])
    )

    # 3) Clean + enrich
    df_curated = (
        df
        .select(
            F.col("sku").cast("bigint").alias("sku"),
            F.col("name").alias("product_name"),
            F.col("regularPrice").cast("double").alias("regular_price"),
            F.col("salePrice").cast("double").alias("sale_price"),
            "department",
            "category",
            "subcategory",
        )
        .withColumn(
            "discount_pct",
            F.when(F.col("regular_price") > 0,
                   (F.col("regular_price") - F.col("sale_price")) / F.col("regular_price") * 100.0
            ).otherwise(F.lit(0.0))
        )
        .withColumn("ingestion_date", F.current_date())
    )

    return df_curated


def main():
    args = getResolvedOptions(sys.argv, ["JOB_NAME"])

    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    spark.sparkContext.setLogLevel("WARN")

    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)

    df_curated = curate_catalog(spark)
    print(f"Curated count: {df_curated.count()}")

    (
        df_curated
        .repartition("ingestion_date")
        .write
        .mode("overwrite")
        .format("parquet")
        .partitionBy("ingestion_date")
        .save(SILVER_PATH)
    )

    print(f"✅ Silver written to {SILVER_PATH}")

    job.commit()


if __name__ == "__main__":
    main()
