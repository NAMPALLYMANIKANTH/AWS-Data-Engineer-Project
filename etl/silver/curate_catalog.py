from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
import sys

# ---- CONFIG ----
BRONZE_PATH = "s3://store-ops-dev-bronze/bestbuy/catalog/"
SILVER_PATH = "s3://store-ops-dev-silver/silver_products/"   # Athena table LOCATION
APP_NAME = "CurateBestBuyCatalog"
# -----------------


def curate_catalog(spark):
    # 1) Read ALL Bronze partitions (dt=YYYY-MM-DD/)
    df_raw = spark.read.json(f"{BRONZE_PATH}*/*.jsonl")


    # 2) Extract dt from the Bronze path and flatten categoryPath (take LAST category)
    df = (
    df_raw
    .withColumn(
        "dt",
        F.to_date(
            F.regexp_extract(F.input_file_name(), r"dt=([0-9-]+)", 1)
        )
    )
    .withColumn(
        "category",
        F.expr("categoryPath[name IS NOT NULL][-1].name")
    )
)


    # 3) Curate final Silver DataFrame
    df_silver = (
        df.select(
            F.col("sku").cast("bigint"),
            F.col("name").alias("name"),
            F.col("brand").alias("brand"),
            F.col("regularPrice").cast("double").alias("regular_price"),
            F.col("salePrice").cast("double").alias("sale_price"),
            F.col("customerReviewAverage").cast("double").alias("review_average"),
            F.col("onlineAvailability").cast("boolean").alias("online_availability"),
            F.col("category"),
            F.col("dt")  # from Bronze folder name
        )
        .withColumn("on_sale", F.col("sale_price") < F.col("regular_price"))
        .withColumn(
            "discount_pct",
            F.when(
                F.col("regular_price") > 0,
                (F.col("regular_price") - F.col("sale_price")) / F.col("regular_price") * 100
            ).otherwise(F.lit(0.0))
        )
        .withColumn("ingested_at", F.current_timestamp())
    )

    return df_silver


def main():
    args = getResolvedOptions(sys.argv, ["JOB_NAME"])

    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    spark.sparkContext.setLogLevel("WARN")

    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)

    df_silver = curate_catalog(spark)
    print(f"Rows curated: {df_silver.count()}")

    (
        df_silver
        .repartition("dt")
        .write
        .mode("overwrite")
        .format("parquet")
        .partitionBy("dt")
        .save(SILVER_PATH)
    )

    print(f"✅ Silver written to: {SILVER_PATH}")

    job.commit()


if __name__ == "__main__":
    main()
