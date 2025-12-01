from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

# ---- Your ETL callables ---------------------------------
# (these paths match your repo layout)
from etl.bronze.bestbuy_pull import run_bronze_bestbuy_ingest
from etl.silver.trigger_curate_catalog_glue import main as silver_catalog_main
from etl.gold.trigger_marts_glue import main as gold_main
from etl.snowflake_load.load_gold_to_snowflake import load_gold as load_gold_main

# Optional: if you want SNS callbacks and already have this file
try:
    from utils_callbacks import (
        airflow_failure_callback,
        airflow_success_callback,
    )
except ImportError:  # safe fallback if file not present / not needed
    airflow_failure_callback = None
    airflow_success_callback = None


# ---- DAG default args ------------------------------------
default_args = {
    "owner": "mani",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# ---- DAG definition --------------------------------------
with DAG(
    dag_id="bestbuy_ingest_to_gold_dag",
    default_args=default_args,
    start_date=datetime(2025, 11, 25),
    schedule_interval="@daily",
    catchup=False,
    tags=["analytics", "bestbuy", "store-ops"],
    on_failure_callback=airflow_failure_callback,
    on_success_callback=airflow_success_callback,
) as dag:

    # 1) BRONZE LAYER – pull from BestBuy API → S3 Bronze
    with TaskGroup(
        group_id="bronze_layer",
        tooltip="Ingest BestBuy products into S3 Bronze",
    ) as bronze_layer:
        bronze_bestbuy_ingest = PythonOperator(
            task_id="bronze_bestbuy_ingest",
            python_callable=run_bronze_bestbuy_ingest,
        )

    # 2) SILVER LAYER – Glue job to curate Bronze → Silver Iceberg
    with TaskGroup(
        group_id="silver_layer",
        tooltip="Curate Bronze JSON into Silver Iceberg tables",
    ) as silver_layer:
        silver_curate_products = PythonOperator(
            task_id="silver_curate_products",
            python_callable=silver_catalog_main,
        )

    # 3) GOLD LAYER – Glue job (or Python) to build KPIs in Iceberg
    with TaskGroup(
        group_id="gold_layer",
        tooltip="Build Gold KPIs in Iceberg (gold_kpis_ic)",
    ) as gold_layer:
        gold_build_kpis = PythonOperator(
            task_id="gold_build_kpis",
            python_callable=gold_main,
        )

    # 4) SNOWFLAKE LAYER – load Gold KPIs into Snowflake table
    with TaskGroup(
        group_id="snowflake_layer",
        tooltip="Load Gold KPIs from S3 Iceberg into Snowflake",
    ) as snowflake_layer:
        snowflake_load_gold_kpis_ic = PythonOperator(
            task_id="snowflake_load_gold_kpis_ic",
            python_callable=load_gold_main,
        )

    # Wire the layers together
    bronze_layer >> silver_layer >> gold_layer >> snowflake_layer
