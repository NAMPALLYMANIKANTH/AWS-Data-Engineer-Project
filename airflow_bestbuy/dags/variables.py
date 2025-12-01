from datetime import timedelta

DAG_ID = "bestbuy_ingest_to_gold_dag"

SCHEDULE = "@daily"

DEFAULT_ARGS = {
    "owner": "mani",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}
