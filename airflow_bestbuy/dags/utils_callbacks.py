from airflow.utils.log.logging_mixin import LoggingMixin

logger = LoggingMixin().log

# Try to use your SNS alert helper, but don't crash if it's missing
try:
    from etl.utils.sns_alerts import publish_alert
except Exception:
    publish_alert = None


def airflow_success_callback(context):
    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    msg = f"✅ DAG {dag_id} – task {task_id} succeeded"
    logger.info(msg)

    if publish_alert:
        publish_alert("SUCCESS", msg)


def airflow_failure_callback(context):
    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    exception = context.get("exception")
    msg = f"❌ DAG {dag_id} – task {task_id} failed: {exception}"
    logger.error(msg)

    if publish_alert:
        publish_alert("FAILURE", msg)
