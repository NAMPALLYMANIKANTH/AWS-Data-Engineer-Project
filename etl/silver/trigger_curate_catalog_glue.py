# etl/silver/trigger_curate_catalog_glue.py

import os
import boto3

from etl.utils.sns_alerts import notify_etl_failure, notify_etl_success

# Name of the Glue job as created in AWS Console
GLUE_JOB_NAME = os.getenv("CURATE_GLUE_JOB_NAME", "curate_catalog")
AWS_REGION = os.getenv("AWS_REGION", "us-east-2")


def main():
    """
    Trigger the AWS Glue job that runs curate_catalog ETL.
    This is called from:
      - local CLI (python -m etl.silver.trigger_curate_catalog_glue)
      - Airflow PythonOperator
    Sends SNS alerts on success/failure of the *trigger* itself.
    """
    job_name = "trigger_curate_catalog_glue"

    try:
        glue = boto3.client("glue", region_name=AWS_REGION)

        print(f"Starting Glue job: {GLUE_JOB_NAME}")
        response = glue.start_job_run(JobName=GLUE_JOB_NAME)

        job_run_id = response["JobRunId"]
        print(f"Glue job started. JobRunId = {job_run_id}")

        details = (
            f"Started Glue job '{GLUE_JOB_NAME}' in region {AWS_REGION}.\n"
            f"JobRunId: {job_run_id}"
        )
        notify_etl_success(job_name, details)

    except Exception as e:
        # Send failure alert + re-raise so Airflow / CI can detect failure
        notify_etl_failure(job_name, e)
        raise


if __name__ == "__main__":
    main()
