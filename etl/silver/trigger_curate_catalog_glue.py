import os
import boto3
from airflow.exceptions import AirflowSkipException

from etl.utils.sns_alerts import notify_etl_failure, notify_etl_success

# Environment-driven configs
GLUE_JOB_NAME = os.getenv("CURATE_GLUE_JOB_NAME", "curate_catalog")
AWS_REGION = os.getenv("AWS_REGION", "us-east-2")


def main():
    """
    Trigger the Silver layer Glue job.  
    Adds proper handling for ConcurrentRunsExceededException to avoid failures
    when Glue is already running.
    """
    job_name = "trigger_curate_catalog_glue"
    glue = boto3.client("glue", region_name=AWS_REGION)

    try:
        print(f"Starting Glue job: {GLUE_JOB_NAME}")
        response = glue.start_job_run(JobName=GLUE_JOB_NAME)

        job_run_id = response["JobRunId"]
        print(f"Glue job started. JobRunId = {job_run_id}")

        notify_etl_success(
            job_name,
            f"Started Glue job '{GLUE_JOB_NAME}' in {AWS_REGION}\nRunId: {job_run_id}"
        )

    except glue.exceptions.ConcurrentRunsExceededException:
        # Glue job is already running — treat this as SUCCESS, not failure
        msg = (
            f"Glue job '{GLUE_JOB_NAME}' already has a running instance. "
            f"Skipping new run."
        )
        print(msg)
        notify_etl_success(job_name, msg)

        # Airflow shows task as SKIPPED (cleaner than FAILED)
        raise AirflowSkipException(msg)

    except Exception as e:
        # Generic failure
        notify_etl_failure(job_name, e)
        raise


if __name__ == "__main__":
    main()
