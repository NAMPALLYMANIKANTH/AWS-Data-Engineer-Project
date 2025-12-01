import boto3
import os
from etl.utils.sns_alerts import notify_etl_success, notify_etl_failure

GLUE_JOB_NAME = os.getenv("MARTS_GLUE_JOB_NAME", "marts_store_ops")
AWS_REGION = os.getenv("AWS_REGION", "us-east-2")

def main():
    job_name = "trigger_marts_glue"
    try:
        glue = boto3.client("glue", region_name=AWS_REGION)
        print(f"Starting Glue job: {GLUE_JOB_NAME}")

        response = glue.start_job_run(JobName=GLUE_JOB_NAME)
        job_run_id = response["JobRunId"]
        print(f"Glue job started. JobRunId = {job_run_id}")

        notify_etl_success(job_name, f"Marts Glue job started.\nRunId: {job_run_id}")

    except Exception as e:
        notify_etl_failure(job_name, e)
        raise


if __name__ == "__main__":
    main()