# etl/utils/sns_alerts.py

import boto3
import traceback
from typing import Optional

SNS_TOPIC_ARN = "arn:aws:sns:us-east-2:604743481383:bestbuy-data-alerts"
AWS_REGION = "us-east-2"


def publish_alert(subject: str, message: str) -> None:
    print(f"[DEBUG] Publishing to SNS topic: {SNS_TOPIC_ARN}")  

    if not SNS_TOPIC_ARN:
        print("SNS_TOPIC_ARN not set, skipping alert.")
        return

    subject = subject[:100]  

    sns = boto3.client("sns", region_name=AWS_REGION)
    sns.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject=subject,
        Message=message,
    )


def notify_etl_failure(job_name: str, error: Exception) -> None:
    subject = f"🚨 ETL FAILED: {job_name}"
    message = f"""
ETL job failed.

Job: {job_name}
Error: {str(error)}

Traceback:
{traceback.format_exc()}
"""
    publish_alert(subject, message)


def notify_etl_success(job_name: str, details: Optional[str] = None) -> None:
    subject = f"✅ ETL SUCCESS: {job_name}"
    message = f"""
ETL job completed successfully.

Job: {job_name}
"""
    if details:
        message += f"\nDetails:\n{details}\n"

    publish_alert(subject, message)
