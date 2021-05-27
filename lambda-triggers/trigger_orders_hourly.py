import json
import boto3 


def lambda_handler(event, context):
    job_name = "hourly_orders_import"

    client = boto3.client("glue")

    client.start_job_run(
        JobName=job_name,
        Arguments={}
    )

    return {
        "statusCode": 200,
        "body": json.dumps(f"{job_name} triggered")
    }
