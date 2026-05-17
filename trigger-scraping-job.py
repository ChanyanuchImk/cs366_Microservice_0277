import json
import boto3
import uuid
import os
from datetime import datetime

dynamodb = boto3.resource("dynamodb")
sqs = boto3.client("sqs")

job_table = dynamodb.Table(os.environ["JOB_TABLE"])
queue_url = os.environ["SCRAPING_QUEUE_URL"]


def lambda_handler(event, context):

    print("EVENT =", json.dumps(event))

    # Called from API Gateway
    if "body" in event:
        try:
            body = json.loads(event.get("body", "{}"))
        except Exception:
            body = {}

        source = body.get("source", "TMD Website")

    # Called from EventBridge or Lambda Test
    else:
        source = event.get("source", "TMD Website")

    print("SOURCE =", source)

    if source != "TMD Website":
        return {
            "statusCode": 400,
            "body": json.dumps({
                "message": "Invalid source"
            })
        }

    jobs = job_table.scan()

    running = [
        j for j in jobs.get("Items", [])
        if j.get("status") == "RUNNING"
    ]

    if running:
        return {
            "statusCode": 409,
            "body": json.dumps({
                "message": "Job already running"
            })
        }

    job_id = str(uuid.uuid4())
    now = datetime.utcnow().isoformat()

    job_table.put_item(
        Item={
            "jobId": job_id,
            "source": source,
            "status": "RUNNING",
            "requestedAt": now
        }
    )

    sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps({
            "jobId": job_id,
            "source": source
        })
    )

    return {
        "statusCode": 202,
        "body": json.dumps({
            "jobId": job_id,
            "status": "RUNNING"
        })
    }