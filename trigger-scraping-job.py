import json
import boto3
import uuid
from datetime import datetime

sqs = boto3.client('sqs')

QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/415848059244/scraping-job-queue"

def lambda_handler(event, context):

    job_id = str(uuid.uuid4())

    message = {
        "jobId": job_id,
        "source": "FACEBOOK_PAGE",
        "requestedAt": datetime.utcnow().isoformat()
    }

    print("Sending message:", message)

    response = sqs.send_message(
        QueueUrl=QUEUE_URL,
        MessageBody=json.dumps(message)
    )

    print("SQS response:", response)

    return {
        "statusCode": 202,
        "body": json.dumps({
            "jobId": job_id,
            "status": "RUNNING"
        })
    }