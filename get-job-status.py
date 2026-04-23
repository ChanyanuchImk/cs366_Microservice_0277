import json
import boto3
import os

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(os.environ["JOB_TABLE"])

def lambda_handler(event, context):

    trace_id = context.aws_request_id

    job_id = event["pathParameters"]["jobId"]

    response = table.get_item(
        Key={"jobId": job_id}
    )

    if "Item" not in response:
        return {
            "statusCode": 404,
            "body": json.dumps({
                "traceId": trace_id,
                "message": "Job not found"
            })
        }

    item = response["Item"]

    return {
        "statusCode": 200,
        "body": json.dumps({
            "traceId": trace_id,
            "jobId": item["jobId"],
            "status": item["status"],
            "requestedAt": item.get("requestedAt"),
            "completedAt": item.get("completedAt"),
            "processedCount": item.get("processedCount", 0),
            "duplicateCount": item.get("duplicateCount", 0),
            "errorMessage": item.get("errorMessage")
        }, ensure_ascii=False)
    }