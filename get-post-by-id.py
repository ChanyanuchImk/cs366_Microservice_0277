import json
import boto3
import os

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(os.environ["POST_TABLE"])

def lambda_handler(event, context):

    trace_id = context.aws_request_id

    path_parameters = event.get("pathParameters") or {}
    post_id = path_parameters.get("postId")

    if not post_id:
        return {
            "statusCode": 400,
            "headers": {
                "Content-Type": "application/json"
            },
            "body": json.dumps({
                "traceId": trace_id,
                "message": "postId is required"
            }, ensure_ascii=False)
        }

    response = table.get_item(
        Key={
            "postId": post_id
        }
    )

    item = response.get("Item")

    if not item:
        return {
            "statusCode": 404,
            "headers": {
                "Content-Type": "application/json"
            },
            "body": json.dumps({
                "traceId": trace_id,
                "message": "Post not found"
            }, ensure_ascii=False)
        }

    data = {
        "postId": item.get("postId"),
        "eventType": item.get("eventType"),
        "severity": item.get("severity"),
        "affectedArea": item.get("affectedArea"),
        "messageText": item.get("messageText"),
        "publishedAt": item.get("publishedAt"),
        "scrapedAt": item.get("scrapedAt"),
        "sourceUrl": item.get("sourceUrl"),
        "contentHash": item.get("contentHash")
    }

    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json"
        },
        "body": json.dumps({
            "traceId": trace_id,
            "data": data
        }, ensure_ascii=False)
    }