import json
import boto3
import os
from boto3.dynamodb.conditions import Attr

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(os.environ["POST_TABLE"])

def lambda_handler(event, context):

    trace_id = context.aws_request_id

    query_params = event.get("queryStringParameters") or {}

    limit = query_params.get("limit")

    if limit is not None:
        limit = int(limit)

        if limit > 100:
            return {
                "statusCode": 400,
                "body": json.dumps({
                    "traceId": trace_id,
                    "message": "limit must be <= 100"
                })
            }

    from_date = query_params.get("fromDate")
    to_date = query_params.get("toDate")

    filter_expression = None

    if from_date and to_date:
        filter_expression = Attr("publishedAt").between(from_date, to_date)

    elif from_date:
        filter_expression = Attr("publishedAt").gte(from_date)

    elif to_date:
        filter_expression = Attr("publishedAt").lte(to_date)

    scan_kwargs = {}

    if limit is not None:
        scan_kwargs["Limit"] = limit

    if filter_expression:
        scan_kwargs["FilterExpression"] = filter_expression

    response = table.scan(**scan_kwargs)

    raw_items = response.get("Items", [])

    items = []

    for item in raw_items:
        items.append({
            "incident_id": item.get("incident_id"),
            "incident_type": item.get("incident_type"),
            "severity": item.get("severity"),
            "location_id": item.get("location_id"),
            "status": item.get("status"),
            "incident_start": item.get("incident_start"),
            "occured_time": item.get("occured_time"),
            "ended_time": item.get("ended_time"),
            "description": item.get("description"),
            "reporter_id": item.get("reporter_id"),
            "created_at": item.get("created_at"),
            "update_id": item.get("update_id")
        })

    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json"
        },
        "body": json.dumps({
            "traceId": trace_id,
            "data": items
        }, ensure_ascii=False)
    }