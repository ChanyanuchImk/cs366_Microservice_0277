import json
import boto3
import os

sns = boto3.client("sns")
topic_arn = os.environ["TOPIC_ARN"]


def build_dynamodb_payload(data):
    return {
        "incident_id": {
            "S": data.get("incident_id", "")
        },
        "created_at": {
            "S": data.get("created_at", "")
        },
        "description": {
            "S": data.get("description", "")
        },
        "ended_time": {
            "NULL": data.get("ended_time") is None
        },
        "incident_start": {
            "S": data.get("incident_start", "")
        },
        "incident_type": {
            "S": data.get("incident_type", "")
        },
        "location_id": {
            "L": [
                {"S": loc}
                for loc in data.get("location_id", [])
            ]
        },
        "occured_time": {
            "S": data.get("occured_time", "")
        },
        "reporter_id": {
            "S": data.get("reporter_id", "")
        },
        "severity": {
            "S": data.get("severity", "")
        },
        "source_category": {
            "S": data.get("source_category", "")
        },
        "status": {
            "S": data.get("status", "")
        },
        "update_id": {
            "NULL": data.get("update_id") is None
        }
    }


def lambda_handler(event, context):

    print("RAW EVENT:", json.dumps(event, ensure_ascii=False))

    for record in event["Records"]:
        try:
            # รับ message จาก SQS
            message = json.loads(record["body"])

            # ดึง body จริง
            data = message.get("body")

            if not data:
                print("No body found")
                continue

            # แปลงเป็น DynamoDB JSON format
            payload = build_dynamodb_payload(data)

            print("PAYLOAD:", json.dumps(payload, ensure_ascii=False))

            # ส่งเข้า SNS
            response = sns.publish(
                TopicArn=topic_arn,
                Subject="TMD Alert",
                Message=json.dumps(payload, ensure_ascii=False)
            )

            print("SNS SENT:", response)

        except Exception as e:
            print("ERROR:", str(e))

    return {
        "statusCode": 200,
        "body": json.dumps("SNS publish completed")
    }