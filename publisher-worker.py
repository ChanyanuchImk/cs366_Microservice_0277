import json
import boto3
import os

sns = boto3.client("sns")
topic_arn = os.environ["TOPIC_ARN"]

def format_message(data):
    return f"""
🚨 NEW INCIDENT ALERT

ID: {data.get("incident_id")}
Type: {data.get("incident_type")}
Severity: {data.get("severity")}
Location: {", ".join(data.get("location_id", []))}

Description:
{data.get("description")[:2000]}
"""

def lambda_handler(event, context):

    print("RAW EVENT:", json.dumps(event))

    for record in event["Records"]:
        try:
            message = json.loads(record["body"])
            data = message.get("body")

            if not data:
                print("No body found")
                continue

            msg = format_message(data)

            response = sns.publish(
                TopicArn=topic_arn,
                Subject="🚨 TMD Alert",
                Message=msg
            )

            print("SNS SENT:", response)

        except Exception as e:
            print("ERROR:", e)

    return {"statusCode": 200}