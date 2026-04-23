import json
import boto3
import os

sns = boto3.client("sns")
topic_arn = os.environ["TOPIC_ARN"]

def lambda_handler(event, context):

    for record in event["Records"]:
        message = json.loads(record["body"])

        data = message["body"]

        sns.publish(
            TopicArn=topic_arn,
            Message=json.dumps(data, ensure_ascii=False)
        )

    return {"statusCode": 200}