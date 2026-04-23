import json
import boto3
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import uuid
import hashlib
import os
import time

# AWS Clients
dynamodb = boto3.resource("dynamodb")
sqs = boto3.client("sqs")

# Environment Variables
POST_TABLE = os.environ["POST_TABLE"]
DEDUP_TABLE = os.environ["DEDUP_TABLE"]
JOB_TABLE = os.environ["JOB_TABLE"]
PUBLISH_QUEUE_URL = os.environ["PUBLISH_QUEUE_URL"]
COUNTER_TABLE = os.environ["COUNTER_TABLE"]

# DynamoDB Tables
post_table = dynamodb.Table(POST_TABLE)
dedup_table = dynamodb.Table(DEDUP_TABLE)
job_table = dynamodb.Table(JOB_TABLE)
counter_table = dynamodb.Table(COUNTER_TABLE)

URL = "https://www.tmd.go.th/warning-and-events/warning-storm/"

def generate_post_id():
    res = counter_table.update_item(
        Key={"counterName": "POST"},
        UpdateExpression="SET #v = if_not_exists(#v, :zero) + :inc",
        ExpressionAttributeNames={"#v": "value"},
        ExpressionAttributeValues={":inc": 1, ":zero": 0},
        ReturnValues="UPDATED_NEW"
    )
    return f"INC_{int(res['Attributes']['value']):04d}"


def normalize_post(text):
    event_type = "UNKNOWN"
    severity = "LOW"
    areas = []

    if "ฝนตกหนัก" in text:
        event_type, severity = "ฝนตกหนัก", "กลาง"
    elif "พายุ" in text:
        event_type, severity = "พายุ", "สูง"
    elif "น้ำท่วม" in text:
        event_type, severity = "น้ำท่วม", "สูง"
    elif "น้ำป่า" in text:
        event_type, severity = "น้ำป่า", "สูง"
    elif "แผ่นดินไหว" in text:
        event_type, severity = "แผ่นดินไหว", "สูง"

    if "ภาคเหนือ" in text: areas.append("ภาคเหนือ")
    if "กรุงเทพ" in text: areas.append("กรุงเทพ")
    if "ภาคตะวันออก" in text: areas.append("ภาคตะวันออก")
    if "ภาคกลาง" in text: areas.append("ภาคกลาง")
    if "ภาคตะวันออกเฉียงเหนือ" in text: areas.append("ภาคตะวันออกเฉียงเหนือ")

    if not areas:
        areas.append("UNKNOWN")

    return {"eventType": event_type, "severity": severity, "affectedArea": areas}


def generate_hash(text):
    return hashlib.md5(text.encode("utf-8")).hexdigest()


def is_duplicate(h):
    return "Item" in dedup_table.get_item(Key={"contentHash": h})


def save_dedup(h, pid):
    dedup_table.put_item(Item={
        "contentHash": h,
        "postId": pid,
        "createdAt": datetime.utcnow().isoformat()
    })


def get_with_retry(url):
    for i in range(3):
        try:
            r = requests.get(url, timeout=5)
            if r.status_code != 200:
                raise Exception(r.status_code)
            return r
        except Exception:
            time.sleep(2 ** i)
    raise Exception("HTTP fail")

def lambda_handler(event, context):
    trace_id = context.aws_request_id

    for record in event["Records"]:
        body = json.loads(record["body"])
        job_id = body["jobId"]

        response = get_with_retry(URL)
        soup = BeautifulSoup(response.text, "html.parser")

        links = []
        for a in soup.find_all("a", href=True):
            if "/warning-storm/" in a["href"]:
                links.append("https://www.tmd.go.th" + a["href"])

        for link in links[:3]:
            detail = get_with_retry(link)
            content = BeautifulSoup(detail.text, "html.parser").find("main")
            if not content:
                continue

            text = content.get_text("\n", strip=True)
            norm = normalize_post(text)
            h = generate_hash(text)

            if is_duplicate(h):
                continue

            post_id = generate_post_id()
            now = datetime.utcnow().isoformat()

            post_item = {
                "incident_id": post_id,
                "incident_type": norm["eventType"],
                "severity": norm["severity"],
                "location_id": norm["affectedArea"],
                "status": "reported",
                "incident_start": None,
                "occured_time": None,
                "ended_time": None,
                "description": text[:2000],
                "reporter_id": "TMD",
                "created_at": now,
                "update_id": None
            }

            post_table.put_item(Item=post_item)
            save_dedup(h, post_id)

            sqs.send_message(
                QueueUrl=PUBLISH_QUEUE_URL,
                MessageBody=json.dumps({
                    "header": {
                        "messageId": str(uuid.uuid4()),
                        "timestamp": now,
                        "schemaVersion": "v1",
                        "source": "scraping-service",
                        "traceId": trace_id,
                        "jobId": job_id
                    },
                    "body": post_item
                }, ensure_ascii=False)
            )

        job_table.update_item(
            Key={"jobId": job_id},
            UpdateExpression="SET #s = :s",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={":s": "COMPLETED"}
        )

    return {"statusCode": 200}