import json
import boto3
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import uuid
import hashlib
import os
import time
import re

# AWS Clients
dynamodb = boto3.resource("dynamodb")
sqs = boto3.client("sqs")

# ENV
POST_TABLE = os.environ["POST_TABLE"]
DEDUP_TABLE = os.environ["DEDUP_TABLE"]
JOB_TABLE = os.environ["JOB_TABLE"]
PUBLISH_QUEUE_URL = os.environ["PUBLISH_QUEUE_URL"]
COUNTER_TABLE = os.environ["COUNTER_TABLE"]

# Tables
post_table = dynamodb.Table(POST_TABLE)
dedup_table = dynamodb.Table(DEDUP_TABLE)
job_table = dynamodb.Table(JOB_TABLE)
counter_table = dynamodb.Table(COUNTER_TABLE)

# Categories
CATEGORIES = {
    "storm": "https://www.tmd.go.th/warning-and-events/warning-storm/",
    "earthquake": "https://www.tmd.go.th/warning-and-events/warning-earthquake"
}

# Utility

def generate_post_id():
    res = counter_table.update_item(
        Key={"counterName": "POST"},
        UpdateExpression="SET #v = if_not_exists(#v, :zero) + :inc",
        ExpressionAttributeNames={"#v": "value"},
        ExpressionAttributeValues={":inc": 1, ":zero": 0},
        ReturnValues="UPDATED_NEW"
    )
    return f"INC_{int(res['Attributes']['value']):04d}"


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
            r = requests.get(url, timeout=10)
            if r.status_code != 200:
                raise Exception(r.status_code)
            return r
        except Exception:
            print(f"Retry {i+1}: {url}")
            time.sleep(2 ** i)
    raise Exception(f"HTTP fail: {url}")

# NLP / Extract

def normalize_post(text):
    if "แผ่นดินไหว" in text:
        return "แผ่นดินไหว", "สูง"
    elif "พายุ" in text:
        return "พายุ", "สูง"
    elif "ฝนตกหนัก" in text:
        return "ฝนตกหนัก", "กลาง"
    return "UNKNOWN", "LOW"

def extract_location(text, category):
    #storm
    if category == "storm":
        areas = []

        if "ภาคเหนือ" in text: areas.append("ภาคเหนือ")
        if "กรุงเทพ" in text: areas.append("กรุงเทพ")
        if "ภาคตะวันออก" in text: areas.append("ภาคตะวันออก")
        if "ภาคกลาง" in text: areas.append("ภาคกลาง")
        if "ภาคตะวันออกเฉียงเหนือ" in text: areas.append("ภาคตะวันออกเฉียงเหนือ")

        if not areas:
            areas.append("UNKNOWN")

        return areas

    #earthquake
    elif category == "earthquake":
        match = re.search(r"จ\.([^\s]+)", text)
        if match:
            return [match.group(1)]

        match = re.search(r"จังหวัด([^\s]+)", text)
        if match:
            return [match.group(1)]

        return ["UNKNOWN"]

    return ["UNKNOWN"]

def extract_country(text):
    match = re.search(r"ประเทศ([^\s\(]+)", text)
    if match:
        return match.group(1)

    match = re.search(r"\(([^)]+)\)", text)
    if match:
        return match.group(1)

    return None

# HTML Extract

def extract_links(soup, category):
    links = set()

    for a in soup.select("a[href]"):
        href = a.get("href")
        if not href:
            continue

        if category == "storm" and "warning-storm" in href:
            pass
        elif category == "earthquake" and "warning-earthquake" in href:
            pass
        else:
            continue

        if not href.startswith("http"):
            href = "https://www.tmd.go.th" + href

        links.add(href)

    print(f"{category} links: {len(links)}")
    return list(links)


def extract_content(html, link):
    soup = BeautifulSoup(html, "html.parser")

    title = soup.select_one("h1")
    paragraphs = soup.select("p")

    if not title:
        print(f"No title: {link}")
        return None

    text_parts = [title.get_text(strip=True)]

    for p in paragraphs:
        t = p.get_text(strip=True)
        if len(t) > 20 and "เมนู" not in t:
            text_parts.append(t)

    text = "\n".join(text_parts)

    if len(text) < 50:
        return None

    return text

# Main

def lambda_handler(event, context):
    trace_id = context.aws_request_id
    print("🔥 START")

    try:
        #EventBridge+SQS
        if "Records" in event:
            body = json.loads(event["Records"][0]["body"])
        else:
            body = event

        job_id = body.get("jobId", "manual-job")
        print(f"Job ID: {job_id}")

        for category, url in CATEGORIES.items():
            print(f"\n=== {category} ===")

            try:
                response = get_with_retry(url)
                soup = BeautifulSoup(response.text, "html.parser")

                links = extract_links(soup, category)

                for link in links[:5]:
                    print(f"{link}")

                    try:
                        detail = get_with_retry(link)
                        text = extract_content(detail.text, link)

                        if not text:
                            continue

                        h = generate_hash(text)

                        if is_duplicate(h):
                            print("Duplicate")
                            continue

                        event_type, severity = normalize_post(text)
                        location = extract_location(text, category)

                        if category == "earthquake":
                            country = extract_country(text)
                            if country and country not in location:
                                location.append(country)

                        print("location:", location)

                        post_id = generate_post_id()
                        now = datetime.utcnow().isoformat()

                        post_item = {
                            "incident_id": post_id,
                            "incident_type": event_type,
                            "severity": severity,
                            "location_id": location,
                            "status": "reported",
                            "incident_start": None,
                            "occured_time": None,
                            "ended_time": None,
                            "description": text[:2000],
                            "reporter_id": "TMD",
                            "source_category": category,
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

                        print(f"Inserted: {post_id}")
                        print("Time left:", context.get_remaining_time_in_millis())

                    except Exception as e:
                        print(f"Error link: {e}")

            except Exception as e:
                print(f"Error category: {e}")

        print("COMPLETE JOB")

        job_table.update_item(
            Key={"jobId": job_id},
            UpdateExpression="SET #s = :s",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={":s": "COMPLETED"}
        )

    except Exception as e:
        print(f"FATAL: {e}")

        try:
            job_table.update_item(
                Key={"jobId": job_id},
                UpdateExpression="SET #s = :s",
                ExpressionAttributeNames={"#s": "status"},
                ExpressionAttributeValues={":s": "FAILED"}
            )
        except:
            pass

    print("END")
    return {"statusCode": 200}