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
BASE_URL = os.environ["BASE_URL"]

# Tables
post_table = dynamodb.Table(POST_TABLE)
dedup_table = dynamodb.Table(DEDUP_TABLE)
job_table = dynamodb.Table(JOB_TABLE)
counter_table = dynamodb.Table(COUNTER_TABLE)

# Categories
CATEGORIES = {
    "storm": f"{BASE_URL}/warnings/weather",
    "earthquake": f"{BASE_URL}/warnings/earthquake"
}

# ------------------------
# Utility
# ------------------------

def generate_post_id():

    res = counter_table.update_item(
        Key={"counterName": "POST"},
        UpdateExpression="SET #v = if_not_exists(#v, :zero) + :inc",
        ExpressionAttributeNames={
            "#v": "value"
        },
        ExpressionAttributeValues={
            ":inc": 1,
            ":zero": 0
        },
        ReturnValues="UPDATED_NEW"
    )

    return f"INC_{int(res['Attributes']['value']):04d}"


def generate_hash(text):

    return hashlib.md5(
        text.encode("utf-8")
    ).hexdigest()


def is_duplicate(h):

    return "Item" in dedup_table.get_item(
        Key={"contentHash": h}
    )


def save_dedup(h, pid):

    dedup_table.put_item(
        Item={
            "contentHash": h,
            "postId": pid,
            "createdAt": datetime.utcnow().isoformat()
        }
    )


def get_with_retry(url):

    for i in range(3):

        try:

            r = requests.get(
                url,
                timeout=15,
                headers={
                    "User-Agent": "Mozilla/5.0"
                }
            )

            if r.status_code != 200:
                raise Exception(r.status_code)

            return r

        except Exception as e:

            print(f"Retry {i+1}: {url}")
            print(e)

            time.sleep(2 ** i)

    raise Exception(f"HTTP fail: {url}")

# ------------------------
# Normalize
# ------------------------

def normalize_post(text):

    if "แผ่นดินไหว" in text:
        return "แผ่นดินไหว", "สูง"

    elif "พายุ" in text:
        return "พายุ", "สูง"

    elif "ฝนตกหนัก" in text:
        return "ฝนตกหนัก", "กลาง"

    elif "น้ำท่วม" in text:
        return "น้ำท่วม", "สูง"

    elif "ระดับน้ำเพิ่มขึ้น" in text:
        return "ระดับน้ำเพิ่มขึ้น", "กลาง"

    return "UNKNOWN", "LOW"

# ------------------------
# Dynamic Location Detection
# ------------------------

def extract_location(text):

    found = []

    text = text.replace("\n", " ")
    text = re.sub(r"\s+", " ", text)

    patterns = [

        # จังหวัด
        r"จังหวัด([ก-๙]+)",

        # จ.
        r"จ\.([ก-๙]+)",

        # อำเภอ
        r"อำเภอ([ก-๙]+)",

        # เขต
        r"(เขต[ก-๙]+)",

        # ภาค
        r"(ภาคเหนือ|ภาคใต้|ภาคกลาง|ภาคตะวันออก|ภาคตะวันออกเฉียงเหนือ)",

        # ตัวเมือง
        r"(ตัวเมือง[ก-๙]+)",

        # พื้นที่...
        r"(พื้นที่[ก-๙]+)",

        # ริมแม่น้ำ...
        r"(ริมแม่น้ำ[ก-๙]+)",

        # ชายฝั่ง...
        r"(ชายฝั่ง[ก-๙]+)"
    ]

    for pattern in patterns:

        matches = re.findall(pattern, text)

        for match in matches:

            if isinstance(match, tuple):
                match = match[0]

            match = match.strip()

            if len(match) > 1:
                found.append(match)

    # remove duplicates
    unique = []

    seen = set()

    for item in found:

        if item not in seen:

            seen.add(item)

            unique.append(item)

    if not unique:
        unique.append("UNKNOWN")

    return unique

# ------------------------
# Extract Country
# ------------------------

def extract_country(text):

    match = re.search(
        r"ประเทศ([^\s\(]+)",
        text
    )

    if match:
        return match.group(1)

    match = re.search(
        r"\(([^)]+)\)",
        text
    )

    if match:
        return match.group(1)

    return None

# ------------------------
# Extract Links
# ------------------------

def extract_links(soup, category):

    links = set()

    for a in soup.select("a[href]"):

        href = a.get("href")

        if not href:
            continue

        # ต้องเป็น warning detail page
        if not href.startswith("/warnings/"):
            continue

        # กันหน้า list
        if href in [
            "/warnings/weather",
            "/warnings/earthquake"
        ]:
            continue

        # Storm
        if category == "storm":

            if "earthquake" in href:
                continue

        # Earthquake
        elif category == "earthquake":

            if "earthquake" not in href:
                continue

        full_url = BASE_URL + href

        links.add(full_url)

    print(f"✅ {category} links: {len(links)}")

    for l in links:
        print("➡️", l)

    return list(links)

# ------------------------
# Extract Content
# ------------------------

def extract_content(html, link):

    soup = BeautifulSoup(
        html,
        "html.parser"
    )

    # Remove script/style
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()

    # ------------------------
    # Title
    # ------------------------

    title = ""

    title_tag = soup.find(["h1", "h2"])

    if title_tag:

        title = title_tag.get_text(
            " ",
            strip=True
        )

    # ------------------------
    # Issued Time
    # ------------------------

    issued_time = None

    time_tag = soup.find("time")

    if time_tag:

        issued_time = (
            time_tag.get("datetime")
            or time_tag.get_text(strip=True)
        )

    # fallback regex
    if not issued_time:

        match = re.search(
            r"Issued At\s+([0-9]{1,2}\s+\w+\s+[0-9]{4},\s+[0-9]{2}:[0-9]{2})",
            soup.get_text(" ", strip=True)
        )

        if match:
            issued_time = match.group(1)

    # fallback current time
    if not issued_time:

        issued_time = datetime.utcnow().isoformat()

    # ------------------------
    # Extract real content only
    # ------------------------

    body = ""

    detail_header = soup.find(
        string=re.compile(
            "Detailed Information",
            re.IGNORECASE
        )
    )

    if detail_header:

        parent = detail_header.parent

        next_element = parent.find_next()

        if next_element:

            body = next_element.get_text(
                " ",
                strip=True
            )

    # fallback
    if not body:

        paragraphs = soup.find_all("p")

        texts = []

        for p in paragraphs:

            t = p.get_text(
                " ",
                strip=True
            )

            if len(t) > 20:
                texts.append(t)

        body = "\n".join(texts)

    # ------------------------
    # Full text
    # ------------------------

    full_text = f"""
{title}
{body}
"""

    full_text = re.sub(
        r"\s+",
        " ",
        full_text
    ).strip()

    print("========== CONTENT ==========")
    print(full_text[:2000])
    print("=============================")

    if len(full_text) < 20:

        print("❌ Empty content")

        return None

    return {
        "text": full_text,
        "issued_time": issued_time
    }

# ------------------------
# Main
# ------------------------

def lambda_handler(event, context):

    trace_id = context.aws_request_id

    print("🔥 START")

    try:

        # รองรับ EventBridge + SQS
        if "Records" in event:

            body = json.loads(
                event["Records"][0]["body"]
            )

        else:
            body = event

        job_id = body.get(
            "jobId",
            "manual-job"
        )

        print(f"Job ID: {job_id}")

        # Loop categories
        for category, url in CATEGORIES.items():

            print(f"\n========== {category.upper()} ==========")

            try:

                # Main page
                response = get_with_retry(url)

                print(f"✅ HTTP {response.status_code}")

                soup = BeautifulSoup(
                    response.text,
                    "html.parser"
                )

                # Extract detail links
                links = extract_links(
                    soup,
                    category
                )

                # ALL links
                for link in links:

                    print(f"\n➡️ SCRAPING: {link}")

                    try:

                        # Detail page
                        detail = get_with_retry(link)

                        # Extract content
                        content = extract_content(
                            detail.text,
                            link
                        )

                        if not content:
                            continue

                        text = content["text"]

                        issued_time = content["issued_time"]

                        # Original dedup
                        h = generate_hash(text)

                        # Duplicate
                        if is_duplicate(h):

                            print("⏩ Duplicate")

                            continue

                        # Normalize
                        event_type, severity = normalize_post(text)

                        # Extract location
                        location = extract_location(text)

                        # Add country
                        if category == "earthquake":

                            country = extract_country(text)

                            if country and country not in location:
                                location.append(country)

                        print("📍 location:", location)

                        # Generate ID
                        post_id = generate_post_id()

                        now = datetime.utcnow().isoformat()

                        # DynamoDB item
                        post_item = {
                            "incident_id": post_id,
                            "incident_type": event_type,
                            "severity": severity,
                            "location_id": location,
                            "status": "reported",
                            "incident_start": issued_time,
                            "occured_time": issued_time,
                            "ended_time": None,
                            "description": text[:2000],
                            "reporter_id": "TMD",
                            "source_category": category,
                            "created_at": now,
                            "update_id": None
                        }

                        # Save DynamoDB
                        post_table.put_item(
                            Item=post_item
                        )

                        # Save dedup
                        save_dedup(h, post_id)

                        # Send SQS
                        sqs.send_message(
                            QueueUrl=PUBLISH_QUEUE_URL,
                            MessageBody=json.dumps(
                                {
                                    "header": {
                                        "messageId": str(uuid.uuid4()),
                                        "timestamp": now,
                                        "schemaVersion": "v1",
                                        "source": "scraping-service",
                                        "traceId": trace_id,
                                        "jobId": job_id
                                    },
                                    "body": post_item
                                },
                                ensure_ascii=False
                            )
                        )

                        print(f"✅ Inserted: {post_id}")

                    except Exception as e:

                        print(f"❌ Error link: {e}")

            except Exception as e:

                print(f"❌ Error category: {e}")

        # COMPLETE JOB
        print("🔥 COMPLETE JOB")

        job_table.update_item(
            Key={"jobId": job_id},
            UpdateExpression="SET #s = :s",
            ExpressionAttributeNames={
                "#s": "status"
            },
            ExpressionAttributeValues={
                ":s": "COMPLETED"
            }
        )

    except Exception as e:

        print(f"❌ FATAL: {e}")

        try:

            job_table.update_item(
                Key={"jobId": job_id},
                UpdateExpression="SET #s = :s",
                ExpressionAttributeNames={
                    "#s": "status"
                },
                ExpressionAttributeValues={
                    ":s": "FAILED"
                }
            )

        except:
            pass

    print("🔥 END")

    return {
        "statusCode": 200
    }