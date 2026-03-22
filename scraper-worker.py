import json
import boto3
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import uuid
import hashlib

dynamodb = boto3.resource('dynamodb')
sns = boto3.client('sns')

post_table = dynamodb.Table('ScrapedPost')
dedup_table = dynamodb.Table('DeduplicationState')

TOPIC_ARN = "arn:aws:sns:us-east-1:415848059244:disaster-post-scraped-topic"
URL = "https://www.tmd.go.th/warning-and-events/warning-storm/"

#Function 1: Normalize Data
def normalize_post(text):
    event_type = "UNKNOWN"
    severity = "LOW"
    areas = []

    if "ฝนตกหนัก" in text:
        event_type = "ฝนตกหนัก"
        severity = "กลาง"
    elif "พายุ" in text:
        event_type = "พายุ"
        severity = "สูง"
    elif "น้ำท่วม" in text:
        event_type = "น้ำท่วม"
        severity = "สูง"
    elif "น้ำป่า" in text:
        event_type = "น้ำป่า"
        severity = "สูง"
    elif "แผ่นดินไหว" in text:
        event_type = "แผ่นดินไหว"
        severity = "สูง"
    elif "ไฟไหม้" in text:
        event_type = "ไฟไหม้"
        severity = "สูง"

    if "ภาคเหนือ" in text:
        areas.append("ภาคเหนือ")
    if "กรุงเทพ" in text:
        areas.append("กรุงเทพ")
    if "ภาคตะวันออก" in text:
        areas.append("ภาคตะวันออก")
    if "ภาคตะวันตก" in text:
        areas.append("ภาคตะวันตก")
    if "ภาคใต้" in text:
        areas.append("ภาคใต้")
    if "ภาคกลาง" in text:
        areas.append("ภาคกลาง")
    if "ภาคตะวันออกเฉียงเหนือ" in text:
        areas.append("ภาคตะวันออกเฉียงเหนือ")

    if not areas:
        areas.append("UNKNOWN")

    return {
        "eventType": event_type,
        "severity": severity,
        "affectedArea": areas
    }

#Function 2: Generate Hash
def generate_hash(text):
    return hashlib.md5(text.encode()).hexdigest()

#Function 3: Check Duplicate
def is_duplicate(content_hash):
    res = dedup_table.get_item(Key={"contentHash": content_hash})
    return "Item" in res

#Function 4: Save Dedup State
def save_dedup(content_hash, post_id):
    dedup_table.put_item(Item={
        "contentHash": content_hash,
        "postId": post_id,
        "publishedToQueue": True,
        "createdAt": datetime.utcnow().isoformat()
    })

#Function 5: Publish Event
def publish_event(event_message):
    sns.publish(
        TopicArn=TOPIC_ARN,
        Message=json.dumps(event_message, ensure_ascii=False)
    )

#Function 6: Save Post
def save_post(item):
    post_table.put_item(Item=item)

#Function 7: Main Pipeline
def process_post(text, warning_link):

    #1.Normalize
    normalized = normalize_post(text)

    #2.Generate hash
    content_hash = generate_hash(text)

    #3.Dedup check
    if is_duplicate(content_hash):
        print("Duplicate post, skip publish")
        return "duplicate"

    #4.Create post
    post_id = str(uuid.uuid4())

    item = {
        "postId": post_id,
        "messageText": text[:2000],
        "publishedAt": datetime.utcnow().isoformat(),
        "scrapedAt": datetime.utcnow().isoformat(),
        "sourceUrl": warning_link,
        "contentHash": content_hash,

        #structured data
        "eventType": normalized["eventType"],
        "severity": normalized["severity"],
        "affectedArea": normalized["affectedArea"]
    }

    #5.Save DB
    save_post(item)

    #6.Save dedup
    save_dedup(content_hash, post_id)

    print("Saved to DynamoDB")

    #7.Create Event
    event_message = {
        "header": {
            "messageId": str(uuid.uuid4()),
            "timestamp": datetime.utcnow().isoformat(),
            "version": "v1",
            "source": "scraping-service"
        },
        "body": {
            "postId": post_id,
            "messageText": text[:200],
            "publishedAt": datetime.utcnow().isoformat(),
            "sourceUrl": warning_link,
            "contentHash": content_hash,

            #structured data
            "eventType": normalized["eventType"],
            "severity": normalized["severity"],
            "affectedArea": normalized["affectedArea"]
        }
    }

    #8.Publish Event
    publish_event(event_message)

    print("Event published")

    return "processed"

#Lambda Handler
def lambda_handler(event, context):

    print("Fetching warning page")

    res = requests.get(URL, timeout=10)
    soup = BeautifulSoup(res.text, "html.parser")

    all_links = soup.find_all("a", href=True)

    warning_links = []

    for a in all_links:
        href = a["href"]

        if href.startswith("/warning-and-events/warning-storm/") and len(href) > 50:
            link = "https://www.tmd.go.th" + href

            if link not in warning_links:
                warning_links.append(link)

    if not warning_links:
        print("No warning links found")
        return {"statusCode": 500}

    # เอา 3 อันแรก (ล่าสุด)
    latest_links = warning_links[:3]

    print("Latest links:", latest_links)

    results = []

    for link in latest_links:
        try:
            res2 = requests.get(link, timeout=10)
            soup2 = BeautifulSoup(res2.text, "html.parser")

            content = soup2.find("main")
            if not content:
                continue

            text = content.get_text(separator="\n", strip=True)

            result = process_post(text, link)

            results.append({
                "link": link,
                "result": result
            })

        except Exception as e:
            print("Error processing:", link, e)

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Scraping completed",
            "results": results
        })
    }