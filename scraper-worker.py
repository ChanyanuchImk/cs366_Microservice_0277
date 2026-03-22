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
    area = "UNKNOWN"

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
        area = "ภาคเหนือ"
    elif "กรุงเทพ" in text:
        area = "กรุงเทพ"
    elif "ภาคตะวันออก" in text:
        area = "ภาคตะวันออก"
    elif "ภาคตะวันตก" in text:
        area = "ภาคตะวันตก"
    elif "ภาคใต้" in text:
        area = "ภาคใต้"
    elif "ภาคกลาง" in text:
        area = "ภาคกลาง"
    elif "ภาคตะวันออกเฉียงใต้" in text:
        area = "ภาคตะวันออกเฉียงใต้"

    return {
        "eventType": event_type,
        "severity": severity,
        "affectedArea": area
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

    #Step1:ดึงหน้า list
    res = requests.get(URL, timeout=10)
    soup = BeautifulSoup(res.text, "html.parser")

    links = soup.find_all("a", href=True)

    warning_link = None
    for a in links:
        if "/warning-and-events/warning-storm/" in a["href"]:
            warning_link = "https://www.tmd.go.th" + a["href"]
            break

    if not warning_link:
        print("Warning link not found")
        return {"statusCode": 500}

    print("Latest warning:", warning_link)

    #Step2:เข้า detail page
    res2 = requests.get(warning_link, timeout=10)
    soup2 = BeautifulSoup(res2.text, "html.parser")

    content = soup2.find("main")
    if not content:
        print("Warning content not found")
        return {"statusCode": 500}

    text = content.get_text(separator="\n", strip=True)

    #Step3:Process pipeline
    result = process_post(text, warning_link)

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Scraping completed",
            "result": result
        })
    }