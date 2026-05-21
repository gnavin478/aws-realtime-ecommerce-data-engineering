import json
import time
import uuid
import random
from datetime import datetime

import boto3
from faker import Faker

from config.constants import (
    AWS_REGION,
    S3_BUCKET_NAME,
    RAW_CUSTOMER_ACTIVITY_PREFIX
)

from config.logging_config import get_logger

fake = Faker()

logger = get_logger(
    "customer_activity_producer",
    "customer_activity_producer.log"
)

s3_client = boto3.client(
    "s3",
    region_name=AWS_REGION
)

ACTIVITY_TYPES = [
    "LOGIN",
    "LOGOUT",
    "SEARCH",
    "PRODUCT_CLICK",
    "ADD_TO_WISHLIST",
    "REMOVE_FROM_WISHLIST",
    "FILTER_APPLIED",
    "SORT_APPLIED",
    "PAGE_VIEW",
    "PROFILE_UPDATED"
]

DEVICES = [
    "Mobile",
    "Desktop",
    "Tablet"
]

BROWSERS = [
    "Chrome",
    "Firefox",
    "Safari",
    "Edge"
]

SEARCH_KEYWORDS = [
    "mobile phone",
    "laptop",
    "headphones",
    "watch",
    "shoes",
    "shirt",
    "bag",
    "speaker",
    "keyboard",
    "monitor"
]

def generate_customer_activity_event():
    activity_type = random.choice(ACTIVITY_TYPES)
    customer_activity_event = {
        "event_id": str(uuid.uuid4()),
        "event_type": "CUSTOMER_ACTIVITY_EVENT",
        "event_time": datetime.utcnow().isoformat(),
        "activity_id": f"ACT-{random.randint(100000, 999999)}",
        "customer_id": f"CUST-{random.randint(1000, 9999)}",
        "customer_name": fake.name(),
        "email": fake.email(),
        "activity_type": activity_type,
        "session_id": uuid.uuid4().hex,
        "search_keyword": random.choice(SEARCH_KEYWORDS)
        if activity_type == "SEARCH"
        else None,
        "page_url": fake.uri_path(),
        "referrer_url": fake.url(),
        "device_type": random.choice(DEVICES),
        "browser": random.choice(BROWSERS),
        "ip_address": fake.ipv4(),
        "city": fake.city(),
        "state": fake.state(),
        "country": "India"
    }
    return customer_activity_event

def upload_event_to_s3(customer_activity_event):
    current_date = datetime.utcnow().strftime("%Y-%m-%d")
    file_name = (
        f"{RAW_CUSTOMER_ACTIVITY_PREFIX}"
        f"date={current_date}/"
        f"{customer_activity_event['event_id']}.json"
    )
    s3_client.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=file_name,
        Body=json.dumps(customer_activity_event),
        ContentType="application/json"
    )
    logger.info(f"Uploaded customer activity event: " f"{customer_activity_event['event_id']}")

    print(f"Uploaded: s3://{S3_BUCKET_NAME}/{file_name}")

def main():
    logger.info("Starting Customer Activity Producer...")
    print("Starting Customer Activity Producer...")
    while True:
        try:
            customer_activity_event = generate_customer_activity_event()
            upload_event_to_s3(customer_activity_event)
            time.sleep(5)
        except Exception as error:
            logger.error(f"Error occurred: {str(error)}")
            print(f"Error occurred: {str(error)}")

if __name__ == "__main__":
    main()