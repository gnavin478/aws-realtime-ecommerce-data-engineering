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
    RAW_PRODUCT_VIEWS_PREFIX
)

from config.logging_config import get_logger

fake = Faker()

logger = get_logger(
    "product_view_event_producer",
    "product_view_event_producer.log"
)

s3_client = boto3.client(
    "s3",
    region_name=AWS_REGION
)


PRODUCTS = [
    "iPhone 15",
    "Samsung Galaxy S24",
    "OnePlus 12",
    "MacBook Pro",
    "Dell XPS Laptop",
    "Sony Headphones",
    "Boat Earbuds",
    "Apple Watch",
    "Gaming Mouse",
    "Bluetooth Speaker",
    "Power Bank",
    "Nike Shoes",
    "Adidas Sneakers",
    "Levi's Jeans",
    "Office Chair"
]

CATEGORIES = [
    "Mobiles",
    "Electronics",
    "Fashion",
    "Accessories",
    "Furniture",
    "Computers",
    "Footwear"
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

def generate_product_view_event():
    product_view_event = {
        "event_id": str(uuid.uuid4()),
        "event_type": "PRODUCT_VIEW_EVENT",
        "event_time": datetime.utcnow().isoformat(),
        "view_id": f"VIEW-{random.randint(100000, 999999)}",
        "customer_id": f"CUST-{random.randint(1000, 9999)}",
        "customer_name": fake.name(),
        "email": fake.email(),
        "product_id": f"PROD-{random.randint(100, 999)}",
        "product_name": random.choice(PRODUCTS),
        "category": random.choice(CATEGORIES),
        "product_price": round(
            random.uniform(500, 50000), 2
        ),
        "view_duration_seconds": random.randint(5, 300),
        "device_type": random.choice(DEVICES),
        "browser": random.choice(BROWSERS),
        "ip_address": fake.ipv4(),
        "city": fake.city(),
        "state": fake.state(),
        "country": "India",
        "session_id": uuid.uuid4().hex,
        "is_logged_in": random.choice([
            True,
            False
        ])
    }
    return product_view_event


def upload_event_to_s3(product_view_event):
    current_date = datetime.utcnow().strftime("%Y-%m-%d")
    file_name = (
        f"{RAW_PRODUCT_VIEWS_PREFIX}"
        f"date={current_date}/"
        f"{product_view_event['event_id']}.json"
    )

    s3_client.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=file_name,
        Body=json.dumps(product_view_event),
        ContentType="application/json"
    )

    logger.info(f"Uploaded product view event: " f"{product_view_event['event_id']}")
    print(f"Uploaded: " f"s3://{S3_BUCKET_NAME}/{file_name}")

def main():
    logger.info("Starting Product View Event Producer...")
    print("Starting Product View Event Producer...")

    while True:
        try:
            product_view_event = (generate_product_view_event())
            upload_event_to_s3(product_view_event)
            time.sleep(5)

        except Exception as error:
            logger.error(f"Error occurred: {str(error)}")
            print(f"Error occurred: {str(error)}")

if __name__ == "__main__":
    main()