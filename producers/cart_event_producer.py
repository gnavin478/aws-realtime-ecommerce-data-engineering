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
    RAW_CART_PREFIX
)

from config.logging_config import get_logger

fake = Faker()

logger = get_logger(
    "cart_event_producer",
    "cart_event_producer.log"
)

s3_client = boto3.client(
    "s3",
    region_name=AWS_REGION
)

PRODUCTS = [
    "iPhone 15",
    "Samsung Galaxy S24",
    "MacBook Pro",
    "Dell XPS Laptop",
    "Sony Headphones",
    "Boat Earbuds",
    "Apple Watch",
    "Gaming Mouse",
    "Bluetooth Speaker",
    "Power Bank"
]

CART_ACTIONS = [
    "ADD_TO_CART",
    "REMOVE_FROM_CART",
    "UPDATE_QUANTITY",
    "CHECKOUT_INITIATED",
    "CART_ABANDONED"
]

def generate_cart_event():
    quantity = random.randint(1, 5)
    price = round(random.uniform(500, 50000), 2)
    cart_event = {
        "event_id": str(uuid.uuid4()),
        "event_type": "CART_EVENT",
        "event_time": datetime.utcnow().isoformat(),
        "cart_id": f"CART-{random.randint(100000, 999999)}",
        "customer_id": f"CUST-{random.randint(1000, 9999)}",
        "customer_name": fake.name(),
        "email": fake.email(),
        "product_id": f"PROD-{random.randint(100, 999)}",
        "product_name": random.choice(PRODUCTS),
        "cart_action": random.choice(CART_ACTIONS),
        "quantity": quantity,
        "price": price,
        "total_amount": round(quantity * price, 2),
        "device_type": random.choice([
            "Mobile",
            "Desktop",
            "Tablet"
        ]),
        "browser": random.choice([
            "Chrome",
            "Firefox",
            "Safari",
            "Edge"
        ]),
        "session_id": uuid.uuid4().hex,
        "is_logged_in": random.choice([
            True,
            False
        ])
    }
    return cart_event

def upload_event_to_s3(cart_event):
    current_date = datetime.utcnow().strftime("%Y-%m-%d")
    file_name = (
        f"{RAW_CART_PREFIX}"
        f"date={current_date}/"
        f"{cart_event['event_id']}.json"
    )
    s3_client.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=file_name,
        Body=json.dumps(cart_event),
        ContentType="application/json"
    )
    logger.info(f"Uploaded cart event: " f"{cart_event['event_id']}")
    print(f"Uploaded: " f"s3://{S3_BUCKET_NAME}/{file_name}")

def main():
    logger.info("Starting Cart Event Producer...")
    print("Starting Cart Event Producer...")
    while True:
        try:
            cart_event = generate_cart_event()
            upload_event_to_s3(cart_event)
            time.sleep(5)

        except Exception as error:
            logger.error(f"Error occurred: {str(error)}")
            print(f"Error occurred: {str(error)}")

if __name__ == "__main__":
    main()