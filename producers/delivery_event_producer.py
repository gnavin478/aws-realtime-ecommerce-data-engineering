import json
import time
import uuid
import random
from datetime import datetime, timedelta

import boto3
from faker import Faker

from config.constants import (
    AWS_REGION,
    S3_BUCKET_NAME,
    RAW_DELIVERY_PREFIX
)

from config.logging_config import get_logger

fake = Faker()

logger = get_logger(
    "delivery_event_producer",
    "delivery_event_producer.log"
)

s3_client = boto3.client(
    "s3",
    region_name=AWS_REGION
)

DELIVERY_STATUS = [
    "ORDER_PLACED",
    "PACKED",
    "SHIPPED",
    "OUT_FOR_DELIVERY",
    "DELIVERED",
    "DELAYED",
    "RETURNED"
]

DELIVERY_PARTNERS = [
    "Amazon Logistics",
    "BlueDart",
    "Delhivery",
    "DTDC",
    "Ekart",
    "Ecom Express"
]

def generate_delivery_event():
    order_date = datetime.utcnow()
    estimated_delivery = order_date + timedelta(
        days=random.randint(2, 7)
    )
    delivery_event = {
        "event_id": str(uuid.uuid4()),
        "event_type": "DELIVERY_EVENT",
        "event_time": datetime.utcnow().isoformat(),
        "delivery_id": f"DEL-{random.randint(100000, 999999)}",
        "order_id": f"ORD-{random.randint(100000, 999999)}",
        "customer_id": f"CUST-{random.randint(1000, 9999)}",
        "customer_name": fake.name(),
        "phone_number": fake.phone_number(),
        "delivery_status": random.choice(DELIVERY_STATUS),
        "delivery_partner": random.choice(DELIVERY_PARTNERS),
        "tracking_number": f"TRK-{uuid.uuid4().hex[:12].upper()}",
        "shipping_address": fake.address(),
        "city": fake.city(),
        "state": fake.state(),
        "country": "India",
        "pincode": fake.postcode(),
        "estimated_delivery_date": estimated_delivery.isoformat(),
        "delivery_charges": round(
            random.uniform(20, 500), 2
        ),
        "is_express_delivery": random.choice([
            True,
            False
        ])
    }
    return delivery_event

def upload_event_to_s3(delivery_event):
    current_date = datetime.utcnow().strftime("%Y-%m-%d")
    file_name = (
        f"{RAW_DELIVERY_PREFIX}"
        f"date={current_date}/"
        f"{delivery_event['event_id']}.json"
    )
    s3_client.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=file_name,
        Body=json.dumps(delivery_event),
        ContentType="application/json"
    )
    logger.info(f"Uploaded delivery event: " f"{delivery_event['event_id']}")
    print(f"Uploaded: " f"s3://{S3_BUCKET_NAME}/{file_name}")

def main():
    logger.info("Starting Delivery Event Producer...")
    print("Starting Delivery Event Producer...")

    while True:
        try:
            delivery_event = generate_delivery_event()
            upload_event_to_s3(delivery_event)
            time.sleep(5)

        except Exception as error:
            logger.error(f"Error occurred: {str(error)}")
            print(f"Error occurred: {str(error)}")

if __name__ == "__main__":
    main()