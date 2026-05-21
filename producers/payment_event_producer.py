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
    RAW_PAYMENTS_PREFIX
)

from config.logging_config import get_logger

fake = Faker()

logger = get_logger(
    "payment_event_producer",
    "payment_event_producer.log"
)

s3_client = boto3.client(
    "s3",
    region_name=AWS_REGION
)

PAYMENT_METHODS = [
    "UPI",
    "CREDIT_CARD",
    "DEBIT_CARD",
    "NET_BANKING",
    "COD",
    "WALLET"
]

PAYMENT_STATUS = [
    "SUCCESS",
    "FAILED",
    "PENDING",
    "REFUNDED"
]

BANK_NAMES = [
    "HDFC Bank",
    "ICICI Bank",
    "SBI",
    "Axis Bank",
    "Kotak Mahindra Bank",
    "Federal Bank",
    "Indian Bank"
]

def generate_payment_event():
    payment_amount = round(random.uniform(500, 50000), 2)
    payment_event = {
        "event_id": str(uuid.uuid4()),
        "event_type": "PAYMENT_EVENT",
        "event_time": datetime.utcnow().isoformat(),
        "payment_id": f"PAY-{random.randint(100000, 999999)}",
        "order_id": f"ORD-{random.randint(100000, 999999)}",
        "customer_id": f"CUST-{random.randint(1000, 9999)}",
        "customer_name": fake.name(),
        "email": fake.email(),
        "payment_method": random.choice(PAYMENT_METHODS),
        "payment_status": random.choice(PAYMENT_STATUS),
        "payment_amount": payment_amount,
        "currency": "INR",
        "bank_name": random.choice(BANK_NAMES),
        "transaction_reference": f"TXN-{uuid.uuid4().hex[:12].upper()}",
        "failure_reason": random.choice([
            None,
            "Insufficient Balance",
            "Invalid Card Details",
            "Bank Server Timeout",
            "Payment Gateway Error",
            "UPI Transaction Failed"
        ]),
        "payment_gateway": random.choice([
            "Razorpay",
            "Paytm",
            "PhonePe",
            "Google Pay",
            "Amazon Pay"
        ]),
        "device_type": random.choice([
            "Mobile",
            "Desktop",
            "Tablet"
        ]),
        "ip_address": fake.ipv4()
    }
    return payment_event

def upload_event_to_s3(payment_event):
    current_date = datetime.utcnow().strftime("%Y-%m-%d")
    file_name = (
        f"{RAW_PAYMENTS_PREFIX}"
        f"date={current_date}/"
        f"{payment_event['event_id']}.json"
    )
    s3_client.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=file_name,
        Body=json.dumps(payment_event),
        ContentType="application/json"
    )
    logger.info(f"Uploaded payment event: {payment_event['event_id']}")

    print(f"Uploaded: s3://{S3_BUCKET_NAME}/{file_name}")

def main():
    logger.info("Starting Payment Event Producer...")
    print("Starting Payment Event Producer...")
    while True:
        try:
            payment_event = generate_payment_event()
            upload_event_to_s3(payment_event)
            time.sleep(5)

        except Exception as error:
            logger.error(f"Error occurred: {str(error)}")
            print(f"Error occurred: {str(error)}")

if __name__ == "__main__":
    main()