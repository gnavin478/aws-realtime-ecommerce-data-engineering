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
    RAW_ORDERS_PREFIX
)

from config.logging_config import get_logger

fake = Faker()

logger = get_logger(
    "order_event_producer",
    "order_event_producer.log"
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
    "HP Pavilion Laptop",
    "Sony Headphones",
    "Boat Earbuds",
    "Apple Watch",
    "Noise Smart Watch",
    "iPad Air",
    "Samsung Tablet",
    "Mechanical Keyboard",
    "Gaming Mouse",
    "LG Monitor",
    "Canon DSLR Camera",
    "Nike Running Shoes",
    "Adidas Sneakers",
    "Puma Sports Shoes",
    "Levi's Jeans",
    "Allen Solly Shirt",
    "Woodland Jacket",
    "American Tourister Bag",
    "Wildcraft Backpack",
    "Sofa Set",
    "Dining Table",
    "Office Chair",
    "Bed Mattress",
    "Refrigerator",
    "Washing Machine",
    "Air Conditioner",
    "Microwave Oven",
    "Mixer Grinder",
    "Water Purifier",
    "Coffee Maker",
    "Protein Powder",
    "Yoga Mat",
    "Dumbbells",
    "Cricket Bat",
    "Football",
    "Face Wash",
    "Shampoo",
    "Perfume",
    "Lipstick",
    "Moisturizer",
    "Baby Diapers",
    "Pet Food",
    "Bluetooth Speaker",
    "Power Bank",
    "External Hard Disk"
]

CATEGORIES = [
    "Electronics",
    "Mobiles",
    "Computers",
    "Accessories",
    "Fashion",
    "Footwear",
    "Home Appliances",
    "Furniture",
    "Sports",
    "Beauty",
    "Health",
    "Fitness",
    "Gaming",
    "Travel",
    "Kitchen",
    "Books",
    "Baby Care",
    "Pet Supplies"
]

PAYMENT_METHODS = [
    "UPI",
    "CREDIT_CARD",
    "DEBIT_CARD",
    "NET_BANKING",
    "COD",
    "WALLET"
]

ORDER_STATUS = [
    "PLACED",
    "CONFIRMED",
    "SHIPPED",
    "DELIVERED",
    "CANCELLED",
    "RETURNED"
]


def generate_order_event():
    quantity = random.randint(1, 50)
    price = round(random.uniform(500, 500000), 2)
    order_event = {
        "event_id": str(uuid.uuid4()),
        "event_type": "ORDER_CREATED",
        "event_time": datetime.utcnow().isoformat(),
        "order_id": f"ORD-{random.randint(100000, 999999)}",
        "customer_id": f"CUST-{random.randint(1000, 9999)}",
        "customer_name": fake.name(),
        "email": fake.email(),
        "phone_number": fake.phone_number(),
        "city": fake.city(),
        "state": fake.state(),
        "country": "India",
        "pincode": fake.postcode(),
        "product_id": f"PROD-{random.randint(100, 999)}",
        "product_name": random.choice(PRODUCTS),
        "category": random.choice(CATEGORIES),
        "quantity": quantity,
        "price": price,
        "total_amount": round(quantity * price, 2),
        "payment_method": random.choice(PAYMENT_METHODS),
        "order_status": random.choice(ORDER_STATUS),
        "shipping_address": fake.address(),
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
        "is_prime_customer": random.choice([
            True,
            False
        ])
    }

    return order_event

def upload_event_to_s3(order_event):

    current_date = datetime.utcnow().strftime("%Y-%m-%d")

    file_name = (
        f"{RAW_ORDERS_PREFIX}"
        f"date={current_date}/"
        f"{order_event['event_id']}.json"
    )

    s3_client.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=file_name,
        Body=json.dumps(order_event),
        ContentType="application/json"
    )

    logger.info(f"Uploaded order event: {order_event['event_id']}")

    print(f"Uploaded: s3://{S3_BUCKET_NAME}/{file_name}")

def main():
    logger.info("Starting Order Event Producer...")

    print("Starting Order Event Producer...")
    while True:
        try:
            order_event = generate_order_event()
            upload_event_to_s3(order_event)
            time.sleep(5)

        except Exception as error:
            logger.error(f"Error occurred: {str(error)}")
            print(f"Error occurred: {str(error)}")


if __name__ == "__main__":
    main()