import json
import time
import uuid
import random
from datetime import datetime

import boto3

from config.constants import (
    AWS_REGION,
    S3_BUCKET_NAME,
    RAW_INVENTORY_PREFIX
)

from config.logging_config import get_logger

logger = get_logger(
    "inventory_event_producer",
    "inventory_event_producer.log"
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
    "Refrigerator",
    "Washing Machine",
    "Air Conditioner",
    "Microwave Oven",
    "Bluetooth Speaker",
    "Power Bank"
]

WAREHOUSES = [
    "Bangalore Warehouse",
    "Chennai Warehouse",
    "Mumbai Warehouse",
    "Delhi Warehouse",
    "Hyderabad Warehouse",
    "Pune Warehouse"
]


def generate_inventory_event():
    current_stock = random.randint(0, 500)
    reorder_level = random.randint(20, 100)
    inventory_event = {
        "event_id": str(uuid.uuid4()),
        "event_type": "INVENTORY_EVENT",
        "event_time": datetime.utcnow().isoformat(),
        "inventory_id": f"INV-{random.randint(100000, 999999)}",
        "product_id": f"PROD-{random.randint(100, 999)}",
        "product_name": random.choice(PRODUCTS),
        "warehouse_id": f"WH-{random.randint(10, 99)}",
        "warehouse_name": random.choice(WAREHOUSES),
        "current_stock": current_stock,
        "reorder_level": reorder_level,
        "stock_status": "LOW_STOCK" if current_stock <= reorder_level else "AVAILABLE",
        "quantity_changed": random.randint(-50, 100),
        "change_reason": random.choice([
            "ORDER_PLACED",
            "ORDER_CANCELLED",
            "STOCK_REPLENISHED",
            "RETURN_RECEIVED",
            "DAMAGED_STOCK"
        ])
    }
    return inventory_event

def upload_event_to_s3(inventory_event):
    current_date = datetime.utcnow().strftime("%Y-%m-%d")
    file_name = (
        f"{RAW_INVENTORY_PREFIX}"
        f"date={current_date}/"
        f"{inventory_event['event_id']}.json"
    )
    s3_client.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=file_name,
        Body=json.dumps(inventory_event),
        ContentType="application/json"
    )
    logger.info(f"Uploaded inventory event: {inventory_event['event_id']}")
    print(f"Uploaded: s3://{S3_BUCKET_NAME}/{file_name}")

def main():
    logger.info("Starting Inventory Event Producer...")
    print("Starting Inventory Event Producer...")
    while True:
        try:
            inventory_event = generate_inventory_event()
            upload_event_to_s3(inventory_event)
            time.sleep(5)

        except Exception as error:
            logger.error(f"Error occurred: {str(error)}")
            print(f"Error occurred: {str(error)}")

if __name__ == "__main__":
    main()