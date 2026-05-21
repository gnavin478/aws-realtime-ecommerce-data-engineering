import json
import boto3
import urllib.parse
from datetime import datetime

s3_client = boto3.client("s3")

def lambda_handler(event, context):
    print("Lambda triggered by S3 event")

    for record in event["Records"]:
        bucket_name = record["s3"]["bucket"]["name"]

        object_key = urllib.parse.unquote_plus(
            record["s3"]["object"]["key"]
        )

        print(f"New file received: s3://{bucket_name}/{object_key}")

        response = s3_client.get_object(
            Bucket=bucket_name,
            Key=object_key
        )

        file_content = response["Body"].read().decode("utf-8")
        json_data = json.loads(file_content)

        event_type = json_data.get("event_type", "UNKNOWN")
        event_id = json_data.get("event_id", "NO_EVENT_ID")

        print(f"Event ID: {event_id}")
        print(f"Event Type: {event_type}")

        processed_time = datetime.utcnow().isoformat()

        json_data["lambda_processed_time"] = processed_time
        json_data["processing_status"] = "PROCESSED"

        destination_key = object_key.replace(
            "raw/",
            "bronze/"
        )

        s3_client.put_object(
            Bucket=bucket_name,
            Key=destination_key,
            Body=json.dumps(json_data),
            ContentType="application/json"
        )

        print(f"Processed file saved to: s3://{bucket_name}/{destination_key}")

    return {
        "statusCode": 200,
        "body": json.dumps("S3 event processed successfully")
    }