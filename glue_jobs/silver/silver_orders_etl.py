import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

from pyspark.context import SparkContext
from pyspark.sql.functions import (
    col,
    to_timestamp,
    current_timestamp
)

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

BRONZE_ORDERS_PATH = "s3://aws-realtime-ecommerce-pipeline/bronze/orders/"
SILVER_ORDERS_PATH = "s3://aws-realtime-ecommerce-pipeline/silver/orders/"

orders_df = (
    spark.read
    .option("recursiveFileLookup", "true")
    .json(BRONZE_ORDERS_PATH)
)

print("Bronze Orders Count:", orders_df.count())
orders_df.printSchema()

silver_orders_df = (
    orders_df
    .dropDuplicates(["event_id"])
    .filter(col("order_id").isNotNull())
    .filter(col("customer_id").isNotNull())
    .filter(col("total_amount") > 0)
    .withColumn("event_time", to_timestamp(col("event_time")))
    .withColumn("silver_processed_time", current_timestamp())
    .select(
        "event_id",
        "event_type",
        "event_time",
        "order_id",
        "customer_id",
        "customer_name",
        "email",
        "city",
        "state",
        "country",
        "product_id",
        "product_name",
        "category",
        "quantity",
        "price",
        "total_amount",
        "payment_method",
        "order_status",
        "device_type",
        "browser",
        "is_prime_customer",
        "processing_status",
        "lambda_processed_time",
        "silver_processed_time"
    )
)

print("Silver Orders Count:", silver_orders_df.count())
silver_orders_df.write.mode("overwrite").parquet(SILVER_ORDERS_PATH)
print(f"Silver orders written successfully to {SILVER_ORDERS_PATH}")

job.commit()