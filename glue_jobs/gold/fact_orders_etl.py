import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

from pyspark.context import SparkContext
from pyspark.sql.functions import (
    col,
    current_timestamp
)

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SILVER_ORDERS_PATH = "s3://aws-realtime-ecommerce-pipeline/silver/orders/"

GOLD_FACT_ORDERS_PATH = (
    "s3://aws-realtime-ecommerce-pipeline/gold/fact_orders/"
)

orders_df = (
    spark.read
    .option("recursiveFileLookup", "true")
    .parquet(SILVER_ORDERS_PATH)
)

print("Silver Orders Count:", orders_df.count())

fact_orders_df = (
    orders_df
    .select(
        "event_id",
        "event_time",
        "order_id",
        "customer_id",
        "product_id",
        "payment_method",
        "order_status",
        "quantity",
        "price",
        "total_amount",
        "city",
        "state",
        "country",
        "device_type",
        "browser",
        "is_prime_customer"
    )
    .filter(col("order_id").isNotNull())
    .withColumn(
        "gold_processed_time",
        current_timestamp()
    )
)

print("Fact Orders Count:", fact_orders_df.count())

fact_orders_df.write.mode("append").parquet(
    GOLD_FACT_ORDERS_PATH
)

print(
    f"Fact orders written successfully to "
    f"{GOLD_FACT_ORDERS_PATH}"
)

job.commit()