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

SILVER_PAYMENTS_PATH = (
    "s3://aws-realtime-ecommerce-pipeline/silver/payments/"
)

GOLD_FACT_PAYMENTS_PATH = (
    "s3://aws-realtime-ecommerce-pipeline/gold/fact_payments/"
)

payments_df = (
    spark.read
    .option("recursiveFileLookup", "true")
    .parquet(SILVER_PAYMENTS_PATH)
)

print("Silver Payments Count:", payments_df.count())

fact_payments_df = (
    payments_df
    .select(
        "event_id",
        "event_time",
        "payment_id",
        "order_id",
        "customer_id",
        "payment_method",
        "payment_status",
        "payment_amount",
        "currency",
        "bank_name",
        "transaction_reference",
        "failure_reason",
        "payment_gateway",
        "device_type",
        "ip_address"
    )
    .filter(col("payment_id").isNotNull())
    .withColumn(
        "gold_processed_time",
        current_timestamp()
    )
)

print("Fact Payments Count:", fact_payments_df.count())

fact_payments_df.write.mode("append").parquet(
    GOLD_FACT_PAYMENTS_PATH
)

print(
    f"Fact payments written successfully to "
    f"{GOLD_FACT_PAYMENTS_PATH}"
)

job.commit()