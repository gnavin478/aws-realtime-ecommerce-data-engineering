import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

from pyspark.context import SparkContext
from pyspark.sql.functions import col, to_timestamp, current_timestamp

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

BRONZE_PAYMENTS_PATH = "s3://aws-realtime-ecommerce-pipeline/bronze/payments/"
SILVER_PAYMENTS_PATH = "s3://aws-realtime-ecommerce-pipeline/silver/payments/"

payments_df = (
    spark.read
    .option("recursiveFileLookup", "true")
    .json(BRONZE_PAYMENTS_PATH)
)

print("Bronze Payments Count:", payments_df.count())
payments_df.printSchema()

silver_payments_df = (
    payments_df
    .dropDuplicates(["event_id"])
    .filter(col("payment_id").isNotNull())
    .filter(col("order_id").isNotNull())
    .filter(col("customer_id").isNotNull())
    .filter(col("payment_amount") > 0)
    .withColumn("event_time", to_timestamp(col("event_time")))
    .withColumn("silver_processed_time", current_timestamp())
    .select(
        "event_id",
        "event_type",
        "event_time",
        "payment_id",
        "order_id",
        "customer_id",
        "customer_name",
        "email",
        "payment_method",
        "payment_status",
        "payment_amount",
        "currency",
        "bank_name",
        "transaction_reference",
        "failure_reason",
        "payment_gateway",
        "device_type",
        "ip_address",
        "processing_status",
        "lambda_processed_time",
        "silver_processed_time"
    )
)

print("Silver Payments Count:", silver_payments_df.count())

silver_payments_df.write.mode("append").parquet(SILVER_PAYMENTS_PATH)

print(f"Silver payments written successfully to {SILVER_PAYMENTS_PATH}")

job.commit()