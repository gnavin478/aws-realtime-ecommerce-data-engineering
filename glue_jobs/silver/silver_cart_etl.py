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

BRONZE_CART_PATH = "s3://aws-realtime-ecommerce-pipeline/bronze/cart/"
SILVER_CART_PATH = "s3://aws-realtime-ecommerce-pipeline/silver/cart/"

cart_df = (
    spark.read
    .option("recursiveFileLookup", "true")
    .json(BRONZE_CART_PATH)
)

print("Bronze Cart Count:", cart_df.count())
cart_df.printSchema()

silver_cart_df = (
    cart_df
    .dropDuplicates(["event_id"])
    .filter(col("cart_id").isNotNull())
    .filter(col("customer_id").isNotNull())
    .filter(col("product_id").isNotNull())
    .withColumn("event_time", to_timestamp(col("event_time")))
    .withColumn("silver_processed_time", current_timestamp())
    .select(
        "event_id",
        "event_type",
        "event_time",
        "cart_id",
        "customer_id",
        "customer_name",
        "email",
        "product_id",
        "product_name",
        "cart_action",
        "quantity",
        "price",
        "total_amount",
        "device_type",
        "browser",
        "session_id",
        "is_logged_in",
        "processing_status",
        "lambda_processed_time",
        "silver_processed_time"
    )
)

print("Silver Cart Count:", silver_cart_df.count())

silver_cart_df.write.mode("append").parquet(SILVER_CART_PATH)

print(f"Silver cart written successfully to {SILVER_CART_PATH}")

job.commit()