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

BRONZE_CUSTOMER_ACTIVITY_PATH = "s3://aws-realtime-ecommerce-pipeline/bronze/customer_activity/"
SILVER_CUSTOMER_ACTIVITY_PATH = "s3://aws-realtime-ecommerce-pipeline/silver/customer_activity/"

customer_activity_df = (
    spark.read
    .option("recursiveFileLookup", "true")
    .json(BRONZE_CUSTOMER_ACTIVITY_PATH)
)

print("Bronze Customer Activity Count:", customer_activity_df.count())
customer_activity_df.printSchema()

silver_customer_activity_df = (
    customer_activity_df
    .dropDuplicates(["event_id"])
    .filter(col("activity_id").isNotNull())
    .filter(col("customer_id").isNotNull())
    .filter(col("activity_type").isNotNull())
    .withColumn("event_time", to_timestamp(col("event_time")))
    .withColumn("silver_processed_time", current_timestamp())
    .select(
        "event_id",
        "event_type",
        "event_time",
        "activity_id",
        "customer_id",
        "customer_name",
        "email",
        "activity_type",
        "session_id",
        "search_keyword",
        "page_url",
        "referrer_url",
        "device_type",
        "browser",
        "ip_address",
        "city",
        "state",
        "country",
        "processing_status",
        "lambda_processed_time",
        "silver_processed_time"
    )
)

print("Silver Customer Activity Count:", silver_customer_activity_df.count())

silver_customer_activity_df.write.mode("append").parquet(
    SILVER_CUSTOMER_ACTIVITY_PATH
)

print(
    f"Silver customer activity written successfully to "
    f"{SILVER_CUSTOMER_ACTIVITY_PATH}"
)

job.commit()