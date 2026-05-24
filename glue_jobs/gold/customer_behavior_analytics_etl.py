import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

from pyspark.context import SparkContext
from pyspark.sql.functions import (
    col,
    count,
    current_timestamp,
    to_date
)

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SILVER_CUSTOMER_ACTIVITY_PATH = "s3://aws-realtime-ecommerce-pipeline/silver/customer_activity/"
GOLD_CUSTOMER_BEHAVIOR_PATH = "s3://aws-realtime-ecommerce-pipeline/gold/customer_behavior_analytics/"

customer_activity_df = (
    spark.read
    .option("recursiveFileLookup", "true")
    .parquet(SILVER_CUSTOMER_ACTIVITY_PATH)
)

print("Silver Customer Activity Count:", customer_activity_df.count())

customer_behavior_df = (
    customer_activity_df
    .withColumn("activity_date", to_date(col("event_time")))
    .groupBy(
        "activity_date",
        "customer_id",
        "activity_type",
        "device_type",
        "browser",
        "city",
        "state"
    )
    .agg(
        count("activity_id").alias("total_activities"),
        count("session_id").alias("total_sessions")
    )
    .withColumn("gold_processed_time", current_timestamp())
)

print("Customer Behavior Count:", customer_behavior_df.count())

customer_behavior_df.write.mode("append").parquet(
    GOLD_CUSTOMER_BEHAVIOR_PATH
)

print(
    f"Customer behavior analytics written successfully to "
    f"{GOLD_CUSTOMER_BEHAVIOR_PATH}"
)

job.commit()