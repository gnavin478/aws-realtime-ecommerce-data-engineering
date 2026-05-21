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

BRONZE_DELIVERY_PATH = "s3://aws-realtime-ecommerce-pipeline/bronze/delivery/"
SILVER_DELIVERY_PATH = "s3://aws-realtime-ecommerce-pipeline/silver/delivery/"

delivery_df = (
    spark.read
    .option("recursiveFileLookup", "true")
    .json(BRONZE_DELIVERY_PATH)
)

print("Bronze Delivery Count:", delivery_df.count())
delivery_df.printSchema()

silver_delivery_df = (
    delivery_df
    .dropDuplicates(["event_id"])
    .filter(col("delivery_id").isNotNull())
    .filter(col("order_id").isNotNull())
    .filter(col("customer_id").isNotNull())
    .withColumn("event_time", to_timestamp(col("event_time")))
    .withColumn("silver_processed_time", current_timestamp())
    .select(
        "event_id",
        "event_type",
        "event_time",
        "delivery_id",
        "order_id",
        "customer_id",
        "customer_name",
        "phone_number",
        "delivery_status",
        "delivery_partner",
        "tracking_number",
        "shipping_address",
        "city",
        "state",
        "country",
        "pincode",
        "estimated_delivery_date",
        "delivery_charges",
        "is_express_delivery",
        "processing_status",
        "lambda_processed_time",
        "silver_processed_time"
    )
)

print("Silver Delivery Count:", silver_delivery_df.count())

silver_delivery_df.write.mode("append").parquet(SILVER_DELIVERY_PATH)

print(f"Silver delivery written successfully to {SILVER_DELIVERY_PATH}")

job.commit()