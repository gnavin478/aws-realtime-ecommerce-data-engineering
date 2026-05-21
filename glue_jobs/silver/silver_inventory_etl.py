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

BRONZE_INVENTORY_PATH = "s3://aws-realtime-ecommerce-pipeline/bronze/inventory/"
SILVER_INVENTORY_PATH = "s3://aws-realtime-ecommerce-pipeline/silver/inventory/"

inventory_df = (
    spark.read
    .option("recursiveFileLookup", "true")
    .json(BRONZE_INVENTORY_PATH)
)

print("Bronze Inventory Count:", inventory_df.count())
inventory_df.printSchema()

silver_inventory_df = (
    inventory_df
    .dropDuplicates(["event_id"])
    .filter(col("inventory_id").isNotNull())
    .filter(col("product_id").isNotNull())
    .filter(col("current_stock").isNotNull())
    .withColumn("event_time", to_timestamp(col("event_time")))
    .withColumn("silver_processed_time", current_timestamp())
    .select(
        "event_id",
        "event_type",
        "event_time",
        "inventory_id",
        "product_id",
        "product_name",
        "warehouse_id",
        "warehouse_name",
        "current_stock",
        "reorder_level",
        "stock_status",
        "quantity_changed",
        "change_reason",
        "processing_status",
        "lambda_processed_time",
        "silver_processed_time"
    )
)

print("Silver Inventory Count:", silver_inventory_df.count())

silver_inventory_df.write.mode("append").parquet(SILVER_INVENTORY_PATH)

print(f"Silver inventory written successfully to {SILVER_INVENTORY_PATH}")

job.commit()