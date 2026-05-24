import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

from pyspark.context import SparkContext
from pyspark.sql.functions import (
    col,
    sum,
    count,
    avg,
    current_timestamp
)

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SILVER_INVENTORY_PATH = "s3://aws-realtime-ecommerce-pipeline/silver/inventory/"
GOLD_INVENTORY_SUMMARY_PATH = "s3://aws-realtime-ecommerce-pipeline/gold/inventory_summary/"

inventory_df = (
    spark.read
    .option("recursiveFileLookup", "true")
    .parquet(SILVER_INVENTORY_PATH)
)

print("Silver Inventory Count:", inventory_df.count())

inventory_summary_df = (
    inventory_df
    .groupBy(
        "product_id",
        "product_name",
        "warehouse_id",
        "warehouse_name",
        "stock_status"
    )
    .agg(
        count("inventory_id").alias("total_inventory_events"),
        sum("current_stock").alias("total_current_stock"),
        avg("current_stock").alias("average_current_stock"),
        sum("quantity_changed").alias("total_quantity_changed")
    )
    .withColumn("gold_processed_time", current_timestamp())
)

print("Inventory Summary Count:", inventory_summary_df.count())

inventory_summary_df.write.mode("append").parquet(
    GOLD_INVENTORY_SUMMARY_PATH
)

print(
    f"Inventory summary written successfully to "
    f"{GOLD_INVENTORY_SUMMARY_PATH}"
)

job.commit()