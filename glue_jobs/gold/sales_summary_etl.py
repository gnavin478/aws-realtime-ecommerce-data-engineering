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
    current_timestamp,
    to_date
)

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SILVER_ORDERS_PATH = "s3://aws-realtime-ecommerce-pipeline/silver/orders/"
GOLD_SALES_SUMMARY_PATH = "s3://aws-realtime-ecommerce-pipeline/gold/sales_summary/"

orders_df = (
    spark.read
    .option("recursiveFileLookup", "true")
    .parquet(SILVER_ORDERS_PATH)
)

print("Silver Orders Count:", orders_df.count())
orders_df.printSchema()

sales_summary_df = (
    orders_df
    .withColumn("order_date", to_date(col("event_time")))
    .groupBy(
        "order_date",
        "category",
        "state",
        "payment_method",
        "order_status"
    )
    .agg(
        count("order_id").alias("total_orders"),
        sum("quantity").alias("total_quantity_sold"),
        sum("total_amount").alias("total_revenue"),
        avg("total_amount").alias("average_order_value")
    )
    .withColumn("gold_processed_time", current_timestamp())
)

print("Sales Summary Count:", sales_summary_df.count())

sales_summary_df.write.mode("append").parquet(
    GOLD_SALES_SUMMARY_PATH
)

print(f"Sales summary written successfully to {GOLD_SALES_SUMMARY_PATH}")

job.commit()