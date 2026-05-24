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

SILVER_ORDERS_PATH = "s3://aws-realtime-ecommerce-pipeline/silver/orders/"
GOLD_TOP_PRODUCTS_PATH = "s3://aws-realtime-ecommerce-pipeline/gold/top_products_analytics/"

orders_df = (
    spark.read
    .option("recursiveFileLookup", "true")
    .parquet(SILVER_ORDERS_PATH)
)

print("Silver Orders Count:", orders_df.count())
orders_df.printSchema()

top_products_df = (
    orders_df
    .groupBy(
        "product_id",
        "product_name",
        "category"
    )
    .agg(
        count("order_id").alias("total_orders"),
        sum("quantity").alias("total_quantity_sold"),
        sum("total_amount").alias("total_revenue"),
        avg("price").alias("average_product_price")
    )
    .withColumn("gold_processed_time", current_timestamp())
)

print("Top Products Count:", top_products_df.count())

top_products_df.write.mode("append").parquet(
    GOLD_TOP_PRODUCTS_PATH
)

print(
    f"Top products analytics written successfully to "
    f"{GOLD_TOP_PRODUCTS_PATH}"
)

job.commit()