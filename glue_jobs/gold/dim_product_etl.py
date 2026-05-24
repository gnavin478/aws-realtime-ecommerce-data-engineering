import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

from pyspark.context import SparkContext
from pyspark.sql.functions import col, current_timestamp

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SILVER_ORDERS_PATH = "s3://aws-realtime-ecommerce-pipeline/silver/orders/"
GOLD_DIM_PRODUCT_PATH = "s3://aws-realtime-ecommerce-pipeline/gold/dim_product/"

orders_df = (
    spark.read
    .option("recursiveFileLookup", "true")
    .parquet(SILVER_ORDERS_PATH)
)

print("Silver Orders Count:", orders_df.count())

dim_product_df = (
    orders_df
    .select(
        "product_id",
        "product_name",
        "category",
        "price"
    )
    .dropDuplicates(["product_id"])
    .filter(col("product_id").isNotNull())
    .withColumn("gold_processed_time", current_timestamp())
)

print("Dim Product Count:", dim_product_df.count())

dim_product_df.write.mode("append").parquet(GOLD_DIM_PRODUCT_PATH)

print(f"Dim product written successfully to {GOLD_DIM_PRODUCT_PATH}")

job.commit()