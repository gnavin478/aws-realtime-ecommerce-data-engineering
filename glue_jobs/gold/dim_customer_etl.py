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
GOLD_DIM_CUSTOMER_PATH = "s3://aws-realtime-ecommerce-pipeline/gold/dim_customer/"

orders_df = (
    spark.read
    .option("recursiveFileLookup", "true")
    .parquet(SILVER_ORDERS_PATH)
)

print("Silver Orders Count:", orders_df.count())

dim_customer_df = (
    orders_df
    .select(
        "customer_id",
        "customer_name",
        "email",
        "city",
        "state",
        "country",
        "is_prime_customer"
    )
    .dropDuplicates(["customer_id"])
    .filter(col("customer_id").isNotNull())
    .withColumn("gold_processed_time", current_timestamp())
)

print("Dim Customer Count:", dim_customer_df.count())

dim_customer_df.write.mode("append").parquet(GOLD_DIM_CUSTOMER_PATH)

print(f"Dim customer written successfully to {GOLD_DIM_CUSTOMER_PATH}")

job.commit()