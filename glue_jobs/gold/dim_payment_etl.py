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

SILVER_PAYMENTS_PATH = "s3://aws-realtime-ecommerce-pipeline/silver/payments/"
GOLD_DIM_PAYMENT_PATH = "s3://aws-realtime-ecommerce-pipeline/gold/dim_payment/"

payments_df = (
    spark.read
    .option("recursiveFileLookup", "true")
    .parquet(SILVER_PAYMENTS_PATH)
)

print("Silver Payments Count:", payments_df.count())

dim_payment_df = (
    payments_df
    .select(
        "payment_method",
        "payment_gateway",
        "bank_name",
        "currency"
    )
    .dropDuplicates()
    .filter(col("payment_method").isNotNull())
    .withColumn("gold_processed_time", current_timestamp())
)

print("Dim Payment Count:", dim_payment_df.count())

dim_payment_df.write.mode("append").parquet(GOLD_DIM_PAYMENT_PATH)

print(f"Dim payment written successfully to {GOLD_DIM_PAYMENT_PATH}")

job.commit()