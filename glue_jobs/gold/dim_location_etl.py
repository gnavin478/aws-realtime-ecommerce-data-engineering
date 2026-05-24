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
GOLD_DIM_LOCATION_PATH = "s3://aws-realtime-ecommerce-pipeline/gold/dim_location/"

orders_df = (
    spark.read
    .option("recursiveFileLookup", "true")
    .parquet(SILVER_ORDERS_PATH)
)

print("Silver Orders Count:", orders_df.count())

dim_location_df = (
    orders_df
    .select(
        "city",
        "state",
        "country"
    )
    .dropDuplicates()
    .filter(col("state").isNotNull())
    .withColumn("gold_processed_time", current_timestamp())
)

print("Dim Location Count:", dim_location_df.count())

dim_location_df.write.mode("append").parquet(
    GOLD_DIM_LOCATION_PATH
)

print(
    f"Dim location written successfully to "
    f"{GOLD_DIM_LOCATION_PATH}"
)

job.commit()