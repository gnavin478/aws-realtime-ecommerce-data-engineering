import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

from pyspark.context import SparkContext
from pyspark.sql.functions import (
    col,
    current_timestamp,
    year,
    month,
    dayofmonth,
    dayofweek,
    weekofyear,
    quarter,
    date_format,
    to_date
)

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SILVER_ORDERS_PATH = "s3://aws-realtime-ecommerce-pipeline/silver/orders/"
GOLD_DIM_DATE_PATH = "s3://aws-realtime-ecommerce-pipeline/gold/dim_date/"

orders_df = (
    spark.read
    .option("recursiveFileLookup", "true")
    .parquet(SILVER_ORDERS_PATH)
)

print("Silver Orders Count:", orders_df.count())

dim_date_df = (
    orders_df
    .withColumn("order_date", to_date(col("event_time")))
    .select("order_date")
    .dropDuplicates()
    .filter(col("order_date").isNotNull())
    .withColumn("year", year(col("order_date")))
    .withColumn("month", month(col("order_date")))
    .withColumn("day", dayofmonth(col("order_date")))
    .withColumn("day_of_week", dayofweek(col("order_date")))
    .withColumn("week_of_year", weekofyear(col("order_date")))
    .withColumn("quarter", quarter(col("order_date")))
    .withColumn(
        "month_name",
        date_format(col("order_date"), "MMMM")
    )
    .withColumn(
        "day_name",
        date_format(col("order_date"), "EEEE")
    )
    .withColumn("gold_processed_time", current_timestamp())
)

print("Dim Date Count:", dim_date_df.count())

dim_date_df.write.mode("append").parquet(
    GOLD_DIM_DATE_PATH
)

print(
    f"Dim date written successfully to "
    f"{GOLD_DIM_DATE_PATH}"
)

job.commit()