import sys

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql.functions import (
    col,
    trim,
    lower
)

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)


# Read raw geolocation data
geolocation_dyf = glueContext.create_dynamic_frame.from_catalog(
    database="sales_optimization_db",
    table_name="geolocation"
)

geolocation_df = geolocation_dyf.toDF()


# Clean and standardize geolocation
geolocation_clean = (
    geolocation_df
    .select(
        trim(col("geolocation_zip_code_prefix")).cast("integer").alias(
            "geolocation_zip_code_prefix"
        ),
        col("geolocation_lat").cast("double").alias("geolocation_lat"),
        col("geolocation_lng").cast("double").alias("geolocation_lng"),
        lower(trim(col("geolocation_city"))).alias("geolocation_city"),
        trim(col("geolocation_state")).alias("geolocation_state")
    )
    .dropDuplicates()
)


# Silver output location
silver_path = (
    "s3://data-engineer-sales-optimization-rakesh-2026/"
    "silver/geolocation/"
)


# Write Silver data as Parquet
(
    geolocation_clean
    .write
    .mode("overwrite")
    .format("parquet")
    .option("compression", "snappy")
    .save(silver_path)
)


job.commit()
