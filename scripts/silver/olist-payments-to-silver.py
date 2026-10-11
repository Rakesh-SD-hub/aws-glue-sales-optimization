import sys

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql.functions import (
    col,
    trim
)

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)


# Read raw payments data from Glue Data Catalog
payments_dyf = glueContext.create_dynamic_frame.from_catalog(
    database="sales_optimization_db",
    table_name="payments"
)

payments_df = payments_dyf.toDF()


# Clean and standardize payments
payments_clean = (
    payments_df
    .select(
        trim(col("order_id")).alias("order_id"),
        col("payment_sequential").cast("integer").alias("payment_sequential"),
        trim(col("payment_type")).alias("payment_type"),
        col("payment_installments").cast("integer").alias("payment_installments"),
        col("payment_value").cast("double").alias("payment_value")
    )
    .dropDuplicates([
        "order_id",
        "payment_sequential"
    ])
)


# Silver output location
silver_path = (
    "s3://data-engineer-sales-optimization-rakesh-2026/"
    "silver/payments/"
)


# Write Silver data as Parquet
(
    payments_clean
    .write
    .mode("overwrite")
    .format("parquet")
    .option("compression", "snappy")
    .save(silver_path)
)


job.commit()
