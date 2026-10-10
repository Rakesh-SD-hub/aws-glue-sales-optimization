import sys

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql.functions import (
    col,
    trim,
    to_timestamp,
    to_date,
    year,
    month
)


# Get job parameters
args = getResolvedOptions(sys.argv, ["JOB_NAME"])


# Initialize Spark and Glue
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)


# ---------------------------------------------------------
# 1. Read RAW orders data from Glue Data Catalog
# ---------------------------------------------------------

orders_dyf = glueContext.create_dynamic_frame.from_catalog(
    database="sales_optimization_db",
    table_name="orders"
)

orders_df = orders_dyf.toDF()


# ---------------------------------------------------------
# 2. Clean and transform the orders data
# ---------------------------------------------------------

orders_clean = (
    orders_df
    .select(
        trim(col("order_id")).alias("order_id"),
        trim(col("customer_id")).alias("customer_id"),
        trim(col("order_status")).alias("order_status"),

        to_timestamp(
            trim(col("order_purchase_timestamp"))
        ).alias("order_purchase_timestamp"),

        to_timestamp(
            trim(col("order_approved_at"))
        ).alias("order_approved_at"),

        to_timestamp(
            trim(col("order_delivered_carrier_date"))
        ).alias("order_delivered_carrier_date"),

        to_timestamp(
            trim(col("order_delivered_customer_date"))
        ).alias("order_delivered_customer_date"),

        to_timestamp(
            trim(col("order_estimated_delivery_date"))
        ).alias("order_estimated_delivery_date")
    )

    # Remove rows without an order ID
    .filter(col("order_id").isNotNull())

    # Remove duplicate orders
    .dropDuplicates(["order_id"])

    # Create a date column for analytics
    .withColumn(
        "order_purchase_date",
        to_date(col("order_purchase_timestamp"))
    )

    # Create partition columns
    .withColumn(
        "purchase_year",
        year(col("order_purchase_timestamp"))
    )

    .withColumn(
        "purchase_month",
        month(col("order_purchase_timestamp"))
    )
)


# ---------------------------------------------------------
# 3. Write cleaned data to SILVER as Parquet
# ---------------------------------------------------------

silver_path = (
    "s3://data-engineer-sales-optimization-rakesh-2026/"
    "silver/orders/"
)

(
    orders_clean
    .write
    .mode("overwrite")
    .format("parquet")
    .option("compression", "snappy")
    .partitionBy("purchase_year", "purchase_month")
    .save(silver_path)
)


# Finish job
job.commit()
