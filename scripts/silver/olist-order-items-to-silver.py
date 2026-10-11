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


# ---------------------------------------------------------
# 1. Initialize Glue job
# ---------------------------------------------------------

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)


# ---------------------------------------------------------
# 2. Read RAW order_items table
# ---------------------------------------------------------

order_items_dyf = glueContext.create_dynamic_frame.from_catalog(
    database="sales_optimization_db",
    table_name="order_items"
)

order_items_df = order_items_dyf.toDF()


# ---------------------------------------------------------
# 3. Clean and transform
# ---------------------------------------------------------

order_items_clean = (
    order_items_df
    .select(
        trim(col("order_id")).alias("order_id"),

        col("order_item_id")
        .cast("integer")
        .alias("order_item_id"),

        trim(col("product_id")).alias("product_id"),

        trim(col("seller_id")).alias("seller_id"),

        to_timestamp(
            trim(col("shipping_limit_date"))
        ).alias("shipping_limit_date"),

        col("price")
        .cast("double")
        .alias("price"),

        col("freight_value")
        .cast("double")
        .alias("freight_value")
    )

    # Remove duplicate order-item records
    .dropDuplicates(["order_id", "order_item_id"])
)


# ---------------------------------------------------------
# 4. Add useful date columns
# ---------------------------------------------------------

order_items_clean = (
    order_items_clean
    .withColumn(
        "shipping_limit_date_only",
        to_date(col("shipping_limit_date"))
    )
    .withColumn(
        "shipping_year",
        year(col("shipping_limit_date"))
    )
    .withColumn(
        "shipping_month",
        month(col("shipping_limit_date"))
    )
)


# ---------------------------------------------------------
# 5. Write SILVER data as Parquet
# ---------------------------------------------------------

silver_path = (
    "s3://data-engineer-sales-optimization-rakesh-2026/"
    "silver/order_items/"
)

(
    order_items_clean
    .write
    .mode("overwrite")
    .format("parquet")
    .option("compression", "snappy")
    .partitionBy(
        "shipping_year",
        "shipping_month"
    )
    .save(silver_path)
)


# ---------------------------------------------------------
# 6. Finish job
# ---------------------------------------------------------

job.commit()
