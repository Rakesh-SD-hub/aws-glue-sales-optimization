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


# Read raw category translation data
category_dyf = glueContext.create_dynamic_frame.from_catalog(
    database="sales_optimization_db",
    table_name="category_translation"
)

category_df = category_dyf.toDF()


# Clean and standardize category names
category_clean = (
    category_df
    .select(
        lower(trim(col("product_category_name"))).alias(
            "product_category_name"
        ),
        lower(trim(col("product_category_name_english"))).alias(
            "product_category_name_english"
        )
    )
    .dropDuplicates(["product_category_name"])
)


# Silver output location
silver_path = (
    "s3://data-engineer-sales-optimization-rakesh-2026/"
    "silver/category_translation/"
)


# Write Silver data as Parquet
(
    category_clean
    .write
    .mode("overwrite")
    .format("parquet")
    .option("compression", "snappy")
    .save(silver_path)
)


job.commit()
