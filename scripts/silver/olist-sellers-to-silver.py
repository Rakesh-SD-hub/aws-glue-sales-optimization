import sys

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql.functions import (
    col,
    trim,
    upper
)

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)


# Read raw sellers from Glue Data Catalog
sellers_dyf = glueContext.create_dynamic_frame.from_catalog(
    database="sales_optimization_db",
    table_name="sellers"
)

sellers_df = sellers_dyf.toDF()


# Clean and standardize sellers
sellers_clean = (
    sellers_df
    .select(
        trim(col("seller_id")).alias("seller_id"),
        trim(col("seller_zip_code_prefix")).cast("integer").alias(
            "seller_zip_code_prefix"
        ),
        trim(col("seller_city")).alias("seller_city"),
        upper(trim(col("seller_state"))).alias("seller_state")
    )
    .dropDuplicates(["seller_id"])
)


# Silver output location
silver_path = (
    "s3://data-engineer-sales-optimization-rakesh-2026/"
    "silver/sellers/"
)


# Write Silver data as Parquet
(
    sellers_clean
    .write
    .mode("overwrite")
    .format("parquet")
    .option("compression", "snappy")
    .save(silver_path)
)


job.commit()
