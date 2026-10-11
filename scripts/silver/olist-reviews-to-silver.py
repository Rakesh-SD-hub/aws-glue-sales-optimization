import sys

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql.functions import (
    col,
    trim,
    to_timestamp
)

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)


# Read raw reviews from Glue Data Catalog
reviews_dyf = glueContext.create_dynamic_frame.from_catalog(
    database="sales_optimization_db",
    table_name="reviews"
)

reviews_df = reviews_dyf.toDF()


# Clean and standardize reviews
reviews_clean = (
    reviews_df
    .select(
        trim(col("review_id")).alias("review_id"),
        trim(col("order_id")).alias("order_id"),
        col("review_score").cast("integer").alias("review_score"),
        trim(col("review_comment_title")).alias("review_comment_title"),
        trim(col("review_comment_message")).alias("review_comment_message"),
        to_timestamp(
            trim(col("review_creation_date"))
        ).alias("review_creation_date"),
        to_timestamp(
            trim(col("review_answer_timestamp"))
        ).alias("review_answer_timestamp")
    )
    .dropDuplicates([
        "review_id",
        "order_id"
    ])
)


# Silver output location
silver_path = (
    "s3://data-engineer-sales-optimization-rakesh-2026/"
    "silver/reviews/"
)


# Write Silver data as Parquet
(
    reviews_clean
    .write
    .mode("overwrite")
    .format("parquet")
    .option("compression", "snappy")
    .save(silver_path)
)


job.commit()
