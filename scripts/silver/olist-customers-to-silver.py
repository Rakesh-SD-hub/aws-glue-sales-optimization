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
# 2. Read customers from Glue Data Catalog
# ---------------------------------------------------------

customers_dyf = glueContext.create_dynamic_frame.from_catalog(
    database="sales_optimization_db",
    table_name="customers"
)

customers_df = customers_dyf.toDF()


# ---------------------------------------------------------
# 3. Clean customer data
# ---------------------------------------------------------

customers_clean = (
    customers_df
    .select(
        trim(col("customer_id")).alias("customer_id"),
        trim(col("customer_unique_id")).alias("customer_unique_id"),
        trim(col("customer_zip_code_prefix")).cast("int").alias(
            "customer_zip_code_prefix"
        ),
        trim(col("customer_city")).alias("customer_city"),
        upper(trim(col("customer_state"))).alias("customer_state")
    )
    .dropDuplicates(["customer_id"])
)


# ---------------------------------------------------------
# 4. Write Silver data as Parquet
# ---------------------------------------------------------

silver_path = (
    "s3://data-engineer-sales-optimization-rakesh-2026/"
    "silver/customers/"
)

(
    customers_clean
    .write
    .mode("overwrite")
    .format("parquet")
    .option("compression", "snappy")
    .save(silver_path)
)


# ---------------------------------------------------------
# 5. Finish job
# ---------------------------------------------------------

job.commit()
