
import sys

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql.functions import col, trim, lower

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Read raw products from the Glue Data Catalog
raw_products = glueContext.create_dynamic_frame.from_catalog(
    database="sales_optimization_db",
    table_name="products"
)

products_df = raw_products.toDF()

# Clean product fields
if "product_category_name" in products_df.columns:
    products_df = products_df.withColumn(
        "product_category_name",
        lower(trim(col("product_category_name")))
    )

# Remove duplicate product records
if "product_id" in products_df.columns:
    products_df = products_df.dropDuplicates(["product_id"])

# Write the Silver products dataset as Parquet
(
    products_df.write
    .mode("overwrite")
    .format("parquet")
    .save("s3://data-engineer-sales-optimization-rakesh-2026/silver/products/")
)

job.commit()
