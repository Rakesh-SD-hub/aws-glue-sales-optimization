import sys

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql.functions import (
    col,
    trim,
    lower,
    to_date,
    year,
    month,
    sum as spark_sum,
    avg,
    collect_set,
    concat_ws
)

# ---------------------------------------------------------
# Initialize Glue job
# ---------------------------------------------------------

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

database_name = "sales_optimization_db"


# ---------------------------------------------------------
# Read tables from Glue Data Catalog
# ---------------------------------------------------------

def read_catalog_table(table_name):
    print(f"Reading table: {database_name}.{table_name}")

    dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
        database=database_name,
        table_name=table_name
    )

    dataframe = dynamic_frame.toDF()
    print(f"Loaded table: {table_name}")

    return dataframe


orders = read_catalog_table("silver_orders")
order_items = read_catalog_table("silver_order_items")
products = read_catalog_table("silver_products")
customers = read_catalog_table("silver_customers")
sellers = read_catalog_table("silver_sellers")
payments = read_catalog_table("silver_payments")
reviews = read_catalog_table("silver_reviews")
category_translation = read_catalog_table(
    "silver_category_translation"
)


# ---------------------------------------------------------
# Aggregate payments to one row per order
# ---------------------------------------------------------

payments_agg = (
    payments
    .groupBy("order_id")
    .agg(
        spark_sum("payment_value").alias("order_payment_total"),
        concat_ws(
            ", ",
            collect_set("payment_type")
        ).alias("payment_types")
    )
)


# ---------------------------------------------------------
# Aggregate reviews to one row per order
# ---------------------------------------------------------

reviews_agg = (
    reviews
    .groupBy("order_id")
    .agg(
        avg("review_score").alias("average_review_score")
    )
)


# ---------------------------------------------------------
# Clean category translation data
# ---------------------------------------------------------

category_translation_clean = (
    category_translation
    .select(
        lower(
            trim(col("product_category_name"))
        ).alias("product_category_name"),
        col("product_category_name_english")
    )
    .dropDuplicates(["product_category_name"])
)


# ---------------------------------------------------------
# Join Silver datasets
# ---------------------------------------------------------

gold_sales = (
    orders.alias("o")

    .join(
        order_items.alias("oi"),
        col("o.order_id") == col("oi.order_id"),
        "inner"
    )

    .join(
        products.alias("p"),
        col("oi.product_id") == col("p.product_id"),
        "left"
    )

    .join(
        customers.alias("c"),
        col("o.customer_id") == col("c.customer_id"),
        "left"
    )

    .join(
        sellers.alias("s"),
        col("oi.seller_id") == col("s.seller_id"),
        "left"
    )

    .join(
        payments_agg.alias("pay"),
        col("o.order_id") == col("pay.order_id"),
        "left"
    )

    .join(
        reviews_agg.alias("r"),
        col("o.order_id") == col("r.order_id"),
        "left"
    )

    .join(
        category_translation_clean.alias("ct"),
        lower(trim(col("p.product_category_name")))
        == col("ct.product_category_name"),
        "left"
    )
)


# ---------------------------------------------------------
# Select business-ready Gold columns
# ---------------------------------------------------------

gold_sales = (
    gold_sales
    .select(
        col("o.order_id").alias("order_id"),
        col("o.customer_id").alias("customer_id"),
        col("oi.order_item_id").alias("order_item_id"),
        col("oi.product_id").alias("product_id"),
        col("oi.seller_id").alias("seller_id"),

        col("o.order_status").alias("order_status"),

        col("o.order_purchase_timestamp").alias(
            "order_purchase_timestamp"
        ),

        to_date(
            col("o.order_purchase_timestamp")
        ).alias("order_purchase_date"),

        col("c.customer_city").alias("customer_city"),
        col("c.customer_state").alias("customer_state"),

        col("s.seller_city").alias("seller_city"),
        col("s.seller_state").alias("seller_state"),

        col("p.product_category_name").alias(
            "product_category_name"
        ),

        col("ct.product_category_name_english").alias(
            "product_category_name_english"
        ),

        col("oi.price").alias("price"),
        col("oi.freight_value").alias("freight_value"),

        (
            col("oi.price") + col("oi.freight_value")
        ).alias("item_sales_value"),

        col("pay.order_payment_total").alias(
            "order_payment_total"
        ),

        col("pay.payment_types").alias("payment_types"),

        col("r.average_review_score").alias(
            "average_review_score"
        )
    )
    .dropDuplicates([
        "order_id",
        "order_item_id"
    ])
)


# ---------------------------------------------------------
# Add year and month partition columns
# ---------------------------------------------------------

gold_sales = (
    gold_sales
    .withColumn(
        "order_year",
        year(col("order_purchase_date"))
    )
    .withColumn(
        "order_month",
        month(col("order_purchase_date"))
    )
)


# ---------------------------------------------------------
# Write Gold dataset to S3
# ---------------------------------------------------------

gold_path = (
    "s3://data-engineer-sales-optimization-rakesh-2026/"
    "gold/sales/"
)

print(f"Writing Gold sales data to: {gold_path}")

(
    gold_sales
    .write
    .mode("overwrite")
    .format("parquet")
    .option("compression", "snappy")
    .partitionBy("order_year", "order_month")
    .save(gold_path)
)

# ---------------------------------------------------------
# Commit Glue job
# ---------------------------------------------------------

job.commit()

print("Gold sales ETL job completed successfully.")
