"""
Sales Data Pipeline — PySpark
Reads raw sales CSVs, runs transformations, and writes aggregated outputs.
Designed to run on a Hadoop cluster or locally with PySpark.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType
from pyspark.sql.window import Window
import os

# ── SPARK SESSION ─────────────────────────────────────────────────────────────

def create_spark_session(app_name="SalesPipeline", master="local[*]"):
    return (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )

# ── SCHEMA ────────────────────────────────────────────────────────────────────

SALES_SCHEMA = StructType([
    StructField("order_id", StringType(), False),
    StructField("order_date", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("customer_segment", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("category", StringType(), True),
    StructField("sub_category", StringType(), True),
    StructField("region", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("discount", DoubleType(), True),
    StructField("shipping_cost", DoubleType(), True),
])

# ── EXTRACT ───────────────────────────────────────────────────────────────────

def extract(spark, path):
    return (
        spark.read
        .schema(SALES_SCHEMA)
        .option("header", "true")
        .csv(path)
    )

# ── TRANSFORM ─────────────────────────────────────────────────────────────────

def transform(df):
    df = df.withColumn("order_date", F.to_date("order_date", "yyyy-MM-dd"))
    df = df.withColumn("year", F.year("order_date"))
    df = df.withColumn("month", F.month("order_date"))
    df = df.withColumn("quarter", F.quarter("order_date"))

    # Revenue calculation
    df = df.withColumn(
        "revenue",
        F.round(F.col("quantity") * F.col("unit_price") * (1 - F.col("discount")), 2)
    )
    df = df.withColumn(
        "profit",
        F.round(F.col("revenue") - F.col("shipping_cost"), 2)
    )

    # Discount tier
    df = df.withColumn(
        "discount_tier",
        F.when(F.col("discount") == 0, "No Discount")
         .when(F.col("discount") <= 0.1, "Low (1-10%)")
         .when(F.col("discount") <= 0.3, "Medium (11-30%)")
         .otherwise("High (>30%)")
    )

    # Running revenue per customer
    window = Window.partitionBy("customer_id").orderBy("order_date").rowsBetween(Window.unboundedPreceding, 0)
    df = df.withColumn("cumulative_revenue", F.sum("revenue").over(window))

    return df.filter(F.col("revenue") > 0)


def aggregate_by_region_category(df):
    return (
        df.groupBy("year", "quarter", "region", "category")
        .agg(
            F.round(F.sum("revenue"), 2).alias("total_revenue"),
            F.round(F.sum("profit"), 2).alias("total_profit"),
            F.count("order_id").alias("num_orders"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.round(F.avg("discount"), 3).alias("avg_discount"),
        )
        .orderBy("year", "quarter", "region", "category")
    )


def top_products(df, n=20):
    return (
        df.groupBy("product_id", "category", "sub_category")
        .agg(
            F.round(F.sum("revenue"), 2).alias("total_revenue"),
            F.sum("quantity").alias("total_units_sold"),
            F.count("order_id").alias("num_orders"),
        )
        .orderBy(F.desc("total_revenue"))
        .limit(n)
    )


def customer_segments(df):
    return (
        df.groupBy("customer_id", "customer_segment")
        .agg(
            F.round(F.sum("revenue"), 2).alias("total_revenue"),
            F.count("order_id").alias("num_orders"),
            F.min("order_date").alias("first_order"),
            F.max("order_date").alias("last_order"),
        )
        .withColumn(
            "clv_tier",
            F.when(F.col("total_revenue") > 5000, "Platinum")
             .when(F.col("total_revenue") > 2000, "Gold")
             .when(F.col("total_revenue") > 500, "Silver")
             .otherwise("Bronze")
        )
    )


# ── LOAD ──────────────────────────────────────────────────────────────────────

def load(df, output_path, format="parquet", partitions=None):
    writer = df.write.mode("overwrite").format(format)
    if partitions:
        writer = writer.partitionBy(*partitions)
    writer.save(output_path)
    print(f"Saved to {output_path} ({format})")


# ── PIPELINE ──────────────────────────────────────────────────────────────────

def run_pipeline(input_path, output_dir):
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("Extracting...")
    raw = extract(spark, input_path)
    print(f"  Raw records: {raw.count():,}")

    print("Transforming...")
    transformed = transform(raw)
    print(f"  Clean records: {transformed.count():,}")

    print("Aggregating...")
    region_cat = aggregate_by_region_category(transformed)
    products = top_products(transformed)
    customers = customer_segments(transformed)

    print("Loading...")
    load(transformed, f"{output_dir}/sales_clean", partitions=["year", "region"])
    load(region_cat, f"{output_dir}/region_category_summary", format="parquet")
    load(products, f"{output_dir}/top_products", format="parquet")
    load(customers, f"{output_dir}/customer_segments", format="parquet")

    print("\n=== Region-Category Summary (sample) ===")
    region_cat.show(10, truncate=False)

    print("=== Top Products ===")
    products.show(10, truncate=False)

    spark.stop()
    print("\nPipeline complete.")


if __name__ == "__main__":
    run_pipeline(
        input_path="data/sales_raw.csv",
        output_dir="output",
    )
