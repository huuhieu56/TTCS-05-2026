"""Transform: dim_products — validate and enforce schema for product dimension.

If cost_price already exists in the source data (data_source follows the
designed schema), this just validates and passes through. Falls back to
deriving cost_price from order_items when the column is missing.
"""

from __future__ import annotations

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F


def transform_products(spark: SparkSession, raw_bucket: str, clean_bucket: str) -> DataFrame:
    products_df = spark.read.csv(
        f"s3a://{raw_bucket}/sql/products.csv", header=True, inferSchema=True,
    )

    has_cost_price = "cost_price" in products_df.columns

    if has_cost_price:
        result = products_df.select(
            F.col("product_id"),
            F.col("product_name"),
            F.col("category"),
            F.col("cost_price").cast("decimal(10,2)"),
        )
    else:
        order_items_df = spark.read.csv(
            f"s3a://{raw_bucket}/sql/order_items.csv", header=True, inferSchema=True,
        )
        price_col = "unit_price" if "unit_price" in order_items_df.columns else "price"
        avg_price_df = (
            order_items_df
            .groupBy("product_id")
            .agg(F.round(F.avg(price_col) * 0.7, 2).alias("cost_price"))
        )
        result = (
            products_df
            .join(avg_price_df, on="product_id", how="left")
            .select(
                F.col("product_id"),
                F.col("product_name"),
                F.col("category"),
                F.coalesce(F.col("cost_price"), F.lit(0.00)).cast("decimal(10,2)").alias("cost_price"),
            )
        )

    result.write.mode("overwrite").parquet(f"s3a://{clean_bucket}/dim_products/")
    return result
