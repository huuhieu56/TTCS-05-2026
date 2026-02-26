"""Transform: fact_order_items — derive item_id, enforce column types.

Handles both pre-mapped data (item_id already exists, column named
unit_price) and raw Olist data (needs item_id derivation, column
named price).
"""

from __future__ import annotations

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F


def transform_order_items(spark: SparkSession, raw_bucket: str, clean_bucket: str) -> DataFrame:
    raw_df = spark.read.csv(
        f"s3a://{raw_bucket}/sql/order_items.csv", header=True, inferSchema=True,
    )

    has_item_id = "item_id" in raw_df.columns
    price_col = "unit_price" if "unit_price" in raw_df.columns else "price"

    if has_item_id:
        item_id_expr = F.col("item_id")
    else:
        seq_col = "order_item_id" if "order_item_id" in raw_df.columns else "item_id"
        item_id_expr = F.concat_ws("_", F.col("order_id"), F.col(seq_col))

    quantity_expr = F.col("quantity") if "quantity" in raw_df.columns else F.lit(1)

    result = raw_df.select(
        item_id_expr.alias("item_id"),
        F.col("order_id"),
        F.col("product_id"),
        quantity_expr.cast("short").alias("quantity"),
        F.col(price_col).cast("decimal(12,2)").alias("unit_price"),
    )

    result.write.mode("overwrite").parquet(f"s3a://{clean_bucket}/fact_order_items/")
    return result
