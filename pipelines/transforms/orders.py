"""Transform: fact_orders — validate status/payment values and enforce types.

Handles both pre-mapped data (data_source follows designed schema where
order_status is already 'Completed') and raw Olist data (where order_status
is 'delivered'). The mapping is applied only when raw values are detected.
"""

from __future__ import annotations

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

RAW_TO_STANDARD_STATUS = {
    "created": "Pending",
    "approved": "Processing",
    "processing": "Processing",
    "invoiced": "Processing",
    "shipped": "Processing",
    "delivered": "Completed",
    "canceled": "Cancelled",
    "unavailable": "Cancelled",
}

STANDARD_STATUSES = {"Pending", "Processing", "Completed", "Cancelled"}

RAW_TO_STANDARD_PAYMENT = {
    "credit_card": "Credit Card",
    "boleto": "Bank Slip",
    "voucher": "Voucher",
    "debit_card": "Debit Card",
}

STANDARD_PAYMENTS = {"Credit Card", "Bank Slip", "Voucher", "Debit Card"}


def _safe_remap(col_name: str, raw_to_standard: dict[str, str], standard_values: set[str]) -> F.Column:
    col = F.col(col_name)
    expr = col
    for raw_val, std_val in raw_to_standard.items():
        expr = F.when(col == raw_val, F.lit(std_val)).otherwise(expr)
    return F.when(expr.isin(*standard_values), expr).otherwise(F.lit(None)).alias(col_name)


def transform_orders(spark: SparkSession, raw_bucket: str, clean_bucket: str) -> DataFrame:
    orders_df = spark.read.csv(
        f"s3a://{raw_bucket}/sql/orders.csv", header=True, inferSchema=True,
    )

    result = orders_df.select(
        F.col("order_id"),
        F.col("user_id"),
        F.col("total_amount").cast("decimal(18,2)"),
        _safe_remap("order_status", RAW_TO_STANDARD_STATUS, STANDARD_STATUSES),
        _safe_remap("payment_method", RAW_TO_STANDARD_PAYMENT, STANDARD_PAYMENTS),
        F.to_timestamp(F.col("created_at")).alias("created_at"),
    )

    result.write.mode("overwrite").parquet(f"s3a://{clean_bucket}/fact_orders/")
    return result
