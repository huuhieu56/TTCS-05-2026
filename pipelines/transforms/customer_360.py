"""Transform: customer_360_view — aggregate wide table from all clean sources."""

from __future__ import annotations

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F


def transform_customer_360(spark: SparkSession, clean_bucket: str, serving_bucket: str) -> DataFrame:
    users_df = spark.read.parquet(f"s3a://{clean_bucket}/dim_users/")
    orders_df = spark.read.parquet(f"s3a://{clean_bucket}/fact_orders/")
    events_df = spark.read.parquet(f"s3a://{clean_bucket}/fact_events_log/")
    tickets_df = spark.read.parquet(f"s3a://{clean_bucket}/fact_cs_tickets/")

    order_agg = (
        orders_df
        .filter(F.col("order_status") == "Completed")
        .groupBy("user_id")
        .agg(
            F.sum("total_amount").alias("total_lifetime_value"),
            F.count("order_id").alias("total_orders_completed"),
            F.max("created_at").alias("last_order_date"),
        )
    )

    cart_aband = (
        events_df
        .filter(F.col("event_type") == "cart_abandonment")
        .groupBy("user_id")
        .agg(F.count("event_id").alias("total_abandoned_carts"))
    )

    last_event = (
        events_df
        .groupBy("user_id")
        .agg(F.max("timestamp").alias("last_event_date"))
    )

    complaints = (
        tickets_df
        .filter(F.col("issue_category") == "Product Issue")
        .join(orders_df.select("order_id", "user_id"), on="order_id", how="inner")
        .groupBy("user_id")
        .agg(F.count("ticket_id").alias("total_cs_complaints"))
    )

    result = (
        users_df
        .select("user_id", "full_name", "customer_city", "customer_state", "loyalty_tier")
        .join(order_agg, on="user_id", how="left")
        .join(cart_aband, on="user_id", how="left")
        .join(last_event, on="user_id", how="left")
        .join(complaints, on="user_id", how="left")
        .select(
            F.col("user_id"),
            F.col("full_name"),
            F.col("customer_city"),
            F.col("customer_state"),
            F.col("loyalty_tier"),
            F.coalesce(F.col("total_lifetime_value"), F.lit(0.00)).cast("decimal(18,2)").alias("total_lifetime_value"),
            F.coalesce(F.col("total_orders_completed"), F.lit(0)).cast("int").alias("total_orders_completed"),
            F.coalesce(F.col("total_abandoned_carts"), F.lit(0)).cast("int").alias("total_abandoned_carts"),
            F.coalesce(F.col("total_cs_complaints"), F.lit(0)).cast("int").alias("total_cs_complaints"),
            F.greatest(F.col("last_order_date"), F.col("last_event_date")).alias("last_active_date"),
            F.lit(None).cast("float").alias("churn_risk_score"),
        )
    )

    result.write.mode("overwrite").parquet(f"s3a://{serving_bucket}/customer_360_view/")
    return result
