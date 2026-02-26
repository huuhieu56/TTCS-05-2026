"""Transform: dim_users — validate and enforce schema for user dimension table.

Since data_source/ already contains the designed schema (loyalty_tier,
created_at are pre-derived by the data source generator), this transform
focuses on type enforcement, null handling, and writing clean Parquet.
"""

from __future__ import annotations

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window

VALID_LOYALTY_TIERS = {"Bronze", "Silver", "Gold", "Platinum"}


def transform_users(spark: SparkSession, raw_bucket: str, clean_bucket: str) -> DataFrame:
    users_df = spark.read.csv(
        f"s3a://{raw_bucket}/sql/users.csv", header=True, inferSchema=True,
    )

    has_loyalty = "loyalty_tier" in users_df.columns
    has_created_at = "created_at" in users_df.columns

    if has_loyalty and has_created_at:
        result = users_df.select(
            F.col("user_id"),
            F.col("full_name"),
            F.col("email"),
            F.col("phone_number"),
            F.col("customer_city"),
            F.col("customer_state"),
            F.when(
                F.col("loyalty_tier").isin(*VALID_LOYALTY_TIERS),
                F.col("loyalty_tier"),
            ).alias("loyalty_tier"),
            F.to_timestamp(F.col("created_at")).alias("created_at"),
        )
    else:
        orders_df = spark.read.csv(
            f"s3a://{raw_bucket}/sql/orders.csv", header=True, inferSchema=True,
        )
        result = _derive_users(users_df, orders_df)

    result.write.mode("overwrite").parquet(f"s3a://{clean_bucket}/dim_users/")
    return result


def _derive_users(users_df: DataFrame, orders_df: DataFrame) -> DataFrame:
    created_at_df = (
        orders_df
        .groupBy("user_id")
        .agg(F.min("created_at").alias("derived_created_at"))
    )

    completed_filter = (
        (F.col("order_status") == "Completed")
        | (F.col("order_status") == "delivered")
    )
    total_spend_df = (
        orders_df
        .filter(completed_filter)
        .groupBy("user_id")
        .agg(F.sum("total_amount").alias("total_spend"))
    )

    spend_with_pct = total_spend_df.withColumn(
        "spend_percentile",
        F.percent_rank().over(Window.orderBy(F.col("total_spend").desc())),
    )

    loyalty_df = spend_with_pct.withColumn(
        "loyalty_tier",
        F.when(F.col("spend_percentile") <= 0.05, F.lit("Platinum"))
        .when(F.col("spend_percentile") <= 0.15, F.lit("Gold"))
        .when(F.col("spend_percentile") <= 0.35, F.lit("Silver"))
        .otherwise(F.lit("Bronze")),
    ).select("user_id", "loyalty_tier")

    return (
        users_df
        .join(created_at_df, on="user_id", how="left")
        .join(loyalty_df, on="user_id", how="left")
        .select(
            F.col("user_id"),
            F.col("full_name"),
            F.col("email"),
            F.col("phone_number"),
            F.col("customer_city"),
            F.col("customer_state"),
            F.col("loyalty_tier"),
            F.coalesce(
                F.col("derived_created_at"),
                F.col("created_at"),
            ).alias("created_at"),
        )
    )
