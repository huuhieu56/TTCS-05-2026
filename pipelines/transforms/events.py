"""Transform: fact_events_log — parse clickstream JSON, validate event types."""

from __future__ import annotations

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
)

VALID_EVENT_TYPES = {"view_item", "add_to_cart", "cart_abandonment"}

CLICKSTREAM_SCHEMA = StructType([
    StructField("event_id", StringType(), nullable=False),
    StructField("timestamp", StringType(), nullable=False),
    StructField("user_id", StringType(), nullable=True),
    StructField("session_id", StringType(), nullable=False),
    StructField("event_type", StringType(), nullable=False),
    StructField("product_id", StringType(), nullable=True),
    StructField("device_os", StringType(), nullable=True),
    StructField("time_spent_seconds", IntegerType(), nullable=True),
])


def transform_events(spark: SparkSession, raw_bucket: str, clean_bucket: str) -> DataFrame:
    raw_df = spark.read.schema(CLICKSTREAM_SCHEMA).json(f"s3a://{raw_bucket}/api/")

    result = (
        raw_df
        .filter(F.col("event_type").isin(VALID_EVENT_TYPES))
        .select(
            F.col("event_id"),
            F.to_timestamp(F.col("timestamp")).alias("timestamp"),
            F.col("user_id"),
            F.col("session_id"),
            F.col("event_type"),
            F.col("product_id"),
            F.col("device_os"),
            F.col("time_spent_seconds").cast("short"),
        )
    )

    result.write.mode("overwrite").parquet(f"s3a://{clean_bucket}/fact_events_log/")
    return result
