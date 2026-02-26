"""Data quality tests — basic rule checks on cleaned output data.

These tests are designed to run against Parquet files in the clean zone.
They use PySpark to read files and validate data quality rules.
"""

from __future__ import annotations

import pytest

from pipelines.settings import load_config, PipelineConfig
from pipelines.spark_session import create_spark_session


@pytest.fixture(scope="module")
def config() -> PipelineConfig:
    return load_config()


@pytest.fixture(scope="module")
def spark(config):
    session = create_spark_session(config)
    yield session
    session.stop()


class TestUsersQuality:
    def test_user_id_not_null(self, spark, config) -> None:
        df = spark.read.parquet(f"s3a://{config.minio.bucket_clean}/dim_users/")
        null_count = df.filter(df.user_id.isNull()).count()
        assert null_count == 0, f"Found {null_count} NULL user_ids"

    def test_loyalty_tier_values(self, spark, config) -> None:
        df = spark.read.parquet(f"s3a://{config.minio.bucket_clean}/dim_users/")
        valid_tiers = {"Bronze", "Silver", "Gold", "Platinum", None}
        actual_tiers = {row.loyalty_tier for row in df.select("loyalty_tier").distinct().collect()}
        assert actual_tiers.issubset(valid_tiers), f"Invalid tiers found: {actual_tiers - valid_tiers}"


class TestOrdersQuality:
    def test_order_status_values(self, spark, config) -> None:
        df = spark.read.parquet(f"s3a://{config.minio.bucket_clean}/fact_orders/")
        valid_statuses = {"Pending", "Processing", "Completed", "Cancelled"}
        actual = {row.order_status for row in df.select("order_status").distinct().collect()}
        assert actual.issubset(valid_statuses), f"Invalid statuses: {actual - valid_statuses}"

    def test_total_amount_non_negative(self, spark, config) -> None:
        df = spark.read.parquet(f"s3a://{config.minio.bucket_clean}/fact_orders/")
        negative_count = df.filter(df.total_amount < 0).count()
        assert negative_count == 0, f"Found {negative_count} negative total_amounts"


class TestEventsQuality:
    def test_event_type_whitelist(self, spark, config) -> None:
        df = spark.read.parquet(f"s3a://{config.minio.bucket_clean}/fact_events_log/")
        valid_types = {"view_item", "add_to_cart", "cart_abandonment"}
        actual = {row.event_type for row in df.select("event_type").distinct().collect()}
        assert actual.issubset(valid_types), f"Invalid event types: {actual - valid_types}"


class TestTicketsQuality:
    def test_rating_range(self, spark, config) -> None:
        df = spark.read.parquet(f"s3a://{config.minio.bucket_clean}/fact_cs_tickets/")
        out_of_range = df.filter((df.rating < 1) | (df.rating > 5)).count()
        assert out_of_range == 0, f"Found {out_of_range} ratings outside 1-5 range"

    def test_issue_category_values(self, spark, config) -> None:
        df = spark.read.parquet(f"s3a://{config.minio.bucket_clean}/fact_cs_tickets/")
        valid_categories = {"Product Issue", "General Inquiry", "Positive Feedback", None}
        actual = {row.issue_category for row in df.select("issue_category").distinct().collect()}
        assert actual.issubset(valid_categories), f"Invalid categories: {actual - valid_categories}"
