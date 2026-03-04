"""Transform: fact_cs_tickets — validate and clean customer service ticket data.

Handles both pre-mapped data (Issue_Type and Status already exist)
and raw Olist review data (needs derivation from Customer_Rating
and review_answer_timestamp).
"""

from __future__ import annotations

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

EMAIL_REGEX = r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$"

VALID_ISSUE_TYPES = {"Product Issue", "General Inquiry", "Positive Feedback"}


def _derive_issue_category(rating_col: str) -> F.Column:
    return (
        F.when(F.col(rating_col).isin(1, 2), F.lit("Product Issue"))
        .when(F.col(rating_col) == 3, F.lit("General Inquiry"))
        .when(F.col(rating_col).isin(4, 5), F.lit("Positive Feedback"))
        .otherwise(F.lit(None))
    )


def transform_cs_tickets(spark: SparkSession, raw_bucket: str, clean_bucket: str) -> DataFrame:
    raw_df = spark.read.csv(
        f"s3a://{raw_bucket}/excel/CS_Tickets.csv",
        header=True,
        inferSchema=True,
    )

    has_issue_type = "Issue_Type" in raw_df.columns
    if has_issue_type:
        issue_expr = F.when(
            F.col("Issue_Type").isin(*VALID_ISSUE_TYPES),
            F.col("Issue_Type"),
        ).otherwise(_derive_issue_category("Customer_Rating"))
    else:
        issue_expr = _derive_issue_category("Customer_Rating")

    email_expr = F.when(
        F.col("Customer_Email").rlike(EMAIL_REGEX),
        F.lower(F.trim(F.col("Customer_Email"))),
    ).otherwise(F.lit(None))

    result = raw_df.select(
        F.col("Ticket_ID").alias("ticket_id"),
        F.col("Order_ID").alias("order_id"),
        email_expr.alias("customer_email"),
        issue_expr.alias("issue_category"),
        # Coerce NULL/empty Status to "Unknown" — DDL is LowCardinality(String) NOT NULL.
        F.coalesce(
            F.when(F.trim(F.col("Status")) != "", F.trim(F.col("Status"))),
            F.lit("Unknown"),
        ).alias("status"),
        # Cast to integer to safely map to ClickHouse Nullable(UInt8); values are 1-5.
        F.col("Customer_Rating").cast("integer").alias("rating"),
        F.to_timestamp(F.col("Reported_Date")).alias("reported_at"),
    )

    result.write.mode("overwrite").parquet(f"s3a://{clean_bucket}/fact_cs_tickets/")
    return result
