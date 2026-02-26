"""Transform stage — Spark reads raw data from MinIO, cleans, and writes Parquet to clean zone."""

from __future__ import annotations

import logging

from pipelines.settings import PipelineConfig
from pipelines.spark_session import create_spark_session
from pipelines.transforms.order_items import transform_order_items
from pipelines.transforms.users import transform_users
from pipelines.transforms.products import transform_products
from pipelines.transforms.orders import transform_orders
from pipelines.transforms.events import transform_events
from pipelines.transforms.cs_tickets import transform_cs_tickets
from pipelines.transforms.customer_360 import transform_customer_360

logger = logging.getLogger(__name__)


def run_transform(config: PipelineConfig) -> None:
    spark = create_spark_session(config)
    raw = config.minio.bucket_raw
    clean = config.minio.bucket_clean
    serving = config.minio.bucket_serving

    try:
        logger.info("=== TRANSFORM: order_items ===")
        transform_order_items(spark, raw, clean)

        logger.info("=== TRANSFORM: users ===")
        transform_users(spark, raw, clean)

        logger.info("=== TRANSFORM: products ===")
        transform_products(spark, raw, clean)

        logger.info("=== TRANSFORM: orders ===")
        transform_orders(spark, raw, clean)

        logger.info("=== TRANSFORM: events ===")
        transform_events(spark, raw, clean)

        logger.info("=== TRANSFORM: cs_tickets ===")
        transform_cs_tickets(spark, raw, clean)

        logger.info("=== TRANSFORM: customer_360 ===")
        transform_customer_360(spark, clean, serving)

        logger.info("=== TRANSFORM stage complete ===")
    finally:
        spark.stop()
