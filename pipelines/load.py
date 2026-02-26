"""Load stage — read clean Parquet from MinIO and insert into ClickHouse."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd

from pipelines.clickhouse_client import ClickHouseClient
from pipelines.settings import PipelineConfig
from pipelines.spark_session import create_spark_session

logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).resolve().parents[1]
_WAREHOUSE_DIR = _PROJECT_ROOT / "warehouse"

DDL_FILES = [
    _WAREHOUSE_DIR / "ddl" / "dim_users.sql",
    _WAREHOUSE_DIR / "ddl" / "dim_products.sql",
    _WAREHOUSE_DIR / "ddl" / "fact_orders.sql",
    _WAREHOUSE_DIR / "ddl" / "fact_order_items.sql",
    _WAREHOUSE_DIR / "ddl" / "fact_events_log.sql",
    _WAREHOUSE_DIR / "ddl" / "fact_cs_tickets.sql",
    _WAREHOUSE_DIR / "views" / "customer_360_view.sql",
]

TABLE_SOURCE_MAP = [
    ("dim_users", "dim_users"),
    ("dim_products", "dim_products"),
    ("fact_orders", "fact_orders"),
    ("fact_order_items", "fact_order_items"),
    ("fact_events_log", "fact_events_log"),
    ("fact_cs_tickets", "fact_cs_tickets"),
    ("customer_360_view", "customer_360_view"),
]


def _resolve_bucket(table: str, config: PipelineConfig) -> str:
    if table == "customer_360_view":
        return config.minio.bucket_serving
    return config.minio.bucket_clean


def run_load(config: PipelineConfig) -> None:
    ch = ClickHouseClient(config.clickhouse)
    spark = create_spark_session(config)

    try:
        ch.execute(f"CREATE DATABASE IF NOT EXISTS {config.clickhouse.database}")
        logger.info("=== LOAD: Running DDL scripts ===")
        for ddl_file in DDL_FILES:
            ch.execute_ddl_file(ddl_file)

        logger.info("=== LOAD: Inserting data ===")
        for table, source_dir in TABLE_SOURCE_MAP:
            bucket = _resolve_bucket(table, config)
            parquet_path = f"s3a://{bucket}/{source_dir}/"

            logger.info("Loading %s from %s", table, parquet_path)
            spark_df = spark.read.parquet(parquet_path)
            pandas_df = spark_df.toPandas()
            ch.insert_dataframe(table, pandas_df)

        logger.info("=== LOAD stage complete ===")
    finally:
        ch.close()
        spark.stop()
