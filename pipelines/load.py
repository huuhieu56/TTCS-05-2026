"""Load stage — read clean Parquet from MinIO and insert into ClickHouse."""

from __future__ import annotations

import logging
from pathlib import Path

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
    _WAREHOUSE_DIR / "views" / "customer_360.sql",
]

TABLE_SOURCE_MAP = [
    ("dim_users", "dim_users"),
    ("dim_products", "dim_products"),
    ("fact_orders", "fact_orders"),
    ("fact_order_items", "fact_order_items"),
    ("fact_events_log", "fact_events_log"),
    ("fact_cs_tickets", "fact_cs_tickets"),
    ("customer_360", "customer_360"),
]


def _resolve_bucket(table: str, config: PipelineConfig) -> str:
    if table == "customer_360":
        return config.minio.bucket_serving
    return config.minio.bucket_clean


def _build_jdbc_url(config: PipelineConfig) -> str:
    ch = config.clickhouse
    return (
        f"jdbc:ch://{ch.host}:{ch.port}/{ch.database}"
        "?custom_http_params=max_partitions_per_insert_block=0"
    )


def _build_jdbc_properties(config: PipelineConfig) -> dict[str, str]:
    ch = config.clickhouse
    return {
        "driver": "com.clickhouse.jdbc.ClickHouseDriver",
        "user": ch.user,
        "password": ch.password,
    }


def run_load(config: PipelineConfig) -> None:
    ch = ClickHouseClient(config.clickhouse)
    spark = create_spark_session(config)

    jdbc_url = _build_jdbc_url(config)
    jdbc_props = _build_jdbc_properties(config)

    try:
        ch.execute(f"CREATE DATABASE IF NOT EXISTS {config.clickhouse.database}")
        logger.info("=== LOAD: Running DDL scripts ===")
        for ddl_file in DDL_FILES:
            ch.execute_ddl_file(ddl_file)

        logger.info("=== LOAD: Inserting data ===")
        for table, source_dir in TABLE_SOURCE_MAP:
            bucket = _resolve_bucket(table, config)
            parquet_path = f"s3a://{bucket}/{source_dir}/"
            logger.info("Reading %s from %s", table, parquet_path)

            spark_df = spark.read.parquet(parquet_path)
            row_count = spark_df.count()

            ch_table = f"{config.clickhouse.database}.{table}"
            ch.execute(f"TRUNCATE TABLE IF EXISTS {ch_table}")
            logger.info("Loading %s (%d rows) via JDBC", table, row_count)
            spark_df.write.jdbc(
                url=jdbc_url,
                table=ch_table,
                mode="append",
                properties=jdbc_props,
            )

        logger.info("=== LOAD stage complete ===")
    finally:
        ch.close()
        spark.stop()
