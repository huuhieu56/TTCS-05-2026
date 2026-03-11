"""SQL source connector — reads tables from PostgreSQL via Spark JDBC and uploads to MinIO raw zone."""

from __future__ import annotations

import logging

from pyspark.sql import SparkSession

from pipelines.settings import PipelineConfig
from pipelines.storage import StorageClient

logger = logging.getLogger(__name__)

SOURCE_TABLES = [
    "users",
    "products",
    "orders",
    "order_items",
]


def _build_jdbc_url(config: PipelineConfig) -> str:
    pg = config.source_pg
    return f"jdbc:postgresql://{pg.host}:{pg.port}/{pg.database}"


def _build_jdbc_properties(config: PipelineConfig) -> dict[str, str]:
    pg = config.source_pg
    return {
        "driver": "org.postgresql.Driver",
        "user": pg.user,
        "password": pg.password,
    }


def extract_sql(config: PipelineConfig, storage: StorageClient, spark: SparkSession) -> None:
    jdbc_url = _build_jdbc_url(config)
    jdbc_props = _build_jdbc_properties(config)

    for table in SOURCE_TABLES:
        logger.info("Reading table '%s' from PostgreSQL via JDBC", table)
        df = spark.read.jdbc(url=jdbc_url, table=table, properties=jdbc_props)
        row_count = df.count()

        # Write as Parquet to MinIO raw zone
        raw_path = f"s3a://{config.minio.bucket_raw}/sql/{table}/"
        df.write.mode("overwrite").parquet(raw_path)
        logger.info("Uploaded %s (%d rows) → %s", table, row_count, raw_path)

    logger.info("SQL extract complete — %d tables uploaded", len(SOURCE_TABLES))
