"""SQL source connector — uploads CSV files from data_source/sql/ to MinIO raw zone."""

from __future__ import annotations

import logging
from pathlib import Path

from pipelines.settings import PipelineConfig
from pipelines.storage import StorageClient

logger = logging.getLogger(__name__)

EXPECTED_FILES = [
    "users.csv",
    "products.csv",
    "orders.csv",
    "order_items.csv",
]


def extract_sql(config: PipelineConfig, storage: StorageClient) -> None:
    sql_dir = config.data_source_dir / "sql"
    if not sql_dir.exists():
        raise FileNotFoundError(f"SQL source directory not found: {sql_dir}")

    uploaded = 0
    for filepath in sorted(sql_dir.glob("*.csv")):
        if not filepath.is_file():
            continue
        storage.upload_file(config.minio.bucket_raw, f"sql/{filepath.name}", filepath)
        uploaded += 1

    for expected in EXPECTED_FILES:
        if not (sql_dir / expected).is_file():
            logger.warning("Missing expected file: %s", expected)

    logger.info("SQL extract complete — %d files uploaded", uploaded)
