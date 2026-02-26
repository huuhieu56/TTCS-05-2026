"""Extract stage — ingest raw files from data_source/ into MinIO raw zone."""

from __future__ import annotations

import logging

from pipelines.settings import PipelineConfig
from pipelines.sources.sql_source import extract_sql
from pipelines.sources.api_source import extract_api
from pipelines.sources.excel_source import extract_excel
from pipelines.storage import StorageClient

logger = logging.getLogger(__name__)


def run_extract(config: PipelineConfig) -> None:
    storage = StorageClient(config.minio)
    storage.ensure_all_buckets()

    logger.info("=== EXTRACT: SQL sources ===")
    extract_sql(config, storage)

    logger.info("=== EXTRACT: API sources ===")
    extract_api(config, storage)

    logger.info("=== EXTRACT: Excel sources ===")
    extract_excel(config, storage)

    logger.info("=== EXTRACT stage complete ===")
