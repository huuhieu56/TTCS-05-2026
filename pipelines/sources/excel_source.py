"""Excel customer-service source connector — uploads XLSX from data_source/excel/ to MinIO raw zone."""

from __future__ import annotations

import logging
from pathlib import Path

from pipelines.settings import PipelineConfig
from pipelines.storage import StorageClient

logger = logging.getLogger(__name__)


def extract_excel(config: PipelineConfig, storage: StorageClient) -> None:
    excel_dir = config.data_source_dir / "excel"
    if not excel_dir.exists():
        raise FileNotFoundError(f"Excel source directory not found: {excel_dir}")

    count = storage.upload_directory(
        config.minio.bucket_raw,
        "excel",
        excel_dir,
        "*.xlsx",
    )
    if count == 0:
        logger.warning("No XLSX files found in %s", excel_dir)
    else:
        logger.info("Excel extract complete — %d files uploaded", count)
