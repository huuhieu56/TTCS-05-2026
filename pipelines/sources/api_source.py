"""API clickstream source connector — uploads JSON lines from data_source/api/ to MinIO raw zone."""

from __future__ import annotations

import logging
from pathlib import Path

from pipelines.settings import PipelineConfig
from pipelines.storage import StorageClient

logger = logging.getLogger(__name__)


def extract_api(config: PipelineConfig, storage: StorageClient) -> None:
    api_dir = config.data_source_dir / "api"
    if not api_dir.exists():
        raise FileNotFoundError(f"API source directory not found: {api_dir}")

    count = storage.upload_directory(
        config.minio.bucket_raw,
        "api",
        api_dir,
        "*.json",
    )
    if count == 0:
        logger.warning("No JSON files found in %s", api_dir)
    else:
        logger.info("API extract complete — %d files uploaded", count)
