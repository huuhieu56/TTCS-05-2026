"""API clickstream source connector — fetches events from FastAPI and uploads to MinIO raw zone."""

from __future__ import annotations

import json
import logging
import tempfile
from pathlib import Path

import requests

from pipelines.settings import PipelineConfig
from pipelines.storage import StorageClient

logger = logging.getLogger(__name__)

PAGE_SIZE = 5000


def extract_api(config: PipelineConfig, storage: StorageClient) -> None:
    base_url = config.source_api.base_url.rstrip("/")

    # Check API health
    health = requests.get(f"{base_url}/health", timeout=10)
    health.raise_for_status()
    logger.info("API health: %s", health.json())

    # Get total count
    count_resp = requests.get(f"{base_url}/events/count", timeout=10)
    count_resp.raise_for_status()
    total = count_resp.json()["total"]
    logger.info("Total events available: %d", total)

    if total == 0:
        logger.warning("No events found in API source")
        return

    # Paginate and collect all events
    all_events: list[dict] = []
    offset = 0
    while offset < total:
        resp = requests.get(
            f"{base_url}/events",
            params={"limit": PAGE_SIZE, "offset": offset},
            timeout=30,
        )
        resp.raise_for_status()
        page_data = resp.json()
        events = page_data["data"]
        all_events.extend(events)
        logger.info("Fetched %d events (offset=%d, total=%d)", len(events), offset, total)
        offset += PAGE_SIZE

    # Write to temp JSON-lines file, then upload to MinIO
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False, encoding="utf-8") as tmp:
        for event in all_events:
            tmp.write(json.dumps(event, ensure_ascii=False) + "\n")
        tmp_path = Path(tmp.name)

    try:
        storage.upload_file(config.minio.bucket_raw, "api/clickstream.json", tmp_path)
    finally:
        tmp_path.unlink(missing_ok=True)

    logger.info("API extract complete — %d events uploaded", len(all_events))
