"""Excel customer-service source connector — uploads XLSX from data_source/excel/ to MinIO raw zone."""

from __future__ import annotations

import logging
from pathlib import Path
import tempfile

import pandas as pd

from pipelines.settings import PipelineConfig
from pipelines.storage import StorageClient

logger = logging.getLogger(__name__)


def extract_excel(config: PipelineConfig, storage: StorageClient) -> None:
    excel_dir = config.data_source_dir / "excel"
    if not excel_dir.exists():
        raise FileNotFoundError(f"Excel source directory not found: {excel_dir}")

    xlsx_files = sorted(excel_dir.glob("*.xlsx"))
    count = 0
    csv_count = 0

    for filepath in xlsx_files:
        if not filepath.is_file():
            continue

        storage.upload_file(config.minio.bucket_raw, f"excel/{filepath.name}", filepath)
        count += 1

        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as temp_csv:
            temp_csv_path = Path(temp_csv.name)

        try:
            df = pd.read_excel(filepath)
            df.to_csv(temp_csv_path, index=False)
            storage.upload_file(
                config.minio.bucket_raw,
                f"excel/{filepath.stem}.csv",
                temp_csv_path,
            )
            csv_count += 1
        finally:
            temp_csv_path.unlink(missing_ok=True)

    if count == 0:
        logger.warning("No XLSX files found in %s", excel_dir)
    else:
        logger.info("Excel extract complete — %d xlsx and %d csv files uploaded", count, csv_count)
