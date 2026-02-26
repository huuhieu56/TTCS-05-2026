"""MinIO storage client wrapper for uploading and downloading objects."""

from __future__ import annotations

import logging
from pathlib import Path
from urllib.parse import urlparse

from minio import Minio

from pipelines.settings import MinioConfig

logger = logging.getLogger(__name__)


class StorageClient:
    """Thin wrapper around the MinIO Python SDK."""

    def __init__(self, config: MinioConfig) -> None:
        parsed = urlparse(config.endpoint)
        host_port = parsed.netloc or parsed.path
        self._client = Minio(
            host_port,
            access_key=config.access_key,
            secret_key=config.secret_key,
            secure=config.secure,
        )
        self._config = config

    def ensure_bucket(self, bucket: str) -> None:
        if not self._client.bucket_exists(bucket):
            self._client.make_bucket(bucket)
            logger.info("Created bucket: %s", bucket)

    def ensure_all_buckets(self) -> None:
        for bucket in (
            self._config.bucket_raw,
            self._config.bucket_clean,
            self._config.bucket_serving,
        ):
            self.ensure_bucket(bucket)

    def upload_file(self, bucket: str, object_name: str, file_path: Path) -> None:
        self._client.fput_object(bucket, object_name, str(file_path))
        logger.info("Uploaded %s -> s3://%s/%s", file_path.name, bucket, object_name)

    def upload_directory(self, bucket: str, prefix: str, local_dir: Path, pattern: str = "*") -> int:
        count = 0
        for file_path in sorted(local_dir.glob(pattern)):
            if file_path.is_file():
                object_name = f"{prefix}/{file_path.name}"
                self.upload_file(bucket, object_name, file_path)
                count += 1
        return count

    def download_file(self, bucket: str, object_name: str, file_path: Path) -> None:
        file_path.parent.mkdir(parents=True, exist_ok=True)
        self._client.fget_object(bucket, object_name, str(file_path))
        logger.info("Downloaded s3://%s/%s -> %s", bucket, object_name, file_path)

    def list_objects(self, bucket: str, prefix: str = "") -> list[str]:
        return [
            obj.object_name
            for obj in self._client.list_objects(bucket, prefix=prefix, recursive=True)
        ]
