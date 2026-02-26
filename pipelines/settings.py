"""Centralised configuration loaded from environment variables.

Env vars are sourced from infra/.env (or docker-compose env_file).
Defaults match the values in .env.example so the pipeline works
out-of-the-box in the Docker environment.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class MinioConfig:
    endpoint: str = field(default_factory=lambda: os.getenv("MINIO_ENDPOINT", "http://localhost:9000"))
    access_key: str = field(default_factory=lambda: os.getenv("MINIO_ROOT_USER", "minioadmin"))
    secret_key: str = field(default_factory=lambda: os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123"))
    secure: bool = False
    bucket_raw: str = field(default_factory=lambda: os.getenv("MINIO_BUCKET_RAW", "ttcs-raw"))
    bucket_clean: str = field(default_factory=lambda: os.getenv("MINIO_BUCKET_CLEAN", "ttcs-clean"))
    bucket_serving: str = field(default_factory=lambda: os.getenv("MINIO_BUCKET_SERVING", "ttcs-serving"))


@dataclass(frozen=True)
class ClickHouseConfig:
    host: str = field(default_factory=lambda: os.getenv("CLICKHOUSE_HOST", "localhost"))
    port: int = field(default_factory=lambda: int(os.getenv("CLICKHOUSE_PORT", "8123")))
    database: str = field(default_factory=lambda: os.getenv("CLICKHOUSE_DB", "ttcs"))
    user: str = field(default_factory=lambda: os.getenv("CLICKHOUSE_USER", "default"))
    password: str = field(default_factory=lambda: os.getenv("CLICKHOUSE_PASSWORD", ""))


@dataclass(frozen=True)
class SparkConfig:
    master: str = field(default_factory=lambda: os.getenv("SPARK_MASTER", "local[*]"))
    app_name: str = "TTCS-Customer360"


@dataclass(frozen=True)
class PipelineConfig:
    data_source_dir: Path = field(default_factory=lambda: _PROJECT_ROOT / "data_source")
    minio: MinioConfig = field(default_factory=MinioConfig)
    clickhouse: ClickHouseConfig = field(default_factory=ClickHouseConfig)
    spark: SparkConfig = field(default_factory=SparkConfig)


def load_config() -> PipelineConfig:
    return PipelineConfig()
