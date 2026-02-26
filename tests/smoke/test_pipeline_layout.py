"""Smoke test — verify pipeline module structure and imports are valid."""

from __future__ import annotations

import importlib

import pytest

PIPELINE_MODULES = [
    "pipelines.settings",
    "pipelines.storage",
    "pipelines.spark_session",
    "pipelines.clickhouse_client",
    "pipelines.extract",
    "pipelines.transform",
    "pipelines.load",
    "pipelines.run_pipeline",
    "pipelines.sources.sql_source",
    "pipelines.sources.api_source",
    "pipelines.sources.excel_source",
    "pipelines.transforms.users",
    "pipelines.transforms.products",
    "pipelines.transforms.orders",
    "pipelines.transforms.order_items",
    "pipelines.transforms.events",
    "pipelines.transforms.cs_tickets",
    "pipelines.transforms.customer_360",
]


@pytest.mark.parametrize("module_name", PIPELINE_MODULES)
def test_module_importable(module_name: str) -> None:
    mod = importlib.import_module(module_name)
    assert mod is not None


def test_settings_load_config() -> None:
    from pipelines.settings import load_config, PipelineConfig

    config = load_config()
    assert isinstance(config, PipelineConfig)
    assert config.data_source_dir.name == "data_source"
    assert config.minio.bucket_raw == "ttcs-raw"


def test_pipeline_stages_defined() -> None:
    from pipelines.run_pipeline import STAGES

    assert "extract" in STAGES
    assert "transform" in STAGES
    assert "load" in STAGES
