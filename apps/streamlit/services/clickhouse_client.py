"""ClickHouse connection helper for Streamlit dashboard."""

from __future__ import annotations

import os

import clickhouse_connect
import pandas as pd

_client = None


def get_client():
    """Return a singleton ClickHouse client."""
    global _client
    if _client is None:
        _client = clickhouse_connect.get_client(
            host=os.getenv("CLICKHOUSE_HOST", "localhost"),
            port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
            database=os.getenv("CLICKHOUSE_DB", "ttcs"),
            username=os.getenv("CLICKHOUSE_USER", "default"),
            password=os.getenv("CLICKHOUSE_PASSWORD", "ttcs"),
        )
    return _client


def query_df(sql: str, params: dict | None = None) -> pd.DataFrame:
    """Execute SQL and return a pandas DataFrame."""
    client = get_client()
    result = client.query(sql, parameters=params or {})
    return pd.DataFrame(result.result_rows, columns=result.column_names)
