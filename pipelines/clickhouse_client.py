"""ClickHouse client wrapper for DDL execution and data insertion."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import clickhouse_connect
    import pandas as pd

logger = logging.getLogger(__name__)


class ClickHouseClient:
    """Thin wrapper around clickhouse-connect.

    The heavy dependency is imported lazily so the module can be imported
    even when ``clickhouse-connect`` is not installed (e.g. during linting
    or unit-testing on a dev machine).
    """

    def __init__(self, config) -> None:
        import clickhouse_connect as _cc

        self._client = _cc.get_client(
            host=config.host,
            port=config.port,
            database=config.database,
            username=config.user,
            password=config.password,
        )
        self._database = config.database

    def execute(self, query: str) -> None:
        self._client.command(query)

    def execute_ddl_file(self, filepath: Path) -> None:
        sql = filepath.read_text(encoding="utf-8").strip()
        if not sql or sql.startswith("-- TODO"):
            logger.warning("Skipping empty/placeholder DDL: %s", filepath.name)
            return
        for statement in sql.split(";"):
            statement = statement.strip()
            if statement and not statement.startswith("--"):
                self._client.command(statement)
        logger.info("Executed DDL: %s", filepath.name)

    def insert_dataframe(self, table: str, df: pd.DataFrame) -> None:
        if df.empty:
            logger.warning("Empty dataframe, skipping insert into %s", table)
            return
        self._client.insert_df(f"{self._database}.{table}", df)
        logger.info("Inserted %d rows into %s", len(df), table)

    def query_df(self, query: str) -> pd.DataFrame:
        return self._client.query_df(query)

    def close(self) -> None:
        self._client.close()
