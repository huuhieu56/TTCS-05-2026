"""Load stage — read clean Parquet from MinIO and insert into ClickHouse.

Strategy per table engine:
    - ReplacingMergeTree (customer_360):
        INSERT directly → ClickHouse deduplicates by ORDER BY key.
        Run OPTIMIZE TABLE FINAL to compact immediately.
    - MergeTree (all other tables):
        Atomic swap via staging table — ensures dashboard never sees
        empty/partial data, even if the pipeline crashes mid-load.
        Steps: CREATE staging → INSERT into staging → EXCHANGE TABLES → DROP old
"""

from __future__ import annotations

import logging
from pathlib import Path

from pipelines.clickhouse_client import ClickHouseClient
from pipelines.settings import PipelineConfig
from pipelines.spark_session import create_spark_session

logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).resolve().parents[1]
_WAREHOUSE_DIR = _PROJECT_ROOT / "warehouse"

DDL_FILES = [
    _WAREHOUSE_DIR / "ddl" / "dim_users.sql",
    _WAREHOUSE_DIR / "ddl" / "dim_products.sql",
    _WAREHOUSE_DIR / "ddl" / "fact_orders.sql",
    _WAREHOUSE_DIR / "ddl" / "fact_order_items.sql",
    _WAREHOUSE_DIR / "ddl" / "fact_events_log.sql",
    _WAREHOUSE_DIR / "ddl" / "fact_cs_tickets.sql",
    _WAREHOUSE_DIR / "views" / "customer_360.sql",
]

TABLE_SOURCE_MAP = [
    ("dim_users", "dim_users"),
    ("dim_products", "dim_products"),
    ("fact_orders", "fact_orders"),
    ("fact_order_items", "fact_order_items"),
    ("fact_events_log", "fact_events_log"),
    ("fact_cs_tickets", "fact_cs_tickets"),
    ("customer_360", "customer_360"),
]

# Tables using ReplacingMergeTree — safe to INSERT directly (auto-dedup).
_REPLACING_TABLES = {"customer_360"}


def _resolve_bucket(table, config):
    if table == "customer_360":
        return config.minio.bucket_serving
    return config.minio.bucket_clean


def _fqn(db: str, table: str) -> str:
    """Fully-qualified ClickHouse table name."""
    return f"{db}.{table}"


def _load_replacing(ch: ClickHouseClient, db: str, table: str, pdf) -> None:
    """Load into ReplacingMergeTree — INSERT + OPTIMIZE FINAL.

    ReplacingMergeTree deduplicates rows with the same ORDER BY key
    (user_id for customer_360), keeping only the latest version.
    OPTIMIZE TABLE FINAL forces immediate deduplication.
    """
    fq = _fqn(db, table)
    ch.insert_dataframe(table, pdf)
    ch.execute(f"OPTIMIZE TABLE {fq} FINAL")
    logger.info("  Optimized %s (ReplacingMergeTree dedup)", fq)


def _load_atomic_swap(ch: ClickHouseClient, db: str, table: str, pdf) -> None:
    """Load into MergeTree via atomic swap — zero downtime.

    1. CREATE TABLE staging AS target (copies structure)
    2. INSERT data into staging
    3. EXCHANGE TABLES staging AND target (atomic, <1ms)
    4. DROP old staging (now contains stale data)

    If pipeline crashes at step 1-2: target is untouched, dashboard OK.
    If pipeline crashes at step 4: just a leftover staging table, no harm.
    """
    fq_target = _fqn(db, table)
    fq_staging = _fqn(db, f"{table}_staging")

    # 1. Clean up any leftover staging from a previous failed run
    ch.execute(f"DROP TABLE IF EXISTS {fq_staging}")

    # 2. Create staging with identical structure
    ch.execute(f"CREATE TABLE {fq_staging} AS {fq_target}")

    # 3. Insert data into staging
    ch.insert_dataframe(f"{table}_staging", pdf)
    logger.info("  Loaded %d rows into staging table %s", len(pdf), fq_staging)

    # 4. Atomic swap — dashboard sees either all-old or all-new, never empty
    ch.execute(f"EXCHANGE TABLES {fq_target} AND {fq_staging}")
    logger.info("  Atomic swap: %s ⟷ %s", fq_target, fq_staging)

    # 5. Drop the old data (now in staging)
    ch.execute(f"DROP TABLE IF EXISTS {fq_staging}")


def run_load(config: PipelineConfig) -> None:
    ch = ClickHouseClient(config.clickhouse)
    spark = create_spark_session(config)

    try:
        ch.execute("CREATE DATABASE IF NOT EXISTS {}".format(config.clickhouse.database))
        logger.info("=== LOAD: Running DDL scripts ===")
        for ddl_file in DDL_FILES:
            ch.execute_ddl_file(ddl_file)

        logger.info("=== LOAD: Inserting data ===")
        for table, source_dir in TABLE_SOURCE_MAP:
            bucket = _resolve_bucket(table, config)
            parquet_path = f"s3a://{bucket}/{source_dir}/"
            logger.info("Reading %s from %s", table, parquet_path)

            spark_df = spark.read.parquet(parquet_path)
            pdf = spark_df.toPandas()
            row_count = len(pdf)

            if pdf.empty:
                logger.warning("  Skipping %s — no data", table)
                continue

            logger.info("  Loading %s (%d rows)", table, row_count)

            if table in _REPLACING_TABLES:
                _load_replacing(ch, config.clickhouse.database, table, pdf)
            else:
                _load_atomic_swap(ch, config.clickhouse.database, table, pdf)

        logger.info("=== LOAD stage complete ===")
    finally:
        ch.close()
        spark.stop()
