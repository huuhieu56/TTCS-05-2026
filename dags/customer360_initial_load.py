"""Customer 360 Initial Load DAG — one-time full data generation and pipeline run.

Trigger manually to initialize the entire platform from scratch.
Generates all fake data (shifted to present dates), then runs the full ETL pipeline.
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "ttcs",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

_AIRFLOW_HOME = Path("/opt/airflow")
for _p in [str(_AIRFLOW_HOME), str(_AIRFLOW_HOME / "pipelines"), str(_AIRFLOW_HOME / "generate_fake_data")]:
    if _p not in sys.path:
        sys.path.insert(0, _p)


def _generate_full_data(**context):
    """Run full data generation from Olist sample data with date shifting."""
    import subprocess
    result = subprocess.run(
        [sys.executable, "-m", "generate_fake_data.run_all",
         "--seed", "42", "--sample-frac", "1.0", "--num-sessions", "5000"],
        cwd=str(_AIRFLOW_HOME),
        capture_output=True, text=True, timeout=600,
    )
    if result.returncode != 0:
        raise RuntimeError(f"Data generation failed:\n{result.stderr}")
    print(result.stdout)


def _run_extract(**context):
    from pipelines.settings import load_config
    from pipelines.extract import run_extract
    config = load_config()
    run_extract(config)


def _run_transform(**context):
    from pipelines.settings import load_config
    from pipelines.transform import run_transform
    config = load_config()
    run_transform(config)


def _run_load(**context):
    from pipelines.settings import load_config
    from pipelines.load import run_load
    config = load_config()
    run_load(config)


with DAG(
    dag_id="customer360_initial_load",
    default_args=default_args,
    description="One-time initial load: generate full data → extract → transform → load",
    schedule=None,  # Manual trigger only
    start_date=datetime(2026, 3, 1),
    catchup=False,
    tags=["customer360", "etl", "initial"],
) as dag:

    generate_full = PythonOperator(
        task_id="generate_full_data",
        python_callable=_generate_full_data,
        doc="Generate all fake data from Olist with dates shifted to present",
    )

    extract = PythonOperator(
        task_id="extract_all_sources",
        python_callable=_run_extract,
        doc="Extract from PostgreSQL, FastAPI, Excel → MinIO raw",
    )

    transform = PythonOperator(
        task_id="transform_spark",
        python_callable=_run_transform,
        doc="Spark transform: raw → clean → customer_360",
    )

    load = PythonOperator(
        task_id="load_to_clickhouse",
        python_callable=_run_load,
        doc="Load into ClickHouse tables",
    )

    generate_full >> extract >> transform >> load
