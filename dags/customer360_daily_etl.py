"""Customer 360 ETL DAG — runs once daily at midnight UTC.

Flow: generate_daily_data → run_pipeline (extract + transform + load)
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "ttcs",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

_AIRFLOW_HOME = Path("/opt/airflow")
for _p in [str(_AIRFLOW_HOME), str(_AIRFLOW_HOME / "pipelines"), str(_AIRFLOW_HOME / "generate_fake_data")]:
    if _p not in sys.path:
        sys.path.insert(0, _p)


def _generate_daily_data(**context):
    """Generate fake incremental data for the execution date."""
    from generate_fake_data.generate_daily_data import run_daily
    execution_date = context["logical_date"]
    target = execution_date.replace(hour=0, minute=0, second=0, microsecond=0)
    if hasattr(target, 'tzinfo') and target.tzinfo is not None:
        target = target.replace(tzinfo=None)
    run_daily(target, dry_run=False)


def _run_pipeline(**context):
    """Run the full ETL pipeline."""
    from pipelines.settings import load_config
    from pipelines.extract import run_extract
    from pipelines.transform import run_transform
    from pipelines.load import run_load

    config = load_config()
    run_extract(config)
    run_transform(config)
    run_load(config)


with DAG(
    dag_id="customer360_daily_etl",
    default_args=default_args,
    description="ETL: generate daily data → extract → transform → load into ClickHouse",
    schedule="@daily",
    start_date=datetime(2026, 3, 1),
    catchup=False,
    tags=["customer360", "etl", "daily"],
) as dag:

    generate_data = PythonOperator(
        task_id="generate_daily_data",
        python_callable=_generate_daily_data,
    )

    run_etl = PythonOperator(
        task_id="run_etl_pipeline",
        python_callable=_run_pipeline,
    )

    generate_data >> run_etl
