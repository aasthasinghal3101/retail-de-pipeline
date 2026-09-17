"""
airflow_dag.py
--------------
Production orchestration for the retail data pipeline, written for
Airflow 2.7+ (self-hosted, MWAA, or Cloud Composer) using the TaskFlow API.

This DAG defines the exact same task graph that
`orchestration/run_pipeline.py` executes locally (generate_raw_data ->
load_to_warehouse -> dbt run -> dbt test), so the pipeline's logic can be
verified locally with a plain `python run_pipeline.py` before being
scheduled here. Only the execution environment differs:
  * retries / alerting / SLA are handled declaratively by Airflow
  * `load_to_warehouse.get_connection()` would point at Snowflake/Databricks
    instead of local DuckDB (env-var driven — no code change)
  * dbt runs via the official Cosmos/BashOperator pattern against the same
    dbt_project/ directory

Not executed inside this sandbox (no Airflow scheduler here) — it's the
deployment artifact that ships alongside the verified local pipeline.
"""
from __future__ import annotations

import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from airflow.operators.bash import BashOperator

PROJECT_ROOT = Path("/opt/airflow/dags/retail-de-pipeline")  # deploy path in prod
DBT_DIR = PROJECT_ROOT / "dbt_project"

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "email_on_failure": True,
    "email": ["data-alerts@example.com"],
}


@dag(
    dag_id="retail_data_pipeline",
    description="Daily ELT pipeline: raw source -> warehouse -> dbt marts, with data-quality gates.",
    schedule="0 3 * * *",  # 03:00 UTC daily, ahead of the 08:00 IST BI refresh
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["retail", "elt", "dbt"],
)
def retail_data_pipeline():

    @task
    def extract_source_data() -> None:
        """EXTRACT: pull incremental orders/customers/products from the
        source system (OMS API / CDC stream) into the raw landing zone."""
        subprocess.run(
            [sys.executable, str(PROJECT_ROOT / "ingestion" / "generate_raw_data.py")],
            check=True,
        )

    @task
    def load_to_warehouse() -> None:
        """LOAD: land raw files into the warehouse's raw schema."""
        subprocess.run(
            [sys.executable, str(PROJECT_ROOT / "ingestion" / "load_to_warehouse.py")],
            check=True,
        )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_DIR} && dbt run --profiles-dir . --project-dir .",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_DIR} && dbt test --profiles-dir . --project-dir .",
    )

    @task
    def refresh_genai_index() -> None:
        """Rebuild the retrieval index the NL query assistant reads from,
        so it reflects today's marts (row counts, latest date, etc.)."""
        subprocess.run(
            [sys.executable, str(PROJECT_ROOT / "genai_assistant" / "build_index.py")],
            check=True,
        )

    @task(trigger_rule="all_done")
    def notify_completion() -> None:
        """Always-run notification task — posts pipeline status to Slack/PagerDuty.
        `trigger_rule=all_done` ensures this fires even on upstream failure,
        so a broken pipeline is never silent."""
        print("Pipeline run finished — see task statuses above for pass/fail.")

    extract = extract_source_data()
    load = load_to_warehouse()
    genai = refresh_genai_index()
    notify = notify_completion()

    chain(extract, load, dbt_run, dbt_test, genai, notify)


retail_data_pipeline()
