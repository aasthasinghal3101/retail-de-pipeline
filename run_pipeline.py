"""
run_pipeline.py
----------------
A locally-runnable orchestrator that executes the full pipeline end-to-end:

    generate_raw_data -> load_to_warehouse -> dbt run -> dbt test

It exists for two reasons:
  1. So the whole project can be verified with a single command, without
     standing up a full Airflow scheduler.
  2. It defines the exact task graph that orchestration/airflow_dag.py
     mirrors in production — each `@task` in this file has a matching
     Airflow task of the same name, so the two stay in lockstep.

Usage:
    python orchestration/run_pipeline.py
    python orchestration/run_pipeline.py --skip-generate   # reuse existing raw data
"""
from __future__ import annotations

import argparse
import logging
import subprocess
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DBT_DIR = PROJECT_ROOT / "dbt_project"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("pipeline")


class TaskError(RuntimeError):
    pass


def run_task(name: str, fn, *, max_retries: int = 2, **kwargs) -> float:
    """Runs a task with basic retry/backoff — the same resilience pattern
    the Airflow DAG configures declaratively via `retries=`."""
    attempt = 0
    start = time.time()
    while True:
        attempt += 1
        try:
            log.info(f"[{name}] starting (attempt {attempt})")
            fn(**kwargs)
            elapsed = time.time() - start
            log.info(f"[{name}] SUCCESS in {elapsed:.2f}s")
            return elapsed
        except Exception as exc:  # noqa: BLE001 — intentionally broad at orchestration boundary
            if attempt > max_retries:
                log.error(f"[{name}] FAILED after {attempt} attempts: {exc}")
                raise TaskError(f"Task '{name}' failed: {exc}") from exc
            wait = 2 ** attempt
            log.warning(f"[{name}] attempt {attempt} failed ({exc}); retrying in {wait}s")
            time.sleep(wait)


def _run_cmd(cmd: list[str], cwd: Path | None = None) -> None:
    result = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(
            f"Command failed ({' '.join(cmd)}):\n{result.stdout[-2000:]}\n{result.stderr[-2000:]}"
        )
    log.debug(result.stdout)


def task_generate_raw_data(customers: int, products: int, orders: int) -> None:
    _run_cmd(
        [
            sys.executable,
            str(PROJECT_ROOT / "ingestion" / "generate_raw_data.py"),
            "--customers", str(customers),
            "--products", str(products),
            "--orders", str(orders),
        ]
    )


def task_load_to_warehouse() -> None:
    _run_cmd([sys.executable, str(PROJECT_ROOT / "ingestion" / "load_to_warehouse.py")])


def task_dbt_run() -> None:
    _run_cmd(["dbt", "run", "--profiles-dir", ".", "--project-dir", "."], cwd=DBT_DIR)


def task_dbt_test() -> None:
    _run_cmd(["dbt", "test", "--profiles-dir", ".", "--project-dir", "."], cwd=DBT_DIR)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the retail data pipeline end-to-end.")
    parser.add_argument("--skip-generate", action="store_true", help="Reuse existing raw CSVs.")
    parser.add_argument("--customers", type=int, default=500)
    parser.add_argument("--products", type=int, default=120)
    parser.add_argument("--orders", type=int, default=8000)
    args = parser.parse_args()

    log.info("=" * 70)
    log.info("RETAIL DATA PIPELINE — starting run")
    log.info("=" * 70)

    timings: dict[str, float] = {}
    pipeline_start = time.time()

    try:
        if not args.skip_generate:
            timings["generate_raw_data"] = run_task(
                "generate_raw_data",
                task_generate_raw_data,
                customers=args.customers,
                products=args.products,
                orders=args.orders,
            )
        else:
            log.info("[generate_raw_data] SKIPPED (--skip-generate)")

        timings["load_to_warehouse"] = run_task("load_to_warehouse", task_load_to_warehouse)
        timings["dbt_run"] = run_task("dbt_run", task_dbt_run)
        timings["dbt_test"] = run_task("dbt_test", task_dbt_test)

    except TaskError as exc:
        log.error(f"PIPELINE FAILED: {exc}")
        sys.exit(1)

    total = time.time() - pipeline_start
    log.info("=" * 70)
    log.info("PIPELINE COMPLETE")
    for task_name, secs in timings.items():
        log.info(f"  {task_name:<22} {secs:>6.2f}s")
    log.info(f"  {'TOTAL':<22} {total:>6.2f}s")
    log.info("=" * 70)


if __name__ == "__main__":
    main()
