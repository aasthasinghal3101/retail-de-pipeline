"""
load_to_warehouse.py
---------------------
Simulates the LOAD step of an ELT pipeline: lands raw CSVs from the raw zone
into a warehouse "raw" schema, unmodified, as-is.

This project uses DuckDB as a free, local, SQL-compatible stand-in for a
cloud warehouse (Snowflake / Databricks SQL). The connection layer is
isolated in `get_connection()` specifically so it can be swapped for a real
Snowflake/Databricks connector in production without touching any
downstream code — only the connection string changes; every SQL statement
in this project is ANSI-SQL and runs unmodified on Snowflake, Databricks
SQL, or DuckDB.

Usage:
    python ingestion/load_to_warehouse.py
"""
from __future__ import annotations

from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"
WAREHOUSE_PATH = PROJECT_ROOT / "data" / "warehouse.duckdb"

RAW_TABLES = ["customers", "products", "orders", "order_items"]


def get_connection() -> duckdb.DuckDBPyConnection:
    """Single point of connection config.

    Swap this function's body for e.g.:
        import snowflake.connector
        return snowflake.connector.connect(account=..., warehouse=..., ...)
    to point the exact same pipeline at a production Snowflake account.
    """
    return duckdb.connect(str(WAREHOUSE_PATH))


def load_raw_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    for table in RAW_TABLES:
        csv_path = RAW_DIR / f"{table}.csv"
        if not csv_path.exists():
            raise FileNotFoundError(
                f"{csv_path} not found — run ingestion/generate_raw_data.py first."
            )
        con.execute(
            f"""
            CREATE OR REPLACE TABLE raw.{table} AS
            SELECT *, current_timestamp AS _loaded_at
            FROM read_csv_auto('{csv_path.as_posix()}', header=True)
            """
        )
        count = con.execute(f"SELECT COUNT(*) FROM raw.{table}").fetchone()[0]
        print(f"  raw.{table:<15} {count:>7,} rows loaded")


def main() -> None:
    con = get_connection()
    print(f"Connected to warehouse: {WAREHOUSE_PATH}")
    load_raw_tables(con)
    con.close()
    print("Load complete.")


if __name__ == "__main__":
    main()
