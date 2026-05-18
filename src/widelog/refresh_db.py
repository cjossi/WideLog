# Standard library imports
from __future__ import annotations


# Local imports
from widelog.build_db import build_db
from widelog.csv_to_parquet import ingest_csv_to_parquet
from widelog.source_snapshot import (
    build_source_snapshot,
    save_snapshot
)
from widelog.tests_index import ingest_tests_index


def refresh_db() -> None:
    """
    Refresh the entire WideLog database pipeline.

    The refresh process:
    1. Converts raw CSV files to Parquet format.
    2. Ingests the tests index data.
    3. Rebuilds the DuckDB database from the Parquet files.
    4. Builds a snapshot of the source data for reproducibility.
    """
    print("Refreshing WideLog database...")

    ingest_csv_to_parquet()

    ingest_tests_index()

    build_db()

    snapshot = build_source_snapshot()

    snapshot_path = save_snapshot(snapshot)

    print(f"Snapshot saved to: {snapshot_path}")
    
    print("Refresh complete.")

if __name__ == "__main__":
    refresh_db()