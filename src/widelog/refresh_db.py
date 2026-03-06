# Import
from __future__ import annotations


# Local imports
from widelog.ingest import ingest_csv_to_parquet
from widelog.tests_index import ingest_tests_index
from widelog.build_db import build_db
from widelog.source_snapshot import build_source_snapshot, save_snapshot

def refresh_db():
    print("Refreshing WideLog database...")

    ingest_csv_to_parquet()
    ingest_tests_index()
    build_db()

    snapshot = build_source_snapshot()
    path = save_snapshot(snapshot)

    print(f"Snapshot saved to: {path}")
    print("Refresh complete.")

if __name__ == "__main__":
    refresh_db()