from widelog.ingest import ingest_csv_to_parquet
from widelog.tests_index import ingest_tests_index
from widelog.build_db import build_db

def refresh_db():
    print("Ingesting CSV files to Parquet...")
    ingest_csv_to_parquet()

    print("Building tests index...")
    ingest_tests_index()

    print("Building DuckDB database...")
    build_db()

if __name__ == "__main__":
    refresh_db()