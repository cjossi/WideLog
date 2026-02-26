# Import
from __future__ import annotations
from pathlib import Path
import duckdb

# Local imports
from .config import load_config

def main() -> None:
    # Load configuration from the config.yaml file
    cfg = load_config()

    # Define the output directory based on the configuration
    processed = Path(cfg.out_dir)
    db_path = Path(cfg.duckdb_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    # Define paths to the Parquet files
    main_parquet = processed / "main.parquet"
    meta_parquet = processed / "meta.parquet"
    index_parquet = processed / "tests_index.parquet"

    # Check that the required Parquet files exist before proceeding
    for p in [main_parquet, meta_parquet, index_parquet]:
        if not p.exists():
            raise FileNotFoundError(f"Missing file: {p}")
        
    

    # Connect to a DuckDB database (or create it if it doesn't exist)
    con = duckdb.connect(database=str(db_path))

    # Create tables from the Parquet files
    con.execute("CREATE OR REPLACE TABLE main AS SELECT * FROM read_parquet(?)", [str(main_parquet)])
    con.execute("CREATE OR REPLACE TABLE meta AS SELECT * FROM read_parquet(?)", [str(meta_parquet)])
    con.execute("CREATE OR REPLACE TABLE tests_index AS SELECT * FROM read_parquet(?)", [str(index_parquet)])

    print("Database created at:", db_path)

    con.execute("""
        CREATE OR REPLACE VIEW objects AS
        SELECT
            me.id       AS object_id,
            me.snr_id   AS snr_id,
            m.*,
            me.*
        FROM meta me
        LEFT JOIN main m
            ON m.patient_snr_id = me.snr_id;
    """)

    con.execute("""
        CREATE OR REPLACE VIEW objects_with_tests AS
        SELECT
            me.id       AS object_id,
            me.snr_id   AS snr_id,
            ti.timeline_stage,
            ti.test_type,
            ti.test_name
        FROM meta me
        LEFT JOIN main m
            ON m.patient_snr_id = me.snr_id
        LEFT JOIN tests_index ti
            ON ti.object_id = me.id;
    """)

    con.close()
    print(f"Wrote: {db_path}")
    print("Tables: main, meta, tests_index")
    print("View: objects_with_tests")

if __name__ == "__main__":
    main()