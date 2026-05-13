# Standard library imports
from __future__ import annotations
from pathlib import Path

# Third-party imports
import duckdb

# Local imports
from widelog.config import load_config


OBJECTS_VIEW_SQL = """
    CREATE OR REPLACE VIEW objects AS
    SELECT
        meta.snr_id AS snr_id,
        main.*,
        meta.*
    FROM meta
    LEFT JOIN main 
        ON main.record_id = meta.snr_id
"""

IMU_FILES_VIEW_SQL = """
    CREATE OR REPLACE VIEW imu_files AS
    SELECT
        snr_id,
        timeline_stage,
        test_type,
        file_path,
        folder,
        file_name
    FROM tests_index;
"""

OBJECTS_WITH_IMU_VIEW_SQL = """
    CREATE OR REPLACE VIEW objects_with_imu AS
    SELECT
        meta.snr_id AS snr_id,
        main.record_id,
        tests_index.timeline_stage,
        tests_index.test_type,
        tests_index.file_path
    FROM meta
    LEFT JOIN main
        ON main.record_id = meta.snr_id
    LEFT JOIN tests_index
        ON tests_index.snr_id = meta.snr_id;
"""


def build_db() -> None:
    """
    Build the DuckDB database from processed parquet files.

    The database contains:
    - main table
    - meta table
    - tests_index table

    As well as helper SQL views used by the application.
    """

    cfg = load_config()

    processed_dir = Path(cfg.out_dir)

    db_path = Path(cfg.duckdb_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    main_parquet = processed_dir / "main.parquet"
    meta_parquet = processed_dir / "meta.parquet"
    index_parquet = processed_dir / "tests_index.parquet"

    required_files = [
        main_parquet,
        meta_parquet,
        index_parquet
    ]

    for parquet_file in required_files:
        if not parquet_file.exists():
            raise FileNotFoundError(f"Missing file: {parquet_file}")

    with duckdb.connect(database=str(db_path)) as con:

        # Create tables from parquet files.
        con.execute(
            "CREATE OR REPLACE TABLE main AS SELECT * FROM read_parquet(?)",
            [str(main_parquet)]
        )

        con.execute(
            "CREATE OR REPLACE TABLE meta AS SELECT * FROM read_parquet(?)",
            [str(meta_parquet)]
        )
        con.execute(
            """CREATE OR REPLACE TABLE tests_index AS
            SELECT * 
            FROM read_parquet(?)
            """,
            [str(index_parquet)]
        )

        # Create application view.
        con.execute(OBJECTS_VIEW_SQL)
        con.execute(IMU_FILES_VIEW_SQL)
        con.execute(OBJECTS_WITH_IMU_VIEW_SQL)

if __name__ == "__main__":
    build_db()