# Standard library imports
from __future__ import annotations

# Third-party imports
from appscript import con
import duckdb
import polars as pl
import pandas as pd

# Local imports
from widelog.config import load_config
from widelog.constants import (
    ALL,
    STAGE_ORDER
)


def get_connection() -> duckdb.DuckDBPyConnection:
    """
    Return a connection to the DuckDB database.
    """
    cfg = load_config()

    return duckdb.connect(database=str(cfg.duckdb_path))

def add_snr_filter(
        query: str,
        params: list[str],
        snr_id: str
) -> tuple[str, list[str]]:
    """
    Add an SNR ID filter to a SQL query

    Supports:
    - Single SNR ID (e.g., "001"). 
    - comma-separated SNR IDs (e.g., "001, 002, 003")
    - "all" to include all SNR IDs
    """

    params = []

    if snr_id == ALL:
        return query, params

    if "," in snr_id:
        snr_ids = [s.strip() for s in snr_id.split(",")]

        placeholders = ",".join("?" for _ in snr_ids)

        query += " AND snr_id IN ({placeholders})"
        params.extend(snr_ids)

    else:
        query += " AND snr_id = ?"
        params.append(snr_id)

    return query, params

def get_stage_order_sql(column_name: str = "timeline_stage") -> str:
    """
    Return a SQL CASE statement to order timeline stages in a specific order.
    The order is defined in the STAGE_ORDER constant.
    """

    cases = "\n".join(
        f"WHEN '{stage}' THEN {order}"
        for stage, order in STAGE_ORDER.items()
    )

    return f"""
        CASE {column_name}
            {cases}
            ELSE 99
        END
    """

def snr_exists(snr_id: str) -> bool:
    """
    Check wehther a patient exists in the database.
    """

    with get_connection() as con:
        result = con.execute("""
            SELECT 1 
            FROM objects 
            WHERE snr_id = ? 
            LIMIT 1
        """, [snr_id]).fetchone()
    
    return result is not None

def get_total_patients() -> int:
    """
    Return the total number of patients in the database.
    """

    with get_connection() as con:
        result = con.execute("""
            SELECT COUNT(DISTINCT snr_id) 
            FROM objects
        """).fetchone()
        
    return result[0] if result else 0

def get_total_patients_with_imu() -> int:
    """
    Return the total number of patients with IMU data in the database.
    """
    with get_connection() as con:
        result = con.execute("""
            SELECT COUNT(DISTINCT snr_id) 
            FROM objects_with_imu
        """).fetchone()
        
    return result[0] if result else 0

def get_timeline_stages_distribution() -> pd.DataFrame:
    """
    Return the distribution of timeline stages in the database as a Pandas
    Dataframe.
    """

    stage_order_sql = get_stage_order_sql()

    with get_connection() as con:
        results = con.execute(f"""
            SELECT timeline_stage, COUNT(*) AS count
            FROM objects_with_imu
            WHERE timeline_stage IS NOT NULL
            GROUP BY timeline_stage
            ORDER BY {stage_order_sql}
        """).df()

    return results

def get_test_types_distribution() -> pd.DataFrame:
    """
    Return the distribution of test types in the database as a
    Pandas Dataframe.
    """

    with get_connection() as con:
        results = con.execute("""
            SELECT test_type, COUNT(*) AS count
            FROM objects_with_imu
            WHERE test_type IS NOT NULL
            GROUP BY test_type
            ORDER BY test_type
        """).df()

    return results

def get_patient_info(snr_id: str) -> pd.DataFrame:
    """
    Return all available information for a given patient.
    """

    with get_connection() as con:
        result = con.execute("""
            SELECT *
            FROM objects
            WHERE snr_id = ?
        """, [snr_id]).df()

    return result

def get_available_stages(snr_id: str) -> list[str]:
    """
    Return the abailable timeline stages for the given SNR IDs.
    """

    query = """
        SELECT DISTINCT timeline_stage
        FROM objects_with_imu
        WHERE timeline_stage IS NOT NULL
    """

    params: list[str] = []

    query, params = add_snr_filter(query, params, snr_id)

    stage_order_sql = get_stage_order_sql()

    query += f"""
        ORDER BY {stage_order_sql}
    """

    with get_connection() as con:
        rows = con.execute(query, params).fetchall()

    return [row[0] for row in rows]

def get_available_test_types(
        snr_id: str,
        timeline_stage: str | None = None
) -> list[str]:
    """
    Return all available test types for the selected SNR IDs and timeline stage.
    """

    query = """
        SELECT DISTINCT test_type
        FROM objects_with_imu
        WHERE test_type IS NOT NULL
    """

    params: list[str] = []

    query, params = add_snr_filter(query, params, snr_id)

    if timeline_stage:
        query += " AND timeline_stage = ?"
        params.append(timeline_stage)

    query += " ORDER BY test_type"

    with get_connection() as con:
        rows = con.execute(query, params).fetchall()

    return [row[0] for row in rows]

def get_basic_stats(
        table_name: str,
        column_name: str
) -> list[tuple[float, float, float]]:
    """
    Return the mean, minimum and maximum values of a given column.
    """

    allowed_table = ["main", "meta", "objects"]

    if table_name not in allowed_table:
        raise ValueError(
            f"Invalid table name. Allowed values are: {allowed_table}"
        )

    with get_connection() as con:
        result = con.execute(f"""
            SELECT
                AVG({column_name}) AS mean,
                MIN({column_name}) AS min,
                MAX({column_name}) AS max
            FROM {table_name}
        """).fetchall()

    return result

def get_all_characteristics() -> list[str]:
    """
    Return all column names from the objects view in the database.
    """

    with get_connection() as con:
        result = con.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'objects'
        """).fetchall()

    return [row[0] for row in result]

def value_exists_objects(
        column_name: str,
        value: str
) -> bool:
    """
    Check wether a value exists in a given column of the objects view
    in the database.
    """

    with get_connection() as con:
        result = con.execute(f"""
            SELECT 1
            FROM objects
            WHERE {column_name} = ?
            LIMIT 1
        """, [value]).fetchone()

    return result is not None

def main() -> None:
    """
    Run a simple test query.
    """

    timeline_distribution = get_timeline_stages_distribution()
    
    print(timeline_distribution)

if __name__ == "__main__":
    main()