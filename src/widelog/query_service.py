# Import
from __future__ import annotations
import duckdb

# Local imports
from widelog.config import load_config


def get_connection() -> duckdb.DuckDBPyConnection:
    cfg = load_config()
    con = duckdb.connect(database=str(cfg.duckdb_path))
    return con


def snr_exists(snr_id: str) -> bool:
    con = get_connection()
    try:
        result = con.execute("""
            SELECT 1 
            FROM objects 
            WHERE snr_id = ? 
            LIMIT 1
        """, [snr_id]).fetchone()
        return result is not None
    finally:
        con.close()


def get_total_patients() -> int:
    con = get_connection()
    try:
        result = con.execute("""
            SELECT COUNT(DISTINCT snr_id) 
            FROM objects
        """).fetchone()

        if result is None:
            return 0
        
        return result[0]
    
    finally:
        con.close()


def get_total_patients_with_imu() -> int:
    con = get_connection()
    try:
        result = con.execute("""
            SELECT COUNT(DISTINCT snr_id) 
            FROM objects_with_imu
        """).fetchone()

        if result is None:
            return 0
        
        return result[0]
    
    finally:
        con.close()


def get_timeline_stages_distribution():
    con = get_connection()
    try:
        results = con.execute("""
            SELECT timeline_stage, COUNT(*) AS count
            FROM objects_with_imu
            WHERE timeline_stage IS NOT NULL
            GROUP BY timeline_stage
            ORDER BY
                CASE timeline_stage
                    WHEN 'admission' THEN 1
                    WHEN 'discharge' THEN 2
                    WHEN 'FU1' THEN 3
                    WHEN 'FU2' THEN 4
                    ELSE 99
                END
        """).df()

        return results
    
    finally:
        con.close()


def get_test_types_distribution():
    con = get_connection()
    try:
        results = con.execute("""
            SELECT test_type, COUNT(*) AS count
            FROM objects_with_imu
            WHERE test_type IS NOT NULL
            GROUP BY test_type
            ORDER BY test_type
        """).df()

        return results
    
    finally:
        con.close()


def get_patient_info(snr_id: str):
    con = get_connection()
    try:
        result = con.execute("""
            SELECT *
            FROM objects
            WHERE snr_id = ?
        """, [snr_id]).df()

        return result
    
    finally:
        con.close()


def get_available_stages(snr_id: str) -> list[str]:
    con = get_connection()
    try:
        rows = con.execute("""
            SELECT DISTINCT timeline_stage
            FROM objects_with_imu
            WHERE snr_id = ?
              AND timeline_stage IS NOT NULL
            ORDER BY
                CASE timeline_stage
                    WHEN 'admission' THEN 1
                    WHEN 'FU1' THEN 2
                    WHEN 'FU2' THEN 3
                    WHEN 'discharge' THEN 4
                    ELSE 99
                END
        """, [snr_id]).fetchall()
        return [row[0] for row in rows]
    finally:
        con.close()


def get_available_test_types(snr_id: str, timeline_stage: str | None = None) -> list[str]:
    con = get_connection()
    try:
        if timeline_stage:
            rows = con.execute("""
                SELECT DISTINCT test_type
                FROM objects_with_imu
                WHERE snr_id = ?
                  AND timeline_stage = ?
                  AND test_type IS NOT NULL
                ORDER BY test_type
            """, [snr_id, timeline_stage]).fetchall()
        else:
            rows = con.execute("""
                SELECT DISTINCT test_type
                FROM objects_with_imu
                WHERE snr_id = ?
                  AND test_type IS NOT NULL
                ORDER BY test_type
            """, [snr_id]).fetchall()

        return [row[0] for row in rows]
    finally:
        con.close()


def get_imu_files(snr_id: str, timeline_stage: str | None = None, test_type: str | None = None):
    con = get_connection()
    try:
        query = """
            SELECT snr_id, timeline_stage, test_type, file_path
            FROM objects_with_imu
            WHERE snr_id = ?
        """
        params = [snr_id]

        if timeline_stage:
            query += " AND timeline_stage = ?"
            params.append(timeline_stage)

        if test_type:
            query += " AND test_type = ?"
            params.append(test_type)

        query += """
            ORDER BY
                CASE timeline_stage
                    WHEN 'admission' THEN 1
                    WHEN 'FU1' THEN 2
                    WHEN 'FU2' THEN 3
                    WHEN 'discharge' THEN 4
                    ELSE 99
                END,
                test_type
        """

        return con.execute(query, params).df()
    finally:
        con.close()

def get_basic_stats(table_name: str, column_name: str) -> list[tuple[str, str]]:
    allowed_table = ["main", "meta", "objects"]

    if table_name not in allowed_table:
        raise ValueError(f"Invalid table name. Allowed values are: {allowed_table}")

    con = get_connection()

    try:
        result = con.execute(f"""
            SELECT
                AVG({column_name}) AS mean,
                MIN({column_name}) AS min,
                MAX({column_name}) AS max
            FROM {table_name}
        """).fetchall()

        return [(row[0], row[1]) for row in result]
    
    finally:        
        con.close()


def main():
    list = get_timeline_stages_distribution()
    print(list)

if __name__ == "__main__":
    main()