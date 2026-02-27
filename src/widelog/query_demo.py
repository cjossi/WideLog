from __future__ import annotations
import duckdb

#local imports
from .config import load_config

def main() -> None:
    # Load configuration from the config.yaml file
    cfg = load_config()

    # Connect to the DuckDB database
    con = duckdb.connect(database=str(cfg.duckdb_path))

    # Example query : get the number of objects
    print("Number of objects:")
    print(con.execute("SELECT COUNT(*) FROM objects").fetchone())

    # Example query : get the an example of objects
    print("\nExample of objects:")
    print(con.execute("""
        SELECT snr_id, timeline_stage, test_type
        FROM objects_with_imu
        LIMIT 5
    """).df())

    # Example query : indexed IMU tests
    print("\nExample of indexed IMU tests:")
    print(con.execute("""
        SELECT snr_id, timeline_stage, test_type
        FROM objects_with_imu
    """).df())

    # Export CSV for patient SNR193
    print("\nExporting CSV for patient SNR193...")
    df = con.execute("""
        SELECT *
        FROM objects_with_imu
        WHERE snr_id = '193'
        AND timeline_stage IS NOT NULL;
    """).df()
    df.to_csv("SNR193_imu_files.csv", index=False)
    print("Done")

    con.close()

if __name__ == "__main__":
    main()