# Import
from __future__ import annotations
from pathlib import Path
from turtle import st
import polars as pl
import duckdb

# Local imports
from widelog.config import load_config
from widelog.query_service import (
    get_connection,
    NULLS
)

# Return the file informations as a list of tuples (file_path, timeline_stage, test_type, snr_id) for the given filters
def get_imu_files(snr_id: str, timeline_stage: str, test_type: str) -> list[tuple[str, str, str, str]]:

    # Dynamic SQL query construction
    query = """
    SELECT
        file_path,
        timeline_stage,
        test_type,
        snr_id
    FROM objects_with_imu
    WHERE 1=1
    """

    params = []

    # Check if snr_id is "all" or a specific id
    if snr_id != "all":
        # Support of the multi-ID query with "xxx,xxx,xxx"
        if "," in snr_id:
            snr_ids = [s.strip() for s in snr_id.split(",")]
            query += " AND snr_id IN ({})".format(",".join("?" for _ in snr_ids))
            params.extend(snr_ids)
        else:
            query += " AND snr_id = ?"
            params.append(snr_id)

    if timeline_stage not in NULLS:
        query += " AND timeline_stage = ?"
        params.append(timeline_stage)

    if test_type not in NULLS:
        query += " AND test_type = ?"
        params.append(test_type)

    # Execute the query and fetch results
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query, params)
        results = cursor.fetchall()

    # Return a list of tuples (file_path, timeline_stage, test_type, snr_id)

    file_infos = []
    for row in results:
        if None in row:
            continue
        file_infos.append((row[0], row[1], row[2], row[3]))

    return file_infos

# Takes a list of file infos (file_path, timeline_stage, test_type, snr_id) and merges the CSV files into a single CSV file at the given output path
def merge_csv_files(file_infos: list[tuple[str, str, str, str]], out_path: str) -> tuple[str, str]:
    dfs_gait = []
    dfs_dlsm = []
    has_gait = False
    has_dlsm = False

    for file_path, stages, test_type, snr_id in file_infos:
        if file_path is None:
            continue

        # Check if the file is gait or dlsm and read the CSV file with the appropriate columns and types
        if "gait" in test_type.lower():
            df = pl.read_csv(
                file_path,
                has_header=False,
                new_columns=[
                    "date",
                    "acc_x",
                    "acc_y",
                    "acc_z",
                    "gyr_x",
                    "gyr_y",
                    "gyr_z"
                ],
                try_parse_dates=True,
                null_values=NULLS
            )
            # Add the metadata columns to the dataframe
            df_gait = df.with_columns([
                pl.lit(stages).alias("timeline_stage"),
                pl.lit(test_type).alias("test_type"),
                pl.lit(snr_id).alias("snr_id")
            ])

            # Append the dataframe to the list of dataframes to merge
            dfs_gait.append(df_gait)

            has_gait = True


       
        elif "dlsm" in test_type.lower() and "gait" not in test_type.lower():
            df = pl.read_csv(
                file_path,
                has_header=True,
                try_parse_dates=True,
                null_values=NULLS
            )
            # Add the metadata columns to the dataframe
            df_dlsm = df.with_columns([
                pl.lit(stages).alias("timeline_stage"),
                pl.lit(test_type).alias("test_type"),
                pl.lit(snr_id).alias("snr_id")
            ])
            
            # Append the dataframe to the list of dataframes to merge
            dfs_dlsm.append(df_dlsm)

            has_dlsm = True

        else:
            continue

    # Set the order of the sorting
    STAGE_ORDER = {
        "admission": 1,
        "discharge": 2,
        "w3": 3,
        "w6": 4,
        "w8": 5,
        "FU1": 6,
        "FU2": 7
    }

    # Merge the dataframes
    if has_gait and not has_dlsm:
        merged_df = pl.concat(dfs_gait)

        # Sort the merged dataframe
        merged_df = merged_df.with_columns(
            pl.col("timeline_stage")
            .map_elements(lambda x: STAGE_ORDER.get(x, 99))
            .alias("stage_order")
        ).sort(["snr_id", "stage_order", "test_type"]).drop("stage_order")

        merged_df.write_csv(out_path)

        return out_path, out_path

    # Only dlsm files
    elif has_dlsm and not has_gait:
        merged_df = pl.concat(dfs_dlsm)

        # Sort the merged dataframe
        merged_df = merged_df.with_columns(
            pl.col("timeline_stage")
            .map_elements(lambda x: STAGE_ORDER.get(x, 99))
            .alias("stage_order")
        ).sort(["snr_id", "stage_order", "test_type"]).drop("stage_order")

        merged_df.write_csv(out_path)

        return out_path, out_path
    
    else:
        merge_df_gait = pl.concat(dfs_gait)
        merge_df_dlsm = pl.concat(dfs_dlsm)

        # Sort the merged dataframes
        merge_df_gait = merge_df_gait.with_columns(
            pl.col("timeline_stage")
            .map_elements(lambda x: STAGE_ORDER.get(x, 99))
            .alias("stage_order")
        ).sort(["snr_id", "stage_order", "test_type"]).drop("stage_order")

        merge_df_dlsm = merge_df_dlsm.with_columns(
            pl.col("timeline_stage")
            .map_elements(lambda x: STAGE_ORDER.get(x, 99))
            .alias("stage_order")
        ).sort(["snr_id", "stage_order", "test_type"]).drop("stage_order")

        out_path_gait = out_path.replace(".csv", "_gait.csv")
        out_path_dlsm = out_path.replace(".csv", "_dlsm.csv")

        merge_df_gait.write_csv(out_path_gait)
        merge_df_dlsm.write_csv(out_path_dlsm)

        return out_path_gait, out_path_dlsm

# This function chooses the name of the output CSV file based on the provided parameters
def choose_path_name(snr_id: str, timeline_stage: str, test_type: str) -> str:
    snr_part = "ALL" if snr_id == "all" else snr_id
    stage_part = timeline_stage if timeline_stage else "ALL"
    type_part = test_type if test_type else "ALL"

    return f"SNR_{snr_part}_{stage_part}_{type_part}.csv"

# This function retrieves the file paths of the IMU CSV files based on the provided parameters and then merges them into a single CSV file.
def imu_csv_export(snr_id: str, timeline_stage: str = "", test_type: str = "") -> tuple[Path, Path]:
    # Load the config
    cfg = load_config()

    # Get the all the files and metadata
    file_infos = get_imu_files(snr_id=snr_id, timeline_stage=timeline_stage, test_type=test_type)

    # Check if we have files to merge
    if not file_infos:
        raise ValueError("No IMU files found for the given filters")

    # Choose the output file name 
    out_csv = choose_path_name(snr_id, timeline_stage, test_type)

    out_path = Path(cfg.export_dir) / out_csv

    # Merge all files into one parquet file from the list of file paths and write it to the output path
    out_path1, out_path2 = merge_csv_files(file_infos, str(out_path))

    return Path(out_path1), Path(out_path2)

if __name__ == "__main__":    # Example usage
    # Get one file for id 193, for all timeline_stage and test_type "gait"
    out_path1, out_path2 = imu_csv_export(snr_id="193", timeline_stage="admission", test_type="gait")

    