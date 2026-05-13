# Standard library imports
from __future__ import annotations
from pathlib import Path
from dataclasses import dataclass

# Third-party imports
import polars as pl

# Local imports
from widelog.config import load_config
from widelog.query_service import get_connection
from widelog.constants import (
    ALL,
    GAIT,
    DLSM,
    STAGE_ORDER,
    NULLS
)


@dataclass
class IMUFileInfo:
    """Store metadata ascociated with an IMU CSV file."""
    file_path: str | None
    timeline_stage: str
    test_type: str
    snr_id: str


def sort_imu_dataframe(dfs: list[pl.DataFrame]) -> pl.DataFrame:
        """
        Concatenate and sort IMU dataframes.

        Dataframes are sorted by:
        1. SNR ID (ascending)
        2. Timeline stage (admission, discharge, w3, w6, w8, FU1, FU2)
        3. Test type (gait before dlsm)
        """

        merged_df = pl.concat(dfs)

        # Timeline stages are sorted manually because alphabetical
        # ordering does not match the clinical timeline order.
        merged_df = merged_df.with_columns(
            pl.col("timeline_stage")
            .map_elements(lambda x: STAGE_ORDER.get(x, 99))
            .alias("stage_order")
        ).sort(["snr_id", "stage_order", "test_type"]).drop("stage_order")

        return merged_df

def get_imu_files(
        snr_id: str,
        timeline_stage: str,
        test_type: str
) -> list[IMUFileInfo]:
    """
    Retrive IMU file metadata matching the given filters.

    Supports:
    - Single SNR ID (e.g., "193")
    - Multiple comma-separated SNR IDs (e.g., "001,002,003")
    - Global export using "all" for SNR ID, timeline stage, and test type.
    """

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

    if snr_id != ALL:
        # Support comma-separated SNR IDs such as:
        # "001, 002, 003"
        if "," in snr_id:
            snr_ids = [s.strip() for s in snr_id.split(",")]

            query += " AND snr_id IN ({})".format(
                ",".join("?" for _ in snr_ids)
            )

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

    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query, params)
        results = cursor.fetchall()

    file_infos = []

    for row in results:
        # Skip incomplete database entries
        if None in row:
            continue

        file_infos.append(
            IMUFileInfo(
                file_path=row[0],
                timeline_stage=row[1],
                test_type=row[2],
                snr_id=row[3]
            )
        )

    return file_infos

def read_imu_files(
        file_infos: list[IMUFileInfo]
) -> tuple[list[pl.DataFrame], list[pl.DataFrame]]:
    """
    Read IMU CSV files and separate them into gait and DLSM datasets.

    Returns
    -------
    tuple[list[pl.DataFrame], list[pl.DataFrame]]
        A tuple containing:
        - gait datagrames
        - DLSM datagrames
    """

    dfs_gait = []
    dfs_dlsm = []

    for file_info in file_infos:
        if file_info.file_path is None:
            continue

        if file_info.test_type == GAIT:
            df = pl.read_csv(
                file_info.file_path,
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

            df_gait = df.with_columns([
                pl.lit(file_info.timeline_stage).alias("timeline_stage"),
                pl.lit(file_info.test_type).alias("test_type"),
                pl.lit(file_info.snr_id).alias("snr_id")
            ])

            dfs_gait.append(df_gait)

        elif file_info.test_type == DLSM:
            df = pl.read_csv(
                file_info.file_path,
                has_header=True,
                try_parse_dates=True,
                null_values=NULLS
            )

            df_dlsm = df.with_columns([
                pl.lit(file_info.timeline_stage).alias("timeline_stage"),
                pl.lit(file_info.test_type).alias("test_type"),
                pl.lit(file_info.snr_id).alias("snr_id")
            ])
            
            dfs_dlsm.append(df_dlsm)

        else:
            continue

    return dfs_gait, dfs_dlsm

def merge_csv_files(
        file_infos: list[IMUFileInfo],
        out_path: str
) -> tuple[str, str]:
    """
    Merge IMU CSV files into one or two output CSV files.

    Depending on the selected test types:
    - only gait data is exported
    - only DLSM data is exported
    - both are exported into separate files,
      with suffixes "_gait.csv" and "_dlsm.csv" respectively.
    """

    dfs_gait, dfs_dlsm = read_imu_files(file_infos)

    if dfs_gait and not dfs_dlsm:
        merged_df = sort_imu_dataframe(dfs_gait)

        merged_df.write_csv(out_path)

        return out_path, out_path

    if dfs_dlsm and not dfs_gait:
        merged_df = sort_imu_dataframe(dfs_dlsm)

        merged_df.write_csv(out_path)

        return out_path, out_path

    merge_df_dlsm = sort_imu_dataframe(dfs_dlsm)
    merge_df_gait = sort_imu_dataframe(dfs_gait)

    out_path_gait = out_path.replace(".csv", "_gait.csv")
    out_path_dlsm = out_path.replace(".csv", "_dlsm.csv")

    merge_df_gait.write_csv(out_path_gait)
    merge_df_dlsm.write_csv(out_path_dlsm)

    return out_path_gait, out_path_dlsm

def choose_path_name(
        snr_id: str,
        timeline_stage: str,
        test_type: str
) -> str:
    """Generate the output CSV filename from the selected filters."""

    snr_part = "ALL" if snr_id == ALL else snr_id
    stage_part = timeline_stage if timeline_stage else ALL
    type_part = test_type if test_type else ALL

    return f"SNR_{snr_part}_{stage_part}_{type_part}.csv"

def imu_csv_export(
        snr_id: str,
        timeline_stage: str = "",
        test_type: str = ""
) -> tuple[Path, Path]:
    """
    Export IMU CSV files matching the selected filters.

    Returns
    -------
    tuple[Path, Path]
        Paths to the exported CSV files.
        If only one file is generated, both paths are identical.
    """

    cfg = load_config()

    file_infos = get_imu_files(
        snr_id=snr_id,
        timeline_stage=timeline_stage,
        test_type=test_type
    )

    if not file_infos:
        raise ValueError("No IMU files found for the given filters")

    # Choose the output file name 
    out_csv = choose_path_name(
        snr_id,
        timeline_stage,
        test_type
    )

    out_path = cfg.export_dir / out_csv

    out_path1, out_path2 = merge_csv_files(
        file_infos,
        str(out_path)
    )

    return Path(out_path1), Path(out_path2)

if __name__ == "__main__":
    out_path1, out_path2 = imu_csv_export(
        snr_id="193",
        timeline_stage="admission",
        test_type="gait")

    