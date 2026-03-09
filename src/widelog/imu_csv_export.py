# Import
from __future__ import annotations
from pathlib import Path
import polars as pl
import duckdb

# Local imports
from widelog.config import load_config

NULLS = ["", "NA", "null", "NULL", "not possible"]

# This function retrieves the file paths of the IMU CSV files and returns them as a list of tuples (file_path, timeline_stage, test_type)
def get_imu_csv_path(snr_id: str, timeline_stage: str, test_type: str) -> tuple[list[tuple[str, str, str]], int]:
    # Load the config
    cfg = load_config()

    # Connect to the DuckDB database
    con = duckdb.connect(database=str(cfg.duckdb_path))

    # Initialisation
    file_infos = []             # List of file paths to return
    case = 0                    # Case number, to know which combination of parameters was used to retrieve the files

    # Check if the snr_id exists in the database
    results = con.execute("""
        SELECT snr_id
        FROM objects_with_imu
        WHERE snr_id = ?;
    """, [snr_id])
    row = results.fetchone()
    if row is None:
        raise ValueError(f"No data found for snr_id: {snr_id}")

    ### ---------IF CASES--------- ###
    # Check if all informations are provided to return vanilla csv
    if timeline_stage not in NULLS and test_type not in NULLS:
        # Get the path of the IMU file 
        results = con.execute("""
            SELECT
                snr_id,
                timeline_stage,
                test_type,
                file_path
            FROM objects_with_imu
            WHERE snr_id = ?
            AND timeline_stage = ?
            AND test_type = ?;
        """, [snr_id, timeline_stage, test_type])

        # Fetch the first row of the results
        row = results.fetchone()

        # If no row is found, raise an error
        if row is None:
            raise ValueError("Case 1: No matching IMU file found")

        # Get the file path from the row and print it
        file_infos = [(row[3], row[1], row[2])] # List of one tuple (file_path, timeline_stage, test_type)
        case = 1


    # If timeline_stage is not provided, return all files for the snr_id and test_type
    elif timeline_stage in NULLS and test_type not in NULLS:
        # Get the path of the IMU files for the snr_id and test_type
        results = con.execute("""
            SELECT
                snr_id,
                timeline_stage,
                test_type,
                file_path
            FROM objects_with_imu
            WHERE snr_id = ?
            AND test_type = ?;
        """, [snr_id, test_type])

        # Fetch all rows of the results
        rows = results.fetchall()
        if len(rows) == 0:
            raise ValueError("Case 2: No matching IMU files found")
        
        # Get the file paths from the rows and print them
        file_infos = [(row[3], row[1], row[2]) for row in rows] # List of tuples (file_path, timeline_stage, test_type)
        case = 2

    # If test_type is not provided, return all files for the snr_id and timeline_stage
    elif timeline_stage not in NULLS and test_type in NULLS:
        # Get the path of the IMU files for the snr_id and timeline_stage
        results = con.execute("""
            SELECT
                snr_id,
                timeline_stage,
                test_type,
                file_path
            FROM objects_with_imu
            WHERE snr_id = ?
            AND timeline_stage = ?;
        """, [snr_id, timeline_stage])

        # Fetch all rows of the results
        rows = results.fetchall()
        if len(rows) == 0:
            raise ValueError("Case 3: No matching IMU files found")
        
        # Get the file paths from the rows and print them
        file_infos = [(row[3], row[1], row[2]) for row in rows] # List of tuples (file_path, timeline_stage, test_type)
        case = 3

    elif timeline_stage in NULLS and test_type in NULLS:
        #Get the path of the IMU files for the snr_id
        results = con.execute("""
            SELECT
                snr_id,
                timeline_stage,
                test_type,
                file_path
            FROM objects_with_imu
            WHERE snr_id = ?;
        """, [snr_id])

        # Fetch all rows of the results
        rows = results.fetchall()
        if len(rows) == 0:
            raise ValueError("Case 4: No matching IMU files found")
        
        # Get the file paths from the rows and print them
        file_infos = [(row[3], row[1], row[2]) for row in rows] # List of tuples (file_path, timeline_stage, test_type)
        case = 4
    
    
    #　For secutity reason, we don't want to return all files for a snr_id if both timeline_stage and test_type are not provided
    else :
        raise ValueError("Case 4: Invalid combination of parameters. Please provide either both timeline_stage and test_type, or only test_type.")    
    
    # Close the database connection
    con.close()

    # Return the files paths
    return file_infos, case

# This function merges multiple CSV files into a single CSV file. From a list of paths
def merge_csv_files(file_infos: list[tuple[str, str, str]], case: int,  out_csv: str) -> None:
    # Reorder le file_infos
    STAGE_ORDER = {
        "admission": 0,
        "discharge": 1,
        "FU1": 2,
        "FU2": 3,
    }
    file_infos.sort(key=lambda x: (STAGE_ORDER.get(x[1], 999), x[2])) # Sort by timeline_stage using the defined order and then by test_type alphabetically


    # Read all CSV files into a single Polars DataFrame
    # Dataframe used here instead of lazyframe because we want to merge all files into one csv, and the number of files is not expected to be very large (max 8 files for one snr_id)
    dfs = []
    for file_path, timeline_stage, test_type in file_infos:
        df = pl.read_csv(
            file_path,
            has_header=False,
            new_columns=[
                "date",
                "gyr_x",
                "gyr_y",
                "gyr_z",
                "acc_x",
                "acc_y",
                "acc_z",
            ],
            try_parse_dates=True,
            null_values=NULLS
        )
        # Check if we need to have the timeline_stage and/or test_type columns in the merged csv, depending on the case
        if case == 1:
            df.write_csv(out_csv) # If we have all the information, we can just write the csv without merging
            print(f"Case {case}: Only one file to merge, written directly to {out_csv}")
            return
        
        if case == 2:
            df = df.with_columns([
                pl.lit(timeline_stage).alias("timeline_stage")
            ])
            

        elif case == 3:
            df = df.with_columns([
                pl.lit(test_type).alias("test_type")
            ])

        elif case == 4:
            df = df.with_columns([
                pl.lit(timeline_stage).alias("timeline_stage"),
                pl.lit(test_type).alias("test_type")
            ])

        else:
            raise ValueError("Invalid case number")

        dfs.append(df)
    
    merged_df = pl.concat(dfs)

    # Write the merged DataFrame to a new CSV file
    merged_df.write_csv(out_csv)
    print(f"Merged {len(file_infos)} files into {out_csv} (case {case})")

def choose_path_name(snr_id: str, timeline_stage: str, test_type: str, case: int) -> str:
    if case == 1:
        return f"SNR{snr_id}_{timeline_stage}_{test_type}.csv"
    
    elif case == 2:        
        return f"SNR{snr_id}_all_{test_type}.csv"
    
    elif case == 3:       
        return f"SNR{snr_id}_{timeline_stage}_all.csv"
    
    elif case == 4:
        return f"SNR{snr_id}.csv"
    
    else:
        raise ValueError("Invalid case number")


# This function retrieves the file paths of the IMU CSV files based on the provided parameters and then merges them into a single CSV file.
def imu_csv_export(snr_id: str, timeline_stage: str = "", test_type: str = "") -> str:
    cfg = load_config()
    case = 0
    file_infos = []

    # Get the file paths of the IMU CSV files based on the provided parameters
    file_infos, case = get_imu_csv_path(snr_id=snr_id, timeline_stage=timeline_stage, test_type=test_type)

    out_csv = choose_path_name(snr_id, timeline_stage, test_type, case)
    out_path = str(Path(cfg.export_dir) / out_csv)

    # Merge all files into one parquet file from the list of file paths and write it to the output path
    merge_csv_files(file_infos, case, out_path)
    return out_csv

if __name__ == "__main__":    # Example usage
    # Get one file for id 193, for all timeline_stage and test_type "gait"
    out_csv = imu_csv_export(snr_id="193", timeline_stage="admission", test_type="gait")

    