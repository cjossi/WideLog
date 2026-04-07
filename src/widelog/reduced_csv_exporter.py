# Import
import polars as pl
from pathlib import Path

# Local imports
from widelog.config import load_config
from widelog.query_service import get_connection


def reduced_csv_exporter(list_of_columns = []):

    # Check if the list of columns is None
    if list_of_columns is None:
        list_of_columns = []

    cfg = load_config()
    out_csv_path = Path(cfg.export_dir) / "reduced_main.csv"

    # Read the view "objects" from the DuckDB database into a Polars DataFrame
    con = get_connection()

    # Use SQL to query
    cols = ", ".join(list_of_columns)

    query = f"""
        SELECT {cols}
        FROM objects
    """

    # Get the "objects" view as pandas DataFrame
    df = con.execute(query).df()

    # Convert the pandas DataFrame to csv
    df.to_csv(out_csv_path, index=False)

    # Write the reduced DataFrame to a new CSV file
    print(f"Exported reduced CSV with columns {list_of_columns} to {out_csv_path}")

    return out_csv_path