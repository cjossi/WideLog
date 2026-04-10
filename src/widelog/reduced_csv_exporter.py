# Import
import polars as pl
from pathlib import Path

# Local imports
from widelog.config import load_config
from widelog.query_service import get_connection

# This function export a reduced size CSV file with only the selected columns and filters
def reduced_csv_exporter(list_of_columns = [], list_of_filters = []):
    ## ---CHECKS--- ##
    # Check if the list of columns is None
    if list_of_columns is None:
        list_of_columns = []

    # Check if the list of filters is None
    if list_of_filters is None:
        list_of_filters = []

    ## ---CONFIG--- ##
    cfg = load_config()
    out_csv_path = Path(cfg.export_dir) / "reduced_main.csv"

    # Read the view "objects" from the DuckDB database into a Polars DataFrame
    con = get_connection()

    ## ---Select the columns to export--- ##
    # Use SQL to query
    cols = ", ".join(list_of_columns)

    query = f"""
        SELECT {cols}
        FROM objects
    """

    ## ---Select the filters--- ##
    params = []

    if list_of_filters:
        filters = []
        for col, val in list_of_filters:
            filters.append(f"{col} = ?")
            params.append(val)
        
        where_clause = " AND ".join(filters)
        query += f" WHERE {where_clause}"

    ## ---Execute the query and export to CSV--- ##
    # Get the "objects" view as pandas DataFrame
    df = con.execute(query, params).df()

    # Convert the pandas DataFrame to csv
    df.to_csv(out_csv_path, index=False)

    # Write the reduced DataFrame to a new CSV file
    print(f"Exported reduced CSV with columns {list_of_columns} to {out_csv_path}")

    return out_csv_path