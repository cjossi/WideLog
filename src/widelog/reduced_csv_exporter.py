# Standard library imports
from __future__ import annotations
from pathlib import Path

# Third-party imports
import polars as pl

# Local imports
from widelog.config import load_config
from widelog.query_service import get_connection


def reduced_csv_exporter(
        selected_columns: list[str] | None = None,
        selected_filters: list[tuple[str, str]] | None = None
) -> Path:
    """
    Export a reduced CSV file from the objects view in the database.

    Parameters
    ----------
    select_columns:
        List of column names to export

    select_filters:
        List of filters as tuples:
        (column_name, value)

    Returns
    -------
    Path
        Path to the exported CSV file
    """

    selected_columns = selected_columns or []
    selected_filters = selected_filters or []

    if not selected_columns:
        columns_sql = "*"
    else:
        columns_sql = ", ".join(selected_columns)

    cfg = load_config()

    output_path = cfg.export_dir / "reduced_main.csv"

    query = f"""
        SELECT {columns_sql}
        FROM objects
    """

    params: list[str] = []

    if selected_filters:
        filter_clauses = []

        for column_name, value in selected_filters:
            filter_clauses.append(f"{column_name} = ?")
            params.append(value)
        
        where_clause = " AND ".join(filter_clauses)

        query += f" WHERE {where_clause}"

    with get_connection() as con:
        df  = pl.from_pandas(
            con.execute(query, params).df()
        )

    output_path.parent.mkdir(parents=True, exist_ok=True)

    df.write_csv(output_path)

    return output_path