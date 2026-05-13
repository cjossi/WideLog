# Standard library imports
from __future__ import annotations
from pathlib import Path

# Third-party imports
import polars as pl

# Local imports
from widelog.config import load_config
from widelog.constants import NULLS

def csv_to_parquet(
        csv_path: Path,
        out_path: Path
) -> None:
    """
    Convert a CSV file into a parquet file.

    Parameters
    ----------
    csv_path : Path
        Path to the input CSV file.

    out_path : Path
        Path to the output Parquet file.
    """

    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Use lazy loading for memory-efficient processing.
    lazy_frame = pl.scan_csv(
        csv_path,
        try_parse_dates=True,
        infer_schema_length=50_000,
        null_values=NULLS,
    )
    
    lazy_frame.sink_parquet(out_path)

def ingest_csv_to_parquet() -> None:
    """
    Convert the main project CSV files into parquet format.
    """

    cfg = load_config()

    out_dir = cfg.out_dir

    parquet_jobs = [
        (cfg.csv_main, out_dir / "main.parquet"),
        (cfg.csv_meta, out_dir / "meta.parquet"),
    ]

    for csv_path, parquet_path in parquet_jobs:
        csv_to_parquet(csv_path, parquet_path)

if __name__ == "__main__":
    ingest_csv_to_parquet()