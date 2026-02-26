# Import
from __future__ import annotations  # Annotations to string, to not evaluate them up front
from pathlib import Path
import polars as pl
import re

from .ingest import csv_to_parquet

# Local imports
from .config import load_config

# Exemple of filename: SNR123_TimelineStage_TestType.csv
FILENAME_RE = FILENAME_RE = re.compile(
    r"^SNR(?P<snr_id>\d+)_(?P<timeline_stage>[^_]+)_(?P<test_type>[^.]+)\.csv$"
)

def main() -> None:
    # Load configuration from the config.yaml file
    cfg = load_config()

    # Define the output directory based on the configuration
    root = Path(cfg.tests_root)
    out_dir = Path(cfg.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    rows = []
    # Walk through all CSV files in the tests_root directory and its subdirectories
    for p in root.rglob("*.csv"):
        print(f"Processing {p}")

        # Extract information from the filename using the regular expression
        m=FILENAME_RE.match(p.name)

        if not m:
            print(f"Skipping {p} because it doesn't match the expected filename pattern")
            continue

        # Append a row to the list of rows with the extracted information and file details
        rows.append(
            {
                "object_id": m.group("snr_id"),
                "timeline_stage": m.group("timeline_stage"),
                "test_type": m.group("test_type"),
                "file_path": str(p),
                "folder": p.parent.name,
                "file_name": p.name,
            }
        )
        
        # Convert the list of rows to a Polars DataFrame and write it to a Parquet file
        df = pl.DataFrame(rows)
        out_path = out_dir / "tests_index.parquet"
        df.write_parquet(out_path)

        print(f"Wrote: {out_path}")
        print(f"Rows: {df.height}")

        # Print a sample of the rows if there are any
        if df.height > 0:
            print("Sample rows:")
            print(df.head(5))

if __name__ == "__main__":
    main()