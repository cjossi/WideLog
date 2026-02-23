# Import
from __future__ import annotations  # Annotations to string, to not evaluate them up front
from pathlib import Path
import polars as pl

from .ingest import csv_to_parquet

# Local imports
from .config import load_config

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

        # Get the relative path and parts
        rel = p.relative_to(root)
        parts = rel.parts

        # We expect at least "object_id/timeline_stage/test_type/test_name.csv"
        if len(parts) < 4:  
            print(f"Skipping {p} because it doesn't have enough parts")
            continue

        rows.append(
            {
                "object_id": str(parts[0]),
                "timeline_stage": str(parts[1]),
                "test_type": str(parts[2]),
                "test_name": p.stem,
                "file_path": str(p),
            }
        )
        
        # Convert the list of rows to a Polars DataFrame and write it to a Parquet file
        df = pl.DataFrame(rows)
        out_path = out_dir / "index.parquet"
        df.write_parquet(out_path)

        print(f"Wrote: {out_path}")
        print(f"Rows: {df.height}")

        # Print a sample of the rows if there are any
        if df.height > 0:
            print("Sample rows:")
            print(df.head(5))

if __name__ == "__main__":
    main()