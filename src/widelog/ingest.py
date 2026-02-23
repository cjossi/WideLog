# Import
from pathlib import Path
import polars as pl

# Local imports
from .config import load_config

NULL_VALUES = ["", "NA", "null", "NULL", "not possible"]

def csv_to_parquet(csv_path: str, out_path: Path):
    # Define the output directory and create it if it doesn't exist
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Read the CSV file with specific options to handle data types and null values
    lf = pl.scan_csv(
        csv_path,                       # Use lazy loading for efficient processing
        try_parse_dates=True,           # Try to parse dates automatically
        infer_schema_length=50000,      # Look at the first 50k rows to determine the schema
        null_values=NULL_VALUES,        # Treat these as null values
    )
    
    # Write the DataFrame to a Parquet file
    lf.sink_parquet(out_path)

def main():
    # Load configuration from the config.yaml file
    cfg = load_config()

    # Define the output directory based on the configuration
    out_dir = Path(cfg.out_dir)

    # Convert main.csv to main.parquet
    print("main.csv -> main.parquet")
    csv_to_parquet(cfg.csv_main, out_dir / "main.parquet")

    # Convert meta.csv to meta.parquet
    print("meta.csv -> meta.parquet")
    csv_to_parquet(cfg.csv_meta, out_dir / "meta.parquet")

    print("Done")


if __name__ == "__main__":
    main()