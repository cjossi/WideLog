# Standard library imports
from __future__ import annotations
from pathlib import Path
import re

# Third-party imports
import polars as pl

# Local imports
from widelog.config import load_config


# Constants
FILENAME_RE = re.compile(
    r"^SNR(?P<snr_id>\d+)_(?P<timeline_stage>[^_]+)_(?P<test_type>[^.]+)\.csv$"
)


def ingest_tests_index() -> None:
    """
    Scan IMU test CSV files and build an index parquet file.
    """

    cfg = load_config()

    root = cfg.tests_root
    out_dir = cfg.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    rows: list[dict[str, str]] = []

    for path in root.rglob("*.csv"):

        match = FILENAME_RE.match(path.name)

        if not match:
            continue

        rows.append(
            {
                "snr_id": match.group("snr_id"),
                "timeline_stage": match.group("timeline_stage"),
                "test_type": match.group("test_type"),
                "file_path": str(path),
                "folder": path.parent.name,
                "file_name": path.name,
            }
        )
        
    df = pl.DataFrame(rows)

    out_path = out_dir / "tests_index.parquet"
    
    df.write_parquet(out_path)

if __name__ == "__main__":
    ingest_tests_index()