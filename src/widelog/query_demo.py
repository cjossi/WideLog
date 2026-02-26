from __future__ import annotations
import duckdb

#local imports
from .config import load_config

def main() -> None:
    # Load configuration from the config.yaml file
    cfg = load_config()

    con = duckdb.connect(database=str(cfg.duckdb_path))

    print("Number of objects:")
    print(con.execute("SELECT COUNT(*) FROM objects").fetchone())

    print("")