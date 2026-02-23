from dataclasses import dataclass
import yaml

@dataclass
class Config:
    csv_main: str
    csv_meta: str
    parquet_path: str
    tests_root: str
    out_dir: str
    duckdb_path: str

def load_config(path: str = "config.yaml") -> Config:
    with open(path, "r", encoding="utf-8") as f:
        config_dict = yaml.safe_load(f)
    return Config(**config_dict)