# Standard library imports
from dataclasses import dataclass
from pathlib import Path

# Third-party imports
import yaml


@dataclass(frozen=True)
class Config:
    """
    Application configuration loaded from config.yaml.
    """

    csv_main: Path
    csv_meta: Path
    parquet_path: Path
    tests_root: Path
    out_dir: Path
    duckdb_path: Path
    export_dir: Path


def load_config(path: str | Path = "config.yaml") -> Config:
    """
    load the application configuration from a YAM file.

    Parameters
    ----------
    path : str | Path, optional
        Path to the configuration YAML file.

    Returns
    -------
    Config
        Parsed application configuration.
    """

    config_path = Path(path)

    with config_path.open("r", encoding="utf-8") as file:
        config_dict = yaml.safe_load(file)

    # Convert string paths to Path objects
    path_fields = {
        key: Path(value)
        for key, value in config_dict.items()
    }

    return Config(**path_fields)