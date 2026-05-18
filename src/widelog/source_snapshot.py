# Standard library imports
from __future__ import annotations
from pathlib import Path
import json

# Local imports
from widelog.config import load_config


def file_signature(path: Path) -> dict[str, str | int]:
    """
    Return a lightweight signature describing a file.

    The signature includes:
    - absolute file path
    - file size
    - last modification timestamp
    """

    stat = path.stat()

    return{
        "path": str(path.resolve()),
        "size": stat.st_size,
        "mtime": int(stat.st_mtime)
    }

def build_source_snapshot() -> dict:
    """
    Build a snapshot of all source files used by the pipeline.

    The snapshot is used to detect changes in:
    - main CSV file
    - meta CSV file
    - IMU CSV files in the tests directory
    """

    cfg = load_config()

    snapshot: dict[str, dict] = {}

    main_csv = cfg.csv_main
    meta_csv = cfg.csv_meta
    tests_root = cfg.tests_root

    required_paths = [
        main_csv,
        meta_csv,
        tests_root
    ]

    for path in required_paths:
        if not path.exists():
            raise FileNotFoundError(f"File or directory not found: {path}")
    
    snapshot: dict[str, dict] = {}

    snapshot["main_csv"] = file_signature(main_csv)

    snapshot["meta_csv"] = file_signature(meta_csv)

    imu_files = sorted(tests_root.glob("**/*.csv"))

    snapshot["imu_files"] = {
        str(p.resolve()): file_signature(p) for p in imu_files
    }

    return snapshot

def get_snapshot_path() -> Path:
    """
    Return the path of the snapshot JSON file.
    """

    cfg = load_config()

    return cfg.out_dir.parent / "source_snapshot.json"

def save_snapshot(snapshot: dict) -> Path:
    """
    Save a snapshot dictionary as a JSON file.
    """

    output_path = get_snapshot_path()
    
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with output_path.open("w", encoding="utf-8") as file:
        json.dump(
            snapshot,
            file,
            indent=4
        )

    return output_path

def load_snapshot() -> dict | None:
    """
    Load the saved source snapshot.

    Returns
    -------
    dict | None
        The loaded snapshot if it exists,
        otherwise None.
    """

    snapshot_path = get_snapshot_path()

    if not snapshot_path.exists():
        return None

    with snapshot_path.open("r", encoding="utf-8") as file:
        snapshot = json.load(file)

    return snapshot

def sources_changed() -> tuple[bool, dict | None, dict]:
    """
    Compare the current sources with the saved snapshot.

    Returns
    -------
    tuple[bool, dict | None, dict]
        A tuple containing:
        - A boolean indicating if sources have changed.
        - The old snapshot (or None if it doesn't exist).
        - The new snapshot.
    """

    old_snapshot = load_snapshot()

    new_snapshot = build_source_snapshot()

    if old_snapshot is None:
        return True, None, new_snapshot
    
    has_changed = old_snapshot != new_snapshot

    return has_changed, old_snapshot, new_snapshot