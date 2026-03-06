# Import
from __future__ import annotations
from pathlib import Path
import json

# Local imports
from widelog.config import load_config

def file_signature(path: Path) -> dict:
    stat = path.stat()
    return{
        "path": str(path),
        "size": stat.st_size,
        "mtime": int(stat.st_mtime)
    }

def build_source_snapshot() -> dict:
    cfg = load_config()

    snapshot: dict[str, dict] = {}

    main_csv = Path(cfg.csv_main)
    meta_csv = Path(cfg.csv_meta)
    tests_root = Path(cfg.tests_root)

    # Check if files exists
    if not main_csv.exists():
        raise FileNotFoundError(f"File not found: {main_csv}")
    if not meta_csv.exists():
        raise FileNotFoundError(f"File not found: {meta_csv}")
    if not tests_root.exists():
        raise FileNotFoundError(f"Directory not found: {tests_root}")
    
    snapshot["main_csv"] = file_signature(main_csv)
    snapshot["meta_csv"] = file_signature(meta_csv)

    imu_files = sorted(tests_root.glob("**/*.csv"))
    snapshot["imu_files"] = {str(p.resolve()): file_signature(p) for p in imu_files}

    return snapshot

def snapshot_path() -> Path:
    cfg = load_config()

    return Path(cfg.out_dir).parent / "source_snapshot.json"

def save_snapshot(snapshot: dict) -> Path:
    out_path = snapshot_path()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(snapshot, f, indent=4)

    return out_path

def load_snapshot() -> dict:
    path = snapshot_path()

    if not path.exists():
        raise FileNotFoundError(f"Snapshot file not found: {path}")
    
    with open(path, "r", encoding="utf-8") as f:
        snapshot = json.load(f)

    return snapshot

def sources_changed() -> tuple[bool, dict | None, dict]:
    old_snapshot = load_snapshot()
    new_snapshot = build_source_snapshot()

    if old_snapshot is None:
        return True, None, new_snapshot
    

    return old_snapshot != new_snapshot, old_snapshot, new_snapshot