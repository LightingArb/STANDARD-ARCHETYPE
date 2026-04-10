from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path


def load_json_file(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def file_signature(path: Path) -> dict:
    if not path.exists():
        return {}

    digest = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(chunk)

    stat = path.stat()
    return {
        "path": str(path),
        "size": stat.st_size,
        "mtime": stat.st_mtime,
        "sha256": digest.hexdigest(),
    }


def csv_semantic_signature(path: Path, ignore_fields: set[str] | None = None) -> dict:
    if not path.exists():
        return {}

    ignore = ignore_fields or set()
    digest = hashlib.sha256()
    row_count = 0

    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = [field for field in (reader.fieldnames or []) if field not in ignore]
        digest.update(",".join(fieldnames).encode("utf-8"))
        digest.update(b"\n")

        for row in reader:
            row_count += 1
            values = [str(row.get(field, "")).strip() for field in fieldnames]
            digest.update("\x1f".join(values).encode("utf-8"))
            digest.update(b"\n")

    stat = path.stat()
    return {
        "path": str(path),
        "size": stat.st_size,
        "mtime": stat.st_mtime,
        "row_count": row_count,
        "sha256": digest.hexdigest(),
    }


def signatures_match(current: dict, recorded: dict) -> bool:
    return bool(
        current
        and recorded
        and current.get("sha256")
        and current.get("sha256") == recorded.get("sha256")
        and current.get("size") == recorded.get("size")
    )


def all_exist(paths: list[Path]) -> bool:
    return all(path.exists() for path in paths)


def max_mtime(paths: list[Path]) -> float:
    return max((path.stat().st_mtime for path in paths if path.exists()), default=0.0)


def min_mtime(paths: list[Path]) -> float:
    values = [path.stat().st_mtime for path in paths if path.exists()]
    return min(values) if values else 0.0
