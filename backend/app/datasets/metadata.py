from __future__ import annotations

import json

from ..config import settings


def get_dataset_metadata(dataset_id: str) -> dict | None:
    """Get metadata for a dataset if it exists."""
    updir = settings.DATA_DIR / dataset_id
    meta_file = updir / ".metadata.json"
    if meta_file.exists():
        try:
            return json.loads(meta_file.read_text())
        except (json.JSONDecodeError, OSError):
            pass
    return None


def save_dataset_metadata(dataset_id: str, metadata: dict) -> None:
    """Save metadata for a dataset."""
    updir = settings.DATA_DIR / dataset_id
    meta_file = updir / ".metadata.json"
    meta_file.write_text(json.dumps(metadata))


def get_all_display_names() -> set[str]:
    """Get all existing display names across all datasets."""
    names: set[str] = set()
    if not settings.DATA_DIR.exists():
        return names
    for subdir in settings.DATA_DIR.iterdir():
        if subdir.is_dir():
            # First try metadata file
            meta_file = subdir / ".metadata.json"
            if meta_file.exists():
                try:
                    meta = json.loads(meta_file.read_text())
                    if "display_name" in meta:
                        names.add(meta["display_name"])
                        continue
                except (json.JSONDecodeError, OSError):
                    pass
            # Fallback: look for original uploaded file (csv or parquet, not df.parquet)
            for f in subdir.iterdir():
                if (
                    f.is_file()
                    and f.name != "df.parquet"
                    and not f.name.startswith(".")
                ):
                    if f.suffix.lower() in (".csv", ".parquet"):
                        names.add(f.name)
                        break
    return names


def get_display_name_for_dataset(dataset_id: str) -> str:
    """Get display name for a dataset, with fallback to original filename or dataset_id."""
    # First try metadata file
    meta = get_dataset_metadata(dataset_id)
    if meta and "display_name" in meta:
        return meta["display_name"]

    # Fallback: look for original uploaded file (csv or parquet, not df.parquet)
    updir = settings.DATA_DIR / dataset_id
    if updir.exists():
        for f in updir.iterdir():
            if f.is_file() and f.name != "df.parquet" and not f.name.startswith("."):
                if f.suffix.lower() in (".csv", ".parquet"):
                    return f.name

    # Last resort: use dataset_id
    return dataset_id
