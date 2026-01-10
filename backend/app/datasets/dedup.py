from __future__ import annotations

from pathlib import Path
import xxhash

from ..config import settings


def get_content_hash_bytes(contents: bytes) -> str:
    """Compute xxhash of file contents for deduplication."""
    return xxhash.xxh64(contents).hexdigest()


def find_existing_dataset_id(content_hash: str) -> str | None:
    """Check if a dataset with the same content hash already exists.
    Returns the existing dataset_id if found, None otherwise."""
    if not settings.DATA_DIR.exists():
        return None
    for subdir in settings.DATA_DIR.iterdir():
        if subdir.is_dir():
            hash_file = subdir / ".content_hash"
            if hash_file.exists() and hash_file.read_text().strip() == content_hash:
                return subdir.name
    return None


def write_content_hash_file(dataset_dir: Path, content_hash: str) -> None:
    """Write .content_hash file inside a dataset directory."""
    (dataset_dir / ".content_hash").write_text(content_hash)
