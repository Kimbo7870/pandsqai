from __future__ import annotations

import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from fastapi import APIRouter, File, UploadFile
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

from .config import settings
from .errors import api_error
from .datasets.dedup import get_content_hash_bytes, write_content_hash_file
from .datasets.store import get_parquet_row_col_counts, write_parquet_df

router = APIRouter(prefix="/multifile")


def _mf_root() -> Path:
    root = settings.MULTIFILE_DATA_DIR
    root.mkdir(parents=True, exist_ok=True)
    return root


def _current_path() -> Path:
    return _mf_root() / "current.json"


def _dataset_dir(dataset_id: str) -> Path:
    return _mf_root() / dataset_id


def _metadata_path(dataset_id: str) -> Path:
    return _dataset_dir(dataset_id) / ".metadata.json"


def _read_json_file(path: Path) -> object | None:
    try:
        if not path.exists():
            return None
        return json.loads(path.read_text())
    except Exception:
        return None


def _write_json_atomic(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload))
    tmp.replace(path)


def _prune_current_ids(ids: list[str]) -> list[str]:
    """Remove ids that no longer exist on disk, while preserving order and uniqueness."""
    seen: set[str] = set()
    kept: list[str] = []
    root = _mf_root()
    for ds_id in ids:
        if not isinstance(ds_id, str):
            continue
        if ds_id in seen:
            continue
        seen.add(ds_id)

        ds_dir = root / ds_id
        parquet_path = ds_dir / "df.parquet"
        if ds_dir.is_dir() and parquet_path.exists():
            kept.append(ds_id)
    return kept


def _read_current_ids() -> list[str]:
    path = _current_path()
    data = _read_json_file(path)
    ids: list[str]
    if isinstance(data, list):
        ids = [x for x in data if isinstance(x, str)]
    else:
        ids = []

    pruned = _prune_current_ids(ids)
    if pruned != ids:
        _write_json_atomic(path, pruned)
    return pruned


def _write_current_ids(ids: list[str]) -> None:
    _write_json_atomic(_current_path(), ids)


def _read_metadata(dataset_id: str) -> dict | None:
    meta = _read_json_file(_metadata_path(dataset_id))
    return meta if isinstance(meta, dict) else None


def _safe_display_name_uploaded_at(dataset_id: str) -> tuple[str, str | None]:
    meta = _read_metadata(dataset_id) or {}
    display_name = meta.get("display_name")
    uploaded_at = meta.get("uploaded_at")

    if not isinstance(display_name, str) or not display_name:
        display_name = dataset_id
    if not isinstance(uploaded_at, str) or not uploaded_at:
        uploaded_at = None

    return display_name, uploaded_at


@router.post("/upload")
async def multifile_upload(file: UploadFile = File(...)):
    if file.size and file.size > 50 * 1024 * 1024:  # 50MB soft cap
        raise api_error(400, "FILE_TOO_LARGE", "Max file size is 50MB")

    name = (file.filename or "upload").strip() or "upload"
    safe_name = Path(name).name
    if not safe_name.lower().endswith((".csv", ".parquet")):
        raise api_error(400, "BAD_EXTENSION", "Only .csv or .parquet allowed")

    contents = await file.read()
    if len(contents) == 0:
        raise api_error(400, "EMPTY_FILE", "File is empty")
    if len(contents) > 10 * 1024 * 1024:
        raise api_error(413, "FILE_TOO_LARGE", "File too large (>10MB)")

    content_hash = get_content_hash_bytes(contents)
    dataset_id = content_hash

    current_ids = _read_current_ids()
    if dataset_id in current_ids:
        ds_dir = _dataset_dir(dataset_id)
        parquet_path = ds_dir / "df.parquet"
        n_rows, n_cols = get_parquet_row_col_counts(parquet_path)
        display_name, uploaded_at = _safe_display_name_uploaded_at(dataset_id)

        return JSONResponse(
            jsonable_encoder(
                {
                    "dataset_id": dataset_id,
                    "display_name": display_name,
                    "n_rows": n_rows,
                    "n_cols": n_cols,
                    "uploaded_at": uploaded_at,
                    "already_present": True,
                    "slot_count": len(current_ids),
                }
            )
        )

    if len(current_ids) >= 3:
        raise api_error(409, "SLOTS_FULL", "Multifile supports up to 3 datasets")

    ds_dir = _dataset_dir(dataset_id)
    hash_file = ds_dir / ".content_hash"
    parquet_path = ds_dir / "df.parquet"

    # Collision/corruption guard (extremely unlikely)
    if ds_dir.exists() and hash_file.exists():
        existing_hash = hash_file.read_text().strip()
        if existing_hash and existing_hash != content_hash:
            shutil.rmtree(ds_dir, ignore_errors=True)

    # If already on disk with matching hash, reuse it.
    if ds_dir.is_dir() and hash_file.exists() and parquet_path.exists():
        existing_hash = hash_file.read_text().strip()
        if existing_hash == content_hash:
            current_ids.append(dataset_id)
            _write_current_ids(current_ids)

            n_rows, n_cols = get_parquet_row_col_counts(parquet_path)
            display_name, uploaded_at = _safe_display_name_uploaded_at(dataset_id)
            return JSONResponse(
                jsonable_encoder(
                    {
                        "dataset_id": dataset_id,
                        "display_name": display_name,
                        "n_rows": n_rows,
                        "n_cols": n_cols,
                        "uploaded_at": uploaded_at,
                        "already_present": False,
                        "slot_count": len(current_ids),
                    }
                )
            )

    # Create new dataset on disk.
    ds_dir.mkdir(parents=True, exist_ok=True)

    raw_path = ds_dir / safe_name
    try:
        raw_path.write_bytes(contents)
    except Exception as e:
        shutil.rmtree(ds_dir, ignore_errors=True)
        raise api_error(500, "STORE_FAILED", f"Failed to write upload: {e}")

    try:
        write_content_hash_file(ds_dir, content_hash)
    except Exception as e:
        shutil.rmtree(ds_dir, ignore_errors=True)
        raise api_error(500, "STORE_FAILED", f"Failed to write content hash: {e}")

    try:
        if safe_name.lower().endswith(".csv"):
            df = pd.read_csv(raw_path, engine="pyarrow", dtype_backend="pyarrow")
        else:
            df = pd.read_parquet(raw_path)
    except Exception as e:
        shutil.rmtree(ds_dir, ignore_errors=True)
        raise api_error(400, "PARSING_FAILED", f"Could not parse file: {e}")

    try:
        write_parquet_df(df, parquet_path)
    except Exception as e:
        shutil.rmtree(ds_dir, ignore_errors=True)
        raise api_error(500, "PARQUET_WRITE_FAILED", f"Failed to write parquet: {e}")

    uploaded_at = datetime.now(timezone.utc).isoformat()
    meta = {"display_name": safe_name, "uploaded_at": uploaded_at}
    _write_json_atomic(_metadata_path(dataset_id), meta)

    current_ids.append(dataset_id)
    _write_current_ids(current_ids)

    return JSONResponse(
        jsonable_encoder(
            {
                "dataset_id": dataset_id,
                "display_name": safe_name,
                "n_rows": int(len(df)),
                "n_cols": int(df.shape[1]),
                "uploaded_at": uploaded_at,
                "already_present": False,
                "slot_count": len(current_ids),
            }
        )
    )


@router.get("/current")
async def multifile_current():
    ids = _read_current_ids()
    datasets: list[dict] = []

    for dataset_id in ids:
        ds_dir = _dataset_dir(dataset_id)
        parquet_path = ds_dir / "df.parquet"
        if not parquet_path.exists():
            continue

        n_rows, n_cols = get_parquet_row_col_counts(parquet_path)
        display_name, uploaded_at = _safe_display_name_uploaded_at(dataset_id)
        datasets.append(
            {
                "dataset_id": dataset_id,
                "display_name": display_name,
                "n_rows": n_rows,
                "n_cols": n_cols,
                "uploaded_at": uploaded_at,
            }
        )

    return JSONResponse(jsonable_encoder({"datasets": datasets}))


@router.delete("/current/{dataset_id}")
async def multifile_delete_one(dataset_id: str):
    ids = _read_current_ids()
    if dataset_id not in ids:
        raise api_error(404, "DATASET_NOT_FOUND", f"Dataset {dataset_id} not found")

    ids = [x for x in ids if x != dataset_id]
    _write_current_ids(ids)

    shutil.rmtree(_dataset_dir(dataset_id), ignore_errors=True)

    return JSONResponse(jsonable_encoder({"ok": True, "slot_count": len(ids)}))


@router.delete("/current")
async def multifile_delete_all():
    ids = _read_current_ids()
    for dataset_id in ids:
        shutil.rmtree(_dataset_dir(dataset_id), ignore_errors=True)

    _write_current_ids([])
    return JSONResponse(jsonable_encoder({"ok": True, "slot_count": 0}))
