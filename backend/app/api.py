import json
import re
from fastapi import APIRouter, UploadFile, File
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder  ###
import xxhash
import pandas as pd

from .config import settings
from .errors import api_error

router = APIRouter()  # router for endpoints


def _get_content_hash(contents: bytes) -> str:
    """Compute xxhash of file contents for deduplication."""
    return xxhash.xxh64(contents).hexdigest()


def _find_existing_dataset(content_hash: str) -> str | None:
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


def _get_all_display_names() -> set[str]:
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
                if f.is_file() and f.name != "df.parquet" and not f.name.startswith("."):
                    if f.suffix.lower() in (".csv", ".parquet"):
                        names.add(f.name)
                        break
    return names


def _generate_unique_display_name(base_name: str, existing_names: set[str]) -> str:
    """Generate a unique display name using macOS-style duplicate handling.
    
    If 'file.csv' exists, returns 'file (1).csv', then 'file (2).csv', etc.
    """
    if base_name not in existing_names:
        return base_name

    # Split name and extension
    # Handle cases like "file.csv", "file.tar.gz", "file"
    match = re.match(r"^(.+?)(\.[^.]+)?$", base_name)
    if match:
        name_part = match.group(1)
        ext_part = match.group(2) or ""
    else:
        name_part = base_name
        ext_part = ""

    # Find the next available number
    counter = 1
    while True:
        candidate = f"{name_part} ({counter}){ext_part}"
        if candidate not in existing_names:
            return candidate
        counter += 1


def _get_dataset_metadata(dataset_id: str) -> dict | None:
    """Get metadata for a dataset if it exists."""
    updir = settings.DATA_DIR / dataset_id
    meta_file = updir / ".metadata.json"
    if meta_file.exists():
        try:
            return json.loads(meta_file.read_text())
        except (json.JSONDecodeError, OSError):
            pass
    return None


def _save_dataset_metadata(dataset_id: str, metadata: dict) -> None:
    """Save metadata for a dataset."""
    updir = settings.DATA_DIR / dataset_id
    meta_file = updir / ".metadata.json"
    meta_file.write_text(json.dumps(metadata))


# upload data endpoint
@router.post("/upload")
async def upload(file: UploadFile = File(...)):
    if file.size and file.size > 50 * 1024 * 1024:  # 50MB soft cap
        raise api_error(400, "FILE_TOO_LARGE", "Max file size is 50MB")
    name = file.filename or "upload"
    if not name.lower().endswith(
        (".csv", ".parquet")
    ):  # supporting csv or parquet for now
        raise api_error(400, "BAD_EXTENSION", "Only .csv or .parquet allowed")

    # Size guard by reading into bytes and keeping size less than 10 MB
    contents = await file.read()
    if len(contents) == 0:  ###
        raise api_error(400, "EMPTY_FILE", "File is empty")  ###
    if len(contents) > 10 * 1024 * 1024:
        raise api_error(413, "FILE_TOO_LARGE", "File too large (>10MB)")

    # Compute content hash for deduplication
    content_hash = _get_content_hash(contents)

    # Check for existing dataset with same content
    existing_id = _find_existing_dataset(content_hash)
    if existing_id:
        # Dataset already exists, return existing dataset info
        dataset_id = existing_id
        updir = settings.DATA_DIR / dataset_id
        parquet_path = updir / "df.parquet"
        df = pd.read_parquet(parquet_path)
        # Get existing display name from metadata
        meta = _get_dataset_metadata(dataset_id)
        display_name = meta.get("display_name", name) if meta else name
    else:
        # New dataset - generate unique id using content hash as dataset_id
        dataset_id = content_hash
        updir = settings.DATA_DIR / dataset_id
        updir.mkdir(parents=True, exist_ok=True)

        # Generate unique display name (macOS-style duplicate handling)
        existing_names = _get_all_display_names()
        display_name = _generate_unique_display_name(name, existing_names)

        # Save raw/original copy
        raw_path = updir / name
        raw_path.write_bytes(contents)

        # Save content hash for future deduplication checks
        hash_file = updir / ".content_hash"
        hash_file.write_text(content_hash)

        # Save metadata with display name
        _save_dataset_metadata(dataset_id, {"display_name": display_name})

        # Load to pandas
        try:
            if name.lower().endswith(".csv"):
                # pyarrow (fast and hanldes edge cases), dtype_backend=pyarrow keeps better dtypes
                df = pd.read_csv(raw_path, engine="pyarrow", dtype_backend="pyarrow")
            else:
                df = pd.read_parquet(raw_path)
        except Exception as e:
            raise api_error(400, "PARSING_FAILED", f"Could not parse file: {e}")

        # save parquet file without index so easy to read back
        parquet_path = updir / "df.parquet"
        try:
            df.to_parquet(parquet_path, index=False)
        except Exception as e:  ###
            raise api_error(
                500, "PARQUET_WRITE_FAILED", f"Failed to write parquet: {e}"
            )  ###

    sample_df = df.head(50).copy()
    dt_cols = sample_df.select_dtypes(include=["datetime", "datetimetz"]).columns
    for c in dt_cols:
        sample_df[c] = pd.to_datetime(sample_df[c]).dt.strftime("%Y-%m-%dT%H:%M:%S")
    sample_df = sample_df.where(sample_df.notna(), None)

    # sample that i'll send to frontend
    sample = sample_df.to_dict(orient="records")
    return JSONResponse(
        jsonable_encoder(
            {  ###
                "dataset_id": dataset_id,  # dataset reference id
                "display_name": display_name,  # user-friendly file name
                "n_rows": int(len(df)),
                "n_cols": int(df.shape[1]),
                "columns": list(df.columns.map(str)),  # column names as strings
                "sample": sample,  # sample of table
            }
        )
    )


def _get_display_name_for_dataset(dataset_id: str) -> str:
    """Get display name for a dataset, with fallback to original filename or dataset_id."""
    # First try metadata file
    meta = _get_dataset_metadata(dataset_id)
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


# list all past datasets endpoint
@router.get("/datasets")
async def list_datasets():
    """List all previously uploaded datasets with their display names."""
    datasets = []
    if not settings.DATA_DIR.exists():
        return JSONResponse(jsonable_encoder({"datasets": datasets}))

    for subdir in settings.DATA_DIR.iterdir():
        if subdir.is_dir():
            parquet_path = subdir / "df.parquet"
            if not parquet_path.exists():
                continue  # Skip incomplete datasets

            dataset_id = subdir.name
            display_name = _get_display_name_for_dataset(dataset_id)

            # Get row/col counts from parquet metadata (fast, no full read)
            try:
                import pyarrow.parquet as pq

                pq_file = pq.ParquetFile(parquet_path)
                n_rows = pq_file.metadata.num_rows
                n_cols = pq_file.metadata.num_columns
            except Exception:
                # Fallback: read full file
                df = pd.read_parquet(parquet_path)
                n_rows = len(df)
                n_cols = df.shape[1]

            datasets.append(
                {
                    "dataset_id": dataset_id,
                    "display_name": display_name,
                    "n_rows": n_rows,
                    "n_cols": n_cols,
                }
            )

    return JSONResponse(jsonable_encoder({"datasets": datasets}))


# load a specific past dataset endpoint
@router.get("/datasets/{dataset_id}")
async def get_dataset(dataset_id: str):
    """Load a specific dataset by ID and return its info (same format as upload)."""
    updir = settings.DATA_DIR / dataset_id
    parquet_path = updir / "df.parquet"

    if not parquet_path.exists():
        raise api_error(404, "DATASET_NOT_FOUND", f"Dataset {dataset_id} not found")

    df = pd.read_parquet(parquet_path)
    display_name = _get_display_name_for_dataset(dataset_id)

    sample_df = df.head(50).copy()
    dt_cols = sample_df.select_dtypes(include=["datetime", "datetimetz"]).columns
    for c in dt_cols:
        sample_df[c] = pd.to_datetime(sample_df[c]).dt.strftime("%Y-%m-%dT%H:%M:%S")
    sample_df = sample_df.where(sample_df.notna(), None)

    sample = sample_df.to_dict(orient="records")
    return JSONResponse(
        jsonable_encoder(
            {
                "dataset_id": dataset_id,
                "display_name": display_name,
                "n_rows": int(len(df)),
                "n_cols": int(df.shape[1]),
                "columns": list(df.columns.map(str)),
                "sample": sample,
            }
        )
    )