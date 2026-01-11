from fastapi import APIRouter, UploadFile, File
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
import pandas as pd

from .config import settings
from .errors import api_error

from .datasets.dedup import (
    get_content_hash_bytes,
    find_existing_dataset_id,
    write_content_hash_file,
)
from .datasets.metadata import (
    get_dataset_metadata,
    save_dataset_metadata,
    get_all_display_names,
    get_display_name_for_dataset,
)
from .datasets.naming import generate_unique_display_name
from .datasets.sampling import make_sample_records
from .datasets.store import (
    read_parquet_df,
    write_parquet_df,
    get_parquet_row_col_counts,
)

router = APIRouter()  # router for endpoints


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
    if len(contents) == 0:
        raise api_error(400, "EMPTY_FILE", "File is empty")
    if len(contents) > 10 * 1024 * 1024:
        raise api_error(413, "FILE_TOO_LARGE", "File too large (>10MB)")

    # Compute content hash for deduplication
    content_hash = get_content_hash_bytes(contents)

    # Check for existing dataset with same content
    existing_id = find_existing_dataset_id(content_hash)
    if existing_id:
        # Dataset already exists, return existing dataset info
        dataset_id = existing_id
        updir = settings.DATA_DIR / dataset_id
        parquet_path = updir / "df.parquet"
        df = read_parquet_df(parquet_path)

        # Get existing display name from metadata
        meta = get_dataset_metadata(dataset_id)
        display_name = meta.get("display_name", name) if meta else name
    else:
        # New dataset - generate unique id using content hash as dataset_id
        dataset_id = content_hash
        updir = settings.DATA_DIR / dataset_id
        updir.mkdir(parents=True, exist_ok=True)

        # Generate unique display name (macOS-style duplicate handling)
        existing_names = get_all_display_names()
        display_name = generate_unique_display_name(name, existing_names)

        # Save raw/original copy
        raw_path = updir / name
        raw_path.write_bytes(contents)

        # Save content hash for future deduplication checks
        write_content_hash_file(updir, content_hash)

        # Save metadata with display name
        save_dataset_metadata(dataset_id, {"display_name": display_name})

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
            write_parquet_df(df, parquet_path)
        except Exception as e:
            raise api_error(
                500, "PARQUET_WRITE_FAILED", f"Failed to write parquet: {e}"
            )

    sample = make_sample_records(df, n=50)

    return JSONResponse(
        jsonable_encoder(
            {
                "dataset_id": dataset_id,  # dataset reference id
                "display_name": display_name,  # user-friendly file name
                "n_rows": int(len(df)),
                "n_cols": int(df.shape[1]),
                "columns": list(df.columns.map(str)),  # column names as strings
                "sample": sample,  # sample of table
            }
        )
    )


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
            display_name = get_display_name_for_dataset(dataset_id)

            # Get row/col counts from parquet metadata (fast, no full read)
            n_rows, n_cols = get_parquet_row_col_counts(parquet_path)

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

    df = read_parquet_df(parquet_path)
    display_name = get_display_name_for_dataset(dataset_id)

    sample = make_sample_records(df, n=50)

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
