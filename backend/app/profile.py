from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from pathlib import Path
import pandas as pd
import numpy as np
from typing import Any
import hashlib

router = APIRouter()
DATA_DIR = Path("data")


def get_content_hash(df: pd.DataFrame) -> str:
    """Generate deterministic hash based on dataframe content"""
    # Use column names + first/last row to create content fingerprint
    content_parts = [
        ",".join(df.columns.astype(str)),
        df.iloc[0].to_json() if len(df) > 0 else "",
        df.iloc[-1].to_json() if len(df) > 0 else "",
        str(len(df)),
        str(df.shape[1]),
    ]
    content_str = "|".join(content_parts)
    return hashlib.sha256(content_str.encode()).hexdigest()[:16]


def get_hash_seed(content_hash: str, column_name: str) -> int:
    """Generate deterministic seed from content hash and column name"""
    hash_str = hashlib.sha256(f"{content_hash}{column_name}".encode()).hexdigest()
    return int(hash_str[:8], 16)


@router.get("/profile")
async def profile(dataset_id: str):
    """Profile a dataset by dataset_id, returns column stats and feature flags"""
    updir = DATA_DIR / dataset_id
    parquet_path = updir / "df.parquet"

    if not updir.exists() or not parquet_path.exists():
        raise HTTPException(status_code=404, detail="Dataset not found")

    try:
        df = pd.read_parquet(parquet_path)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to read dataset: {e}")

    n_rows = int(len(df))
    n_cols = int(df.shape[1])

    # Get content-based hash for deterministic sampling
    content_hash = get_content_hash(df)

    columns = []
    has_numeric = False
    has_datetime = False
    has_categorical = False
    categorical_cols = []

    for col_name in df.columns:
        col = df[col_name]
        col_name_str = str(col_name)
        dtype_str = str(col.dtype)

        null_count = int(col.isna().sum())
        unique_count = int(col.nunique(dropna=True))

        # Get three distinct example values deterministically
        non_null_unique = col.dropna().unique()
        if len(non_null_unique) > 0:
            # Sort to ensure deterministic order across runs
            sorted_unique = sorted(
                non_null_unique, key=lambda x: (type(x).__name__, str(x))
            )
            seed = get_hash_seed(content_hash, col_name_str)
            rng = np.random.RandomState(seed)
            sample_size = min(3, len(sorted_unique))
            indices = rng.choice(len(sorted_unique), size=sample_size, replace=False)
            examples = [sorted_unique[i] for i in sorted(indices)]
        else:
            examples = []

        col_info: dict[str, Any] = {
            "name": col_name_str,
            "dtype": dtype_str,
            "null_count": null_count,
            "unique_count": unique_count,
            "examples": examples,
        }

        # Numeric columns
        if pd.api.types.is_numeric_dtype(col):
            has_numeric = True
            col_info["min"] = (
                round(float(col.min()), 2) if not col.isna().all() else None
            )
            col_info["mean"] = (
                round(float(col.mean()), 2) if not col.isna().all() else None
            )
            col_info["max"] = (
                round(float(col.max()), 2) if not col.isna().all() else None
            )
            col_info["std"] = (
                round(float(col.std()), 2) if not col.isna().all() else None
            )

        # Datetime columns
        elif pd.api.types.is_datetime64_any_dtype(col):
            has_datetime = True
            if not col.isna().all():
                col_info["min_ts"] = pd.Timestamp(col.min()).strftime(
                    "%Y-%m-%dT%H:%M:%S"
                )
                col_info["max_ts"] = pd.Timestamp(col.max()).strftime(
                    "%Y-%m-%dT%H:%M:%S"
                )

        # Categorical columns (unique_count <= 10)
        if unique_count <= 10 and unique_count > 0:
            has_categorical = True
            categorical_cols.append(col_name_str)

            # Get top 5 most common values
            value_counts = col.value_counts(dropna=False).head(5)
            top_k = [
                {"value": None if pd.isna(val) else val, "count": int(count)}
                for val, count in value_counts.items()
            ]
            col_info["top_k"] = top_k

        columns.append(col_info)

    # Pivot candidates: pairs of categorical columns (unique_count <= 10)
    pivot_candidates = []
    for i, col1 in enumerate(categorical_cols):
        for col2 in categorical_cols[i + 1 :]:
            pivot_candidates.append([col1, col2])
            if len(pivot_candidates) >= 8:
                break
        if len(pivot_candidates) >= 8:
            break

    features = {
        "has_numeric": has_numeric,
        "has_datetime": has_datetime,
        "has_categorical": has_categorical,
        "pivot_candidates": pivot_candidates,
        "wide_to_long_candidates": [],  # empty for now
    }

    return JSONResponse(
        jsonable_encoder(
            {
                "n_rows": n_rows,
                "n_cols": n_cols,
                "columns": columns,
                "features": features,
            }
        )
    )
