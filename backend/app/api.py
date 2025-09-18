from fastapi import APIRouter, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder  ###
from uuid import uuid4  # helps generate unique id per upload
from pathlib import Path
import pandas as pd

router = APIRouter()  # router for endpoints
DATA_DIR = Path("data")  # store uploads in data for now
DATA_DIR.mkdir(exist_ok=True)


# upload data endpoint
@router.post("/upload")
async def upload(file: UploadFile = File(...)):
    name = file.filename or "upload"
    if not name.lower().endswith(
        (".csv", ".parquet")
    ):  # supporting csv or parquet for now
        raise HTTPException(status_code=400, detail="Only .csv or .parquet allowed")

    # Size guard by reading into bytes and keeping size less than 10 MB
    contents = await file.read()
    if len(contents) > 10 * 1024 * 1024:
        raise HTTPException(status_code=413, detail="File too large (>10MB)")
    # separate each dataset by unique id, and each unique id associates with a folder
    dataset_id = str(uuid4())
    updir = DATA_DIR / dataset_id
    updir.mkdir(parents=True, exist_ok=True)

    # Save raw/original copy
    raw_path = updir / name
    raw_path.write_bytes(contents)

    # Load to pandas
    try:
        if name.lower().endswith(".csv"):
            # pyarrow (fast and hanldes edge cases), dtype_backend=pyarrow keeps better dtypes
            df = pd.read_csv(raw_path, engine="pyarrow", dtype_backend="pyarrow")
        else:
            df = pd.read_parquet(raw_path)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to parse: {e}")

    # save parquet file without index so easy to read back
    parquet_path = updir / "df.parquet"
    df.to_parquet(parquet_path, index=False)

    sample_df = df.head(50).copy()  ###
    dt_cols = sample_df.select_dtypes(include=["datetime", "datetimetz"]).columns  ###
    for c in dt_cols:  ###
        sample_df[c] = pd.to_datetime(sample_df[c]).dt.strftime(
            "%Y-%m-%dT%H:%M:%S"
        )  ###
    sample_df = sample_df.where(sample_df.notna(), None)  ###

    # sample that i'll send to frontend
    sample = sample_df.to_dict(orient="records")
    return JSONResponse(
        jsonable_encoder(
            {  ###
                "dataset_id": dataset_id,  # dataset reference id
                "n_rows": int(len(df)),
                "n_cols": int(df.shape[1]),
                "columns": list(df.columns.map(str)),  # column names as strings
                "sample": sample,  # sample of table
            }
        )
    )
