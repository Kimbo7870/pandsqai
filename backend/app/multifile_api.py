from __future__ import annotations

import decimal
import json
import math
import re
import shutil
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeout
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from fastapi import APIRouter, File, Query, UploadFile
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from .config import settings
from .datasets.dedup import get_content_hash_bytes, write_content_hash_file
from .datasets.store import get_parquet_row_col_counts, write_parquet_df
from .errors import api_error

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


def _clamp_int(v: int, lo: int, hi: int) -> int:
    return max(lo, min(hi, v))


def _json_safe_cell(v: Any) -> str | int | float | bool | None:
    """Convert a single cell to JSON-safe primitive."""
    if v is None:
        return None

    # unwrap numpy scalars
    if hasattr(v, "item") and callable(getattr(v, "item")):
        try:
            return _json_safe_cell(v.item())
        except Exception:
            pass

    # pandas NaT
    try:
        if v is pd.NaT:
            return None
    except Exception:
        pass

    if isinstance(v, bool):
        return v
    if isinstance(v, int):
        return v
    if isinstance(v, float):
        if math.isnan(v) or math.isinf(v):
            return None
        return v
    if isinstance(v, str):
        return v

    if isinstance(v, decimal.Decimal):
        try:
            return float(v)
        except Exception:
            return str(v)

    if isinstance(v, (datetime, date)):
        try:
            return v.isoformat()
        except Exception:
            return str(v)

    if isinstance(v, pd.Timestamp):
        try:
            if pd.isna(v):
                return None
            return v.to_pydatetime().isoformat()
        except Exception:
            return str(v)

    if isinstance(v, (bytes, bytearray)):
        try:
            return v.decode("utf-8", errors="replace")
        except Exception:
            return str(v)

    return str(v)


def _df_to_rows_2d(df: pd.DataFrame) -> list[list[str | int | float | bool | None]]:
    # datetimes -> ISO-ish strings
    dt_cols = df.select_dtypes(include=["datetime", "datetimetz"]).columns
    for c in dt_cols:
        df[c] = pd.to_datetime(df[c], errors="coerce").map(
            lambda x: x.isoformat() if pd.notna(x) else None
        )

    # NaN/NaT -> None
    df = df.where(df.notna(), None)

    arr = df.to_numpy(dtype=object)
    return [[_json_safe_cell(v) for v in row] for row in arr.tolist()]


def _read_parquet_chunk_df(
    parquet_path: Path,
    columns: list[str],
    row_start: int,
    n_rows: int,
) -> pd.DataFrame:
    """Read only required row groups intersecting [row_start, row_start+n_rows)."""
    import pyarrow.parquet as pq

    if n_rows <= 0:
        return pd.DataFrame({c: [] for c in columns})

    pf = pq.ParquetFile(parquet_path)
    total_rows = int(pf.metadata.num_rows)
    if total_rows <= 0:
        return pd.DataFrame({c: [] for c in columns})

    row_end = row_start + n_rows

    needed: list[int] = []
    rg_start = 0
    first_rg_start: int | None = None
    for i in range(pf.metadata.num_row_groups):
        rg_rows = int(pf.metadata.row_group(i).num_rows)
        rg_end = rg_start + rg_rows
        if rg_end > row_start and rg_start < row_end:
            needed.append(i)
            if first_rg_start is None:
                first_rg_start = rg_start
        rg_start = rg_end

    if not needed:
        return pd.DataFrame({c: [] for c in columns})

    table = pf.read_row_groups(needed, columns=columns)
    offset = row_start - (first_rg_start or 0)
    table = table.slice(offset, n_rows)
    return table.to_pandas()


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


@router.get("/chunk")
async def multifile_chunk(
    dataset_id: str = Query(...),
    row_start: int = Query(0),
    col_start: int = Query(0),
    n_rows: int = Query(20),
    n_cols: int = Query(20),
):
    parquet_path = _dataset_dir(dataset_id) / "df.parquet"
    if not parquet_path.exists():
        raise api_error(404, "DATASET_NOT_FOUND", f"Dataset {dataset_id} not found")

    import pyarrow.parquet as pq

    pf = pq.ParquetFile(parquet_path)
    total_rows = int(pf.metadata.num_rows)
    full_columns = list(pf.schema_arrow.names)
    total_cols = int(len(full_columns))

    if total_rows <= 0 or total_cols <= 0:
        return JSONResponse(
            jsonable_encoder(
                {
                    "dataset_id": dataset_id,
                    "total_rows": total_rows,
                    "total_cols": total_cols,
                    "row_start": 0,
                    "col_start": 0,
                    "n_rows": 0,
                    "n_cols": 0,
                    "columns": [],
                    "rows": [],
                }
            )
        )

    n_rows_req = _clamp_int(int(n_rows), 1, 100)
    n_cols_req = _clamp_int(int(n_cols), 1, 100)

    row_start_clamped = _clamp_int(int(row_start), 0, max(0, total_rows - 1))
    col_start_clamped = _clamp_int(int(col_start), 0, max(0, total_cols - 1))

    row_end = min(total_rows, row_start_clamped + n_rows_req)
    col_end = min(total_cols, col_start_clamped + n_cols_req)
    n_rows_actual = max(0, row_end - row_start_clamped)
    n_cols_actual = max(0, col_end - col_start_clamped)

    columns = full_columns[col_start_clamped:col_end]

    if n_rows_actual == 0 or n_cols_actual == 0:
        return JSONResponse(
            jsonable_encoder(
                {
                    "dataset_id": dataset_id,
                    "total_rows": total_rows,
                    "total_cols": total_cols,
                    "row_start": row_start_clamped,
                    "col_start": col_start_clamped,
                    "n_rows": n_rows_actual,
                    "n_cols": n_cols_actual,
                    "columns": columns,
                    "rows": [],
                }
            )
        )

    df = _read_parquet_chunk_df(
        parquet_path=parquet_path,
        columns=columns,
        row_start=row_start_clamped,
        n_rows=n_rows_actual,
    )
    rows = _df_to_rows_2d(df)

    return JSONResponse(
        jsonable_encoder(
            {
                "dataset_id": dataset_id,
                "total_rows": total_rows,
                "total_cols": total_cols,
                "row_start": row_start_clamped,
                "col_start": col_start_clamped,
                "n_rows": n_rows_actual,
                "n_cols": n_cols_actual,
                "columns": columns,
                "rows": rows,
            }
        )
    )


# -------------------------
# Chunk 6: SQL via DuckDB
# -------------------------

class MultiSQLRequest(BaseModel):
    query: str
    max_cells: int | None = None  # default 20000
    max_rows: int | None = None   # default computed (but we also cap)


def _normalize_sql(q: str) -> str:
    s = (q or "").strip()
    s = re.sub(r";+\s*$", "", s)  # strip trailing semicolons only
    return s.strip()


def _validate_sql_select_only(q: str) -> None:
    s = _normalize_sql(q)
    if not s:
        raise api_error(400, "BAD_SQL", "Query is empty")
    if ";" in s:
        raise api_error(400, "BAD_SQL", "Only a single SQL statement is allowed")
    head = s.lstrip().lower()
    if not (head.startswith("select") or head.startswith("with")):
        raise api_error(400, "BAD_SQL", "Only SELECT/WITH queries are allowed")


@router.post("/sql")
async def multifile_sql(req: MultiSQLRequest):
    _validate_sql_select_only(req.query)

    ids = _read_current_ids()
    if len(ids) == 0:
        raise api_error(400, "NO_DATASETS", "No multifile datasets are loaded")

    # Limits / protection
    max_cells_default = 20000
    max_cells = int(req.max_cells) if req.max_cells is not None else max_cells_default
    max_cells = _clamp_int(max_cells, 1_000, 200_000)

    # Default max_rows is "computed", but also keep a reasonable cap for safety.
    default_max_rows_cap = 500
    hard_max_rows_cap = 5_000

    user_query = _normalize_sql(req.query)

    try:
        import duckdb  # type: ignore
    except Exception as e:
        raise api_error(500, "MISSING_DEPENDENCY", f"DuckDB not installed: {e}")

    con = duckdb.connect(database=":memory:")
    try:
        # Register t1/t2/t3 based on current order
        for i, dataset_id in enumerate(ids[:3], start=1):
            parquet_path = (_dataset_dir(dataset_id) / "df.parquet").resolve()
            if not parquet_path.exists():
                raise api_error(
                    500,
                    "STORAGE_INCONSISTENT",
                    f"Missing parquet for dataset {dataset_id}",
                )
            p = str(parquet_path).replace("'", "''")
            con.execute(f"CREATE OR REPLACE VIEW t{i} AS SELECT * FROM read_parquet('{p}')")

        # 1) Determine output columns without pulling rows
        try:
            rel = con.sql(f"SELECT * FROM ({user_query}) AS q LIMIT 0")
            columns = list(rel.columns)
        except Exception as e:
            raise api_error(400, "SQL_ERROR", str(e))

        k = max(1, len(columns))
        if k > max_cells:
            raise api_error(
                400,
                "TOO_MANY_COLUMNS",
                f"Query returns {k} columns which exceeds max_cells={max_cells}",
            )

        # 2) Compute max rows we can return under max_cells
        max_rows_by_cells = max(1, max_cells // k)

        # Apply user max_rows (or a safe default cap), then apply cell cap, then a hard cap.
        base_rows = int(req.max_rows) if req.max_rows is not None else default_max_rows_cap
        base_rows = _clamp_int(base_rows, 1, hard_max_rows_cap)
        limit_rows = min(base_rows, max_rows_by_cells)

        # 3) Execute wrapped query with limit+1 to detect truncation
        wrapped = f"SELECT * FROM ({user_query}) AS q LIMIT {limit_rows + 1}"

        timeout_s = 5.0

        def _run_sql() -> pd.DataFrame:
            return con.execute(wrapped).fetchdf()

        try:
            with ThreadPoolExecutor(max_workers=1) as ex:
                fut = ex.submit(_run_sql)
                df = fut.result(timeout=timeout_s)
        except FutureTimeout:
            try:
                con.interrupt()
            except Exception:
                pass
            raise api_error(408, "SQL_TIMEOUT", f"Query exceeded {timeout_s:.0f}s")
        except Exception as e:
            raise api_error(400, "SQL_ERROR", str(e))

        truncated = False
        if len(df) > limit_rows:
            truncated = True
            df = df.iloc[:limit_rows]

        rows = _df_to_rows_2d(df)

        note: str | None = None
        if truncated:
            note = f"Results truncated to {limit_rows} rows to stay under {max_cells} cells."

        return JSONResponse(
            jsonable_encoder(
                {
                    "columns": [str(c) for c in columns],
                    "rows": rows,
                    "truncated": truncated,
                    "note": note,
                }
            )
        )
    finally:
        try:
            con.close()
        except Exception:
            pass


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
