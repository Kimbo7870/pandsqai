from __future__ import annotations

import decimal
import json
import math
import re
import shutil
import duckdb

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


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _duckdb_register_current_tables(
    con: "duckdb.DuckDBPyConnection",
) -> dict[str, list[str]]:
    """
    Registers current ordered datasets as t1/t2/t3 views.
    Returns mapping: {"t1": [cols...], ...}
    """
    ids = _read_current_ids()
    if len(ids) == 0:
        raise api_error(400, "NO_DATASETS", "No multifile datasets loaded")

    table_cols: dict[str, list[str]] = {}

    for idx, dataset_id in enumerate(ids[:3], start=1):
        tname = f"t{idx}"
        parquet_path = (_dataset_dir(dataset_id) / "df.parquet").resolve()
        if not parquet_path.exists():
            raise api_error(
                500,
                "STORAGE_INCONSISTENT",
                f"Missing parquet for {dataset_id}. Try deleting and re-uploading.",
            )

        # DuckDB wants a string path; ensure quotes are safe
        path_sql = str(parquet_path).replace("'", "''")
        con.execute(
            f"CREATE OR REPLACE VIEW {tname} AS SELECT * FROM read_parquet('{path_sql}')"
        )

        info = con.execute(f"PRAGMA table_info('{tname}')").fetchall()
        cols = [row[1] for row in info]  # row[1] is column name
        table_cols[tname] = cols

    return table_cols


def _parse_csv_list(s: str) -> list[str]:
    return [x.strip() for x in s.split(",") if x.strip()]


def _ops_validate_table(table: str, table_cols: dict[str, list[str]]) -> None:
    if table not in table_cols:
        raise api_error(
            400,
            "OPS_VALIDATION_ERROR",
            f"Unknown table '{table}'. Available: {', '.join(sorted(table_cols.keys()))}",
        )


def _ops_validate_cols_exist(cols: list[str], available: list[str], ctx: str) -> None:
    missing = [c for c in cols if c not in available]
    if missing:
        raise api_error(
            400,
            "OPS_VALIDATION_ERROR",
            f"Missing column(s) in {ctx}: {missing}. Available: {available}",
        )


def _ops_literal_cmp_expr(
    col_sql: str,
    cmp: str,
    value,
    params: list,
) -> str:
    # handle null comparisons
    if value is None:
        if cmp == "==":
            return f"{col_sql} IS NULL"
        if cmp == "!=":
            return f"{col_sql} IS NOT NULL"
        raise api_error(
            400,
            "OPS_VALIDATION_ERROR",
            f"Comparator '{cmp}' cannot be used with null; use == or !=",
        )

    op_map = {"==": "=", "!=": "<>", "<": "<", "<=": "<=", ">": ">", ">=": ">="}
    if cmp not in op_map:
        raise api_error(400, "OPS_VALIDATION_ERROR", f"Unsupported comparator '{cmp}'")

    params.append(value)
    return f"{col_sql} {op_map[cmp]} ?"


def _ops_build_ctes(
    steps: list[dict],
    table_cols: dict[str, list[str]],
) -> tuple[str, list, list[str]]:
    """
    Returns (sql_with_ctes_and_final_select, params, final_columns)
    We simulate/track the column names to validate later steps.
    """
    if not steps or not isinstance(steps, list):
        raise api_error(400, "OPS_VALIDATION_ERROR", "steps must be a non-empty list")

    # first step must be source
    first = steps[0]
    if not isinstance(first, dict) or first.get("op") != "source":
        raise api_error(400, "OPS_VALIDATION_ERROR", "First step must be op='source'")

    base_table = first.get("table")
    if not isinstance(base_table, str):
        raise api_error(400, "OPS_VALIDATION_ERROR", "source.table must be a string")
    _ops_validate_table(base_table, table_cols)

    params: list = []
    ctes: list[tuple[str, str]] = []
    cur_cols = list(table_cols[base_table])

    ctes.append(("q0", f"SELECT * FROM {base_table}"))
    prev = "q0"
    qi = 1

    for step in steps[1:]:
        if not isinstance(step, dict):
            raise api_error(400, "OPS_VALIDATION_ERROR", "Each step must be an object")

        op = step.get("op")
        if op == "select":
            cols = step.get("columns")
            if not isinstance(cols, list) or not all(isinstance(x, str) for x in cols):
                raise api_error(
                    400, "OPS_VALIDATION_ERROR", "select.columns must be string[]"
                )
            _ops_validate_cols_exist(cols, cur_cols, ctx="select")
            sel = ", ".join(_quote_ident(c) for c in cols)
            ctes.append((f"q{qi}", f"SELECT {sel} FROM {prev}"))
            cur_cols = list(cols)

        elif op == "filter":
            conds = step.get("conditions")
            if not isinstance(conds, list) or not conds:
                raise api_error(
                    400,
                    "OPS_VALIDATION_ERROR",
                    "filter.conditions must be a non-empty array",
                )

            parts: list[str] = []
            for cond in conds:
                if not isinstance(cond, dict):
                    raise api_error(
                        400, "OPS_VALIDATION_ERROR", "Each condition must be an object"
                    )
                column = cond.get("column")
                cmp = cond.get("cmp")
                value = cond.get("value")
                if not isinstance(column, str) or not isinstance(cmp, str):
                    raise api_error(
                        400,
                        "OPS_VALIDATION_ERROR",
                        "filter condition requires column (string) and cmp (string)",
                    )
                _ops_validate_cols_exist([column], cur_cols, ctx="filter")
                col_sql = f"{prev}.{_quote_ident(column)}"
                parts.append(_ops_literal_cmp_expr(col_sql, cmp, value, params))

            where_sql = " AND ".join(parts)
            ctes.append((f"q{qi}", f"SELECT * FROM {prev} WHERE {where_sql}"))

        elif op == "merge":
            right_table = step.get("right_table")
            how = step.get("how")
            left_on = step.get("left_on")
            right_on = step.get("right_on")

            if not isinstance(right_table, str):
                raise api_error(
                    400, "OPS_VALIDATION_ERROR", "merge.right_table must be a string"
                )
            _ops_validate_table(right_table, table_cols)

            if how not in ("inner", "left", "right", "outer"):
                raise api_error(
                    400,
                    "OPS_VALIDATION_ERROR",
                    "merge.how must be one of inner/left/right/outer",
                )

            if (
                not isinstance(left_on, list)
                or not isinstance(right_on, list)
                or not left_on
                or len(left_on) != len(right_on)
                or not all(isinstance(x, str) for x in left_on)
                or not all(isinstance(x, str) for x in right_on)
            ):
                raise api_error(
                    400,
                    "OPS_VALIDATION_ERROR",
                    "merge.left_on and merge.right_on must be string[] of same non-zero length",
                )

            _ops_validate_cols_exist(left_on, cur_cols, ctx="merge left_on")
            _ops_validate_cols_exist(
                right_on, table_cols[right_table], ctx="merge right_on"
            )

            join_map = {
                "inner": "INNER JOIN",
                "left": "LEFT JOIN",
                "right": "RIGHT JOIN",
                "outer": "FULL OUTER JOIN",
            }

            on_parts: list[str] = []
            for lo, ro in zip(left_on, right_on):
                on_parts.append(f"l.{_quote_ident(lo)} = r.{_quote_ident(ro)}")
            on_sql = " AND ".join(on_parts)

            # build a predictable column list (no duplicates)
            out_names: set[str] = set()
            select_exprs: list[str] = []

            # left columns preserved
            for c in cur_cols:
                out_names.add(c)
                select_exprs.append(f"l.{_quote_ident(c)} AS {_quote_ident(c)}")

            # skip right join keys when identical to left join keys
            right_skip = {ro for lo, ro in zip(left_on, right_on) if lo == ro}

            right_cols = list(table_cols[right_table])
            right_out_cols: list[str] = []
            for c in right_cols:
                if c in right_skip:
                    continue
                out = c
                if out in out_names:
                    out = f"{c}_right"
                i2 = 2
                while out in out_names:
                    out = f"{c}_right{i2}"
                    i2 += 1
                out_names.add(out)
                right_out_cols.append(out)
                select_exprs.append(f"r.{_quote_ident(c)} AS {_quote_ident(out)}")

            cur_cols = cur_cols + right_out_cols

            select_sql = ", ".join(select_exprs)
            ctes.append(
                (
                    f"q{qi}",
                    f"SELECT {select_sql} FROM {prev} AS l {join_map[how]} {right_table} AS r ON {on_sql}",
                )
            )

        elif op == "groupby":
            by = step.get("by")
            aggs = step.get("aggs")

            if (
                not isinstance(by, list)
                or not all(isinstance(x, str) for x in by)
                or not by
            ):
                raise api_error(
                    400,
                    "OPS_VALIDATION_ERROR",
                    "groupby.by must be a non-empty string[]",
                )
            if not isinstance(aggs, list) or not aggs:
                raise api_error(
                    400,
                    "OPS_VALIDATION_ERROR",
                    "groupby.aggs must be a non-empty array",
                )

            _ops_validate_cols_exist(by, cur_cols, ctx="groupby.by")

            agg_exprs: list[str] = []
            out_cols = list(by)
            allowed_fns = {"sum", "avg", "count", "min", "max"}

            for a in aggs:
                if not isinstance(a, dict):
                    raise api_error(
                        400, "OPS_VALIDATION_ERROR", "Each agg must be an object"
                    )
                col = a.get("column")
                fn = a.get("fn")
                as_name = a.get("as")

                if not isinstance(fn, str) or fn not in allowed_fns:
                    raise api_error(
                        400,
                        "OPS_VALIDATION_ERROR",
                        f"agg.fn must be one of {sorted(allowed_fns)}",
                    )
                if not isinstance(as_name, str) or not as_name.strip():
                    raise api_error(
                        400, "OPS_VALIDATION_ERROR", "agg.as must be a non-empty string"
                    )

                if fn == "count" and col == "*":
                    expr = "COUNT(*)"
                else:
                    if not isinstance(col, str) or not col.strip():
                        raise api_error(
                            400,
                            "OPS_VALIDATION_ERROR",
                            "agg.column must be a string (or '*' for count)",
                        )
                    _ops_validate_cols_exist([col], cur_cols, ctx="groupby.aggs")
                    expr = f"{fn.upper()}({_quote_ident(col)})"

                agg_exprs.append(f"{expr} AS {_quote_ident(as_name)}")
                out_cols.append(as_name)

            by_sql = ", ".join(_quote_ident(c) for c in by)
            agg_sql = ", ".join(agg_exprs)
            ctes.append(
                (f"q{qi}", f"SELECT {by_sql}, {agg_sql} FROM {prev} GROUP BY {by_sql}")
            )
            cur_cols = out_cols

        elif op == "sort":
            by = step.get("by")
            ascending = step.get("ascending")

            if (
                not isinstance(by, list)
                or not all(isinstance(x, str) for x in by)
                or not by
            ):
                raise api_error(
                    400, "OPS_VALIDATION_ERROR", "sort.by must be a non-empty string[]"
                )
            _ops_validate_cols_exist(by, cur_cols, ctx="sort.by")

            if ascending is None:
                ascending_list = [True] * len(by)
            else:
                if not isinstance(ascending, list) or not all(
                    isinstance(x, bool) for x in ascending
                ):
                    raise api_error(
                        400,
                        "OPS_VALIDATION_ERROR",
                        "sort.ascending must be boolean[] if provided",
                    )
                # pad/truncate
                ascending_list = (ascending + [True] * len(by))[: len(by)]

            parts: list[str] = []
            for c, asc in zip(by, ascending_list):
                parts.append(f"{_quote_ident(c)} {'ASC' if asc else 'DESC'}")

            order_sql = ", ".join(parts)
            ctes.append((f"q{qi}", f"SELECT * FROM {prev} ORDER BY {order_sql}"))

        elif op == "limit":
            n = step.get("n")
            if not isinstance(n, int) or n <= 0:
                raise api_error(
                    400, "OPS_VALIDATION_ERROR", "limit.n must be a positive integer"
                )
            n = min(n, 100000)  # hard safety cap
            ctes.append((f"q{qi}", f"SELECT * FROM {prev} LIMIT {n}"))

        elif op == "source":
            raise api_error(
                400, "OPS_VALIDATION_ERROR", "Only the first step may be 'source'"
            )

        else:
            raise api_error(400, "OPS_VALIDATION_ERROR", f"Unsupported op '{op}'")

        prev = f"q{qi}"
        qi += 1

    # build final SQL
    with_parts = []
    for name, sql in ctes:
        with_parts.append(f"{name} AS ({sql})")
    with_sql = "WITH " + ", ".join(with_parts)

    final_sql = f"{with_sql} SELECT * FROM {prev}"
    return final_sql, params, cur_cols


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
    max_rows: int | None = None  # default computed (but we also cap)


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
            con.execute(
                f"CREATE OR REPLACE VIEW t{i} AS SELECT * FROM read_parquet('{p}')"
            )

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
        base_rows = (
            int(req.max_rows) if req.max_rows is not None else default_max_rows_cap
        )
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


class MultiOpsRequest(BaseModel):
    steps: list[dict]
    max_cells: int | None = None
    max_rows: int | None = None


@router.post("/ops")
async def multifile_ops(req: MultiOpsRequest):
    # defaults + caps
    max_cells = int(req.max_cells or 20000)
    max_cells = max(1000, min(200000, max_cells))

    hard_max_rows = 5000
    max_rows_req = int(req.max_rows or hard_max_rows)
    max_rows_req = max(1, min(hard_max_rows, max_rows_req))

    con = duckdb.connect(database=":memory:")
    try:
        table_cols = _duckdb_register_current_tables(con)

        # build SQL from ops
        sql, params, final_cols = _ops_build_ctes(req.steps, table_cols)

        k = max(1, len(final_cols))
        limit_by_cells = max(1, max_cells // k)
        limit_rows = min(max_rows_req, limit_by_cells)
        limit_plus_one = limit_rows + 1

        # apply truncation cap outside pipeline
        sql_limited = f"SELECT * FROM ({sql}) AS q LIMIT {limit_plus_one}"

        df = con.execute(sql_limited, params).fetchdf()
        truncated = len(df) > limit_rows
        if truncated:
            df = df.head(limit_rows)

        columns = list(df.columns)
        rows = _df_to_rows_2d(df)

        note = None
        if truncated:
            note = f"Truncated to {limit_rows} rows (max_cells={max_cells})."

        return JSONResponse(
            jsonable_encoder(
                {"columns": columns, "rows": rows, "truncated": truncated, "note": note}
            )
        )

    except duckdb.Error as e:
        raise api_error(400, "OPS_ERROR", f"DuckDB error: {e}")
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
