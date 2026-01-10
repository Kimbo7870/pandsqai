from __future__ import annotations

from pathlib import Path
import pandas as pd


def read_parquet_df(parquet_path: Path) -> pd.DataFrame:
    return pd.read_parquet(parquet_path)


def write_parquet_df(df: pd.DataFrame, parquet_path: Path) -> None:
    # save parquet file without index so easy to read back
    df.to_parquet(parquet_path, index=False)


def get_parquet_row_col_counts(parquet_path: Path) -> tuple[int, int]:
    """Fast row/col counts via parquet metadata, fallback to full read."""
    try:
        import pyarrow.parquet as pq

        pq_file = pq.ParquetFile(parquet_path)
        n_rows = pq_file.metadata.num_rows
        n_cols = pq_file.metadata.num_columns
        return int(n_rows), int(n_cols)
    except Exception:
        df = pd.read_parquet(parquet_path)
        return int(len(df)), int(df.shape[1])
