from __future__ import annotations

import pandas as pd


def make_sample_records(df: pd.DataFrame, n: int = 50) -> list[dict]:
    sample_df = df.head(n).copy()
    dt_cols = sample_df.select_dtypes(include=["datetime", "datetimetz"]).columns
    for c in dt_cols:
        sample_df[c] = pd.to_datetime(sample_df[c]).dt.strftime("%Y-%m-%dT%H:%M:%S")
    sample_df = sample_df.where(sample_df.notna(), None)
    return sample_df.to_dict(orient="records")
