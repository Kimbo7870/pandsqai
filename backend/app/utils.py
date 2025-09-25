from __future__ import annotations
import hashlib
import pandas as pd


def get_content_hash(df: pd.DataFrame) -> str:
    """takes in dataframe, uses column names, first, and last row, as the fingerprint, for deterministic sampling"""
    parts = [
        ",".join(df.columns.astype(str)),
        df.iloc[0].to_json() if len(df) > 0 else "",
        df.iloc[-1].to_json() if len(df) > 0 else "",
        str(len(df)),
        str(df.shape[1]),
    ]
    s = "|".join(parts)
    return hashlib.sha256(s.encode()).hexdigest()[:16]


def get_hash_seed(content_hash: str, key: str) -> int:
    """Mix content hash + key into a 32-bit-ish seed."""
    h = hashlib.sha256(f"{content_hash}{key}".encode()).hexdigest()
    return int(h[:8], 16)
