from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, Optional
import numpy as np
import pandas as pd


@dataclass
class TemplateContext:
    df: pd.DataFrame
    rng: np.random.Generator


def _human_dtype(s: pd.Series) -> str:
    if pd.api.types.is_integer_dtype(s):
        return "integer"
    if pd.api.types.is_float_dtype(s):
        return "float"
    if pd.api.types.is_bool_dtype(s):
        return "boolean"
    if pd.api.types.is_datetime64_any_dtype(s):
        return "datetime"
    return "string"


def _pick_col(ctx: TemplateContext, pred) -> Optional[str]:
    cols = [c for c in ctx.df.columns if pred(ctx.df[c])]
    if not cols:
        return None
    i = ctx.rng.integers(0, len(cols))
    return str(cols[int(i)])


# return a question per df


def t_col_dtype_id(ctx: TemplateContext) -> Optional[Dict[str, Any]]:
    """mc: identify a column dtype"""
    col = _pick_col(ctx, lambda s: True)
    if col is None:
        return None
    s = ctx.df[col]
    dtype = _human_dtype(s)
    choices = ["integer", "float", "boolean", "datetime", "string"]
    ctx.rng.shuffle(choices)
    return {
        "type": "col_dtype_id",
        "prompt": f"What is the data type of column `{col}`?",
        "choices": choices,
        "answer": dtype,
        "metadata": {"column": col},
    }


def t_col_missing_pct(ctx: TemplateContext) -> Optional[Dict[str, Any]]:
    """find what percent of a column is missing"""
    col = _pick_col(ctx, lambda s: s.isna().any())
    if col is None:
        return None
    s = ctx.df[col]
    pct = float(s.isna().mean() * 100.0)
    return {
        "type": "col_missing_pct",
        "prompt": f"What percentage of values are missing in `{col}`? (round to 1 decimal)",
        "answer": round(pct, 1),
        "metadata": {"column": col, "round": 1},
    }


def t_col_unique_count(ctx: TemplateContext) -> Optional[Dict[str, Any]]:
    """find distinct non-na values"""
    col = _pick_col(ctx, lambda s: True)
    if col is None:
        return None
    nunique = int(ctx.df[col].nunique(dropna=True))
    return {
        "type": "col_unique_count",
        "prompt": f"How many unique non-missing values are in `{col}`?",
        "answer": nunique,
        "metadata": {"column": col},
    }


def t_col_topk_value(ctx: TemplateContext, k: int = 3) -> Optional[Dict[str, Any]]:
    """mc: find most common value"""
    col = _pick_col(
        ctx, lambda s: (not pd.api.types.is_float_dtype(s)) and s.notna().any()
    )  # don't want only float columns
    if col is None:
        return None
    vc = ctx.df[col].dropna().value_counts()
    if vc.empty:
        return None
    top = vc.index.tolist()[: max(1, min(k, len(vc)))]
    if not top:
        return None
    answer = str(top[0])  # most frequent vak
    choices = [str(x) for x in top]
    universe = [
        str(x) for x in ctx.df[col].dropna().unique().tolist() if str(x) not in choices
    ]
    ctx.rng.shuffle(universe)
    while len(choices) < 4 and universe:
        choices.append(universe.pop())
    ctx.rng.shuffle(choices)
    return {
        "type": "col_topk_value",
        "prompt": f"Which value appears most often in `{col}`?",
        "choices": choices,
        "answer": answer,
        "metadata": {"column": col, "k": k},
    }


def _numeric_series(df: pd.DataFrame, col: str) -> Optional[pd.Series]:
    """practice convering strings to numbers"""
    s = df[col]
    if not pd.api.types.is_numeric_dtype(s):
        s = pd.to_numeric(s, errors="coerce")
    if s.dropna().empty:
        return None
    return s


def t_col_numeric_min(ctx: TemplateContext) -> Optional[Dict[str, Any]]:
    col = _pick_col(
        ctx,
        lambda s: pd.api.types.is_numeric_dtype(s) or pd.api.types.is_string_dtype(s),
    )
    if col is None:
        return None
    s = _numeric_series(ctx.df, col)
    if s is None:
        return None
    return {
        "type": "col_numeric_min",
        "prompt": f"What is the minimum value of `{col}`? (exact)",
        "answer": float(s.min()),
        "metadata": {"column": col},
    }


# next three, pick a numeric/string columna and try to finx the min, max, or mean
def t_col_numeric_max(ctx: TemplateContext) -> Optional[Dict[str, Any]]:
    col = _pick_col(
        ctx,
        lambda s: pd.api.types.is_numeric_dtype(s) or pd.api.types.is_string_dtype(s),
    )
    if col is None:
        return None
    s = _numeric_series(ctx.df, col)
    if s is None:
        return None
    return {
        "type": "col_numeric_max",
        "prompt": f"What is the maximum value of `{col}`? (exact)",
        "answer": float(s.max()),
        "metadata": {"column": col},
    }


def t_col_numeric_mean(ctx: TemplateContext) -> Optional[Dict[str, Any]]:
    col = _pick_col(
        ctx,
        lambda s: pd.api.types.is_numeric_dtype(s) or pd.api.types.is_string_dtype(s),
    )
    if col is None:
        return None
    s = _numeric_series(ctx.df, col)
    if s is None:
        return None
    mean = float(s.mean())
    return {
        "type": "col_numeric_mean",
        "prompt": f"What is the mean of `{col}`? (round to 2 decimals)",
        "answer": round(mean, 2),
        "metadata": {"column": col, "round": 2},
    }


def t_col_date_range(ctx: TemplateContext) -> Optional[Dict[str, Any]]:
    col = _pick_col(ctx, lambda s: pd.api.types.is_datetime64_any_dtype(s))
    if col is None:
        return None
    s = ctx.df[col]
    if s.dropna().empty:
        return None
    lo = pd.Timestamp(s.min()).strftime("%Y-%m-%dT%H:%M:%S")
    hi = pd.Timestamp(s.max()).strftime("%Y-%m-%dT%H:%M:%S")
    return {
        "type": "col_date_range",
        "prompt": f"What is the min and max timestamp of `{col}`? (ISO, no timezone)",
        "answer": {"min": lo, "max": hi},
        "metadata": {"column": col},
    }


TEMPLATES = [
    t_col_dtype_id,
    t_col_missing_pct,
    t_col_unique_count,
    t_col_topk_value,
    t_col_numeric_min,
    t_col_numeric_max,
    t_col_numeric_mean,
    t_col_date_range,
]
