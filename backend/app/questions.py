from __future__ import annotations
from fastapi import APIRouter
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from typing import Any, Dict, List
import hashlib
import numpy as np
import pandas as pd

from .utils import get_content_hash, get_hash_seed
from .services.questions.templates import TemplateContext, TEMPLATES
from .errors import api_error
from .config import settings


router = APIRouter()


def _qid(dataset_id: str, payload: Dict[str, Any]) -> str:
    """helper when you get the question payload, builds a hash so no dupes"""
    meta = payload.get("metadata", {})
    core = f"{dataset_id}|{payload.get('type')}|{meta.get('column')}|{payload.get('prompt')}"
    return hashlib.sha256(core.encode()).hexdigest()[:12]


@router.get("/questions")
async def get_questions(dataset_id: str, limit: int = 12, seed: int | None = None):
    """Deterministic questions; mix seed with dataset content hash."""
    if limit < 1 or limit > 64:
        raise api_error(400, "BAD_LIMIT", "limit must be between 1 and 64")
    updir = settings.DATA_DIR / dataset_id
    parquet_path = updir / "df.parquet"
    if not updir.exists() or not parquet_path.exists():
        raise api_error(404, "DATASET_NOT_FOUND", "Dataset not found")

    try:
        df = pd.read_parquet(parquet_path)
    except Exception as e:
        raise api_error(400, "PARQUET_READ_FAILED", f"Failed to read dataset: {e}")

    # deterministic but random
    content_hash = get_content_hash(df)
    base_seed = 0 if seed is None else int(seed)
    mix = base_seed ^ get_hash_seed(content_hash, "questions")
    rng = np.random.default_rng(mix)
    ctx = TemplateContext(df=df, rng=rng)

    idxs = list(range(len(TEMPLATES)))
    rng.shuffle(idxs)

    q: List[Dict[str, Any]] = []
    seen_prompts: set[str] = (
        set()
    )  # don't want repeat questions (like same type & prompt)
    lim = max(1, min(64, limit))

    for _ in range(4):  # multiple passes to fill up to limit
        for i in idxs:
            if len(q) >= lim:
                break
            # a payload is a dictionary with one question
            payload = TEMPLATES[i](ctx)
            if not payload:
                continue
            key = f"{payload['type']}|{payload['prompt']}"
            if key in seen_prompts:
                continue
            seen_prompts.add(key)
            payload["id"] = _qid(dataset_id, payload)
            q.append(payload)
        if len(q) >= lim:
            break

    resp = {
        "dataset_id": dataset_id,
        "seed": base_seed,
        "count": len(q),
        "questions": q,
    }
    return JSONResponse(jsonable_encoder(resp))
