from pathlib import Path
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from fastapi.testclient import TestClient

from app.main import app
from app.config import settings

client = TestClient(app)


def _patch_data_dir(tmp_path: Path):
    # Make the app read/write datasets under the pytest tmp dir
    settings.DATA_DIR = tmp_path
    tmp_path.mkdir(parents=True, exist_ok=True)


def _write_parquet(df: pd.DataFrame, root: Path, dataset_id: str):
    updir = root / dataset_id
    updir.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pandas(df), updir / "df.parquet")


def test_profile_happy(tmp_path):
    _patch_data_dir(tmp_path)
    df = pd.DataFrame({"a": [1, 2, None], "b": ["x", "y", "y"]})
    _write_parquet(df, tmp_path, "ds1")

    r = client.get("/profile", params={"dataset_id": "ds1"})
    assert r.status_code == 200
    j = r.json()
    assert j["n_rows"] == 3
    assert j["n_cols"] == 2
    names = {c["name"] for c in j["columns"]}
    assert {"a", "b"} <= names


def test_profile_not_found(tmp_path):
    _patch_data_dir(tmp_path)
    r = client.get("/profile", params={"dataset_id": "nope"})
    assert r.status_code == 404
    j = r.json()
    assert j["detail"]["code"] == "DATASET_NOT_FOUND"


def test_questions_determinism(tmp_path):
    _patch_data_dir(tmp_path)
    df = pd.DataFrame(
        {
            "age": [10, 20, 30, 40, 50],
            "region": ["A", "B", "A", "B", "A"],
            "ts": pd.to_datetime(
                ["2022-01-01", "2022-01-02", "2022-01-03", "2022-01-04", "2022-01-05"]
            ),
        }
    )
    _write_parquet(df, tmp_path, "quiz")
    params = {"dataset_id": "quiz", "limit": 8, "seed": 42}

    r1 = client.get("/questions", params=params)
    r2 = client.get("/questions", params=params)
    assert r1.status_code == 200 and r2.status_code == 200

    q1 = r1.json()["questions"]
    q2 = r2.json()["questions"]
    ids1 = [q["id"] for q in q1]
    ids2 = [q["id"] for q in q2]
    assert ids1 == ids2  # same order, same IDs


def test_questions_bad_limit(tmp_path):
    _patch_data_dir(tmp_path)
    df = pd.DataFrame({"x": [1, 2, 3]})
    _write_parquet(df, tmp_path, "quiz2")

    r = client.get("/questions", params={"dataset_id": "quiz2", "limit": 0})
    assert r.status_code == 400
    j = r.json()
    assert j["detail"]["code"] == "BAD_LIMIT"
