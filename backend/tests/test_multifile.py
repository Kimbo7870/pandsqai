import io
from pathlib import Path

import pytest
from httpx import ASGITransport, AsyncClient

from app.config import settings
from app.main import app


def _patch_multifile_data_dir(tmp_path: Path) -> None:
    settings.MULTIFILE_DATA_DIR = tmp_path
    tmp_path.mkdir(parents=True, exist_ok=True)


@pytest.mark.asyncio
async def test_multifile_upload_then_current(tmp_path: Path):
    _patch_multifile_data_dir(tmp_path)
    csv = b"a,b\n1,2\n3,4\n"

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        files = {"file": ("tiny.csv", io.BytesIO(csv), "text/csv")}
        r1 = await ac.post("/multifile/upload", files=files)
        assert r1.status_code == 200
        j1 = r1.json()
        assert j1["slot_count"] == 1
        ds_id = j1["dataset_id"]

        r2 = await ac.get("/multifile/current")
        assert r2.status_code == 200
        j2 = r2.json()

    assert "datasets" in j2
    assert len(j2["datasets"]) == 1
    assert j2["datasets"][0]["dataset_id"] == ds_id


@pytest.mark.asyncio
async def test_multifile_upload_dedup_does_not_consume_slot(tmp_path: Path):
    _patch_multifile_data_dir(tmp_path)
    csv = b"x,y\n10,20\n30,40\n"

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        files = {"file": ("data.csv", io.BytesIO(csv), "text/csv")}
        r1 = await ac.post("/multifile/upload", files=files)
        assert r1.status_code == 200
        j1 = r1.json()
        assert j1["already_present"] is False
        assert j1["slot_count"] == 1
        ds_id_1 = j1["dataset_id"]

        files2 = {"file": ("data.csv", io.BytesIO(csv), "text/csv")}
        r2 = await ac.post("/multifile/upload", files=files2)
        assert r2.status_code == 200
        j2 = r2.json()
        assert j2["already_present"] is True
        assert j2["slot_count"] == 1
        ds_id_2 = j2["dataset_id"]

    assert ds_id_1 == ds_id_2
    subdirs = [p for p in tmp_path.iterdir() if p.is_dir()]
    assert len(subdirs) == 1


@pytest.mark.asyncio
async def test_multifile_slots_full(tmp_path: Path):
    _patch_multifile_data_dir(tmp_path)

    csv1 = b"a,b\n1,2\n"
    csv2 = b"a,b\n3,4\n"
    csv3 = b"a,b\n5,6\n"
    csv4 = b"a,b\n7,8\n"

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        for csv in (csv1, csv2, csv3):
            r = await ac.post(
                "/multifile/upload",
                files={"file": ("data.csv", io.BytesIO(csv), "text/csv")},
            )
            assert r.status_code == 200
        r4 = await ac.post(
            "/multifile/upload",
            files={"file": ("data.csv", io.BytesIO(csv4), "text/csv")},
        )

    assert r4.status_code == 409
    assert r4.json()["detail"]["code"] == "SLOTS_FULL"


@pytest.mark.asyncio
async def test_multifile_delete_one_removes_folder(tmp_path: Path):
    _patch_multifile_data_dir(tmp_path)
    csv1 = b"a,b\n1,2\n"
    csv2 = b"a,b\n3,4\n"
    csv3 = b"a,b\n5,6\n"

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        r1 = await ac.post(
            "/multifile/upload",
            files={"file": ("one.csv", io.BytesIO(csv1), "text/csv")},
        )
        r2 = await ac.post(
            "/multifile/upload",
            files={"file": ("two.csv", io.BytesIO(csv2), "text/csv")},
        )
        r3 = await ac.post(
            "/multifile/upload",
            files={"file": ("three.csv", io.BytesIO(csv3), "text/csv")},
        )

        assert r1.status_code == 200 and r2.status_code == 200 and r3.status_code == 200
        ds_id_2 = r2.json()["dataset_id"]

        delr = await ac.delete(f"/multifile/current/{ds_id_2}")
        assert delr.status_code == 200
        assert delr.json()["ok"] is True
        assert delr.json()["slot_count"] == 2

        cur = await ac.get("/multifile/current")
        assert cur.status_code == 200
        ids = [d["dataset_id"] for d in cur.json()["datasets"]]

    assert ds_id_2 not in ids
    assert not (tmp_path / ds_id_2).exists()


@pytest.mark.asyncio
async def test_multifile_delete_all(tmp_path: Path):
    _patch_multifile_data_dir(tmp_path)
    csv1 = b"a,b\n1,2\n"
    csv2 = b"a,b\n3,4\n"

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        await ac.post(
            "/multifile/upload",
            files={"file": ("one.csv", io.BytesIO(csv1), "text/csv")},
        )
        await ac.post(
            "/multifile/upload",
            files={"file": ("two.csv", io.BytesIO(csv2), "text/csv")},
        )

        delr = await ac.delete("/multifile/current")
        assert delr.status_code == 200
        assert delr.json()["ok"] is True
        assert delr.json()["slot_count"] == 0

        cur = await ac.get("/multifile/current")
        assert cur.status_code == 200
        assert cur.json()["datasets"] == []

    subdirs = [p for p in tmp_path.iterdir() if p.is_dir()]
    assert len(subdirs) == 0


@pytest.mark.asyncio
async def test_multifile_chunk_basic_shape_and_values(tmp_path: Path):
    _patch_multifile_data_dir(tmp_path)
    csv = b"a,b,c\n1,2,3\n4,5,6\n7,8,9\n"

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        up = await ac.post(
            "/multifile/upload",
            files={"file": ("tiny.csv", io.BytesIO(csv), "text/csv")},
        )
        assert up.status_code == 200
        dataset_id = up.json()["dataset_id"]

        r = await ac.get(
            "/multifile/chunk",
            params={
                "dataset_id": dataset_id,
                "row_start": 0,
                "col_start": 0,
                "n_rows": 2,
                "n_cols": 2,
            },
        )
        assert r.status_code == 200
        j = r.json()

    assert j["dataset_id"] == dataset_id
    assert j["total_rows"] == 3
    assert j["total_cols"] == 3
    assert j["row_start"] == 0
    assert j["col_start"] == 0
    assert j["n_rows"] == 2
    assert j["n_cols"] == 2
    assert j["columns"] == ["a", "b"]
    assert j["rows"] == [[1, 2], [4, 5]]


@pytest.mark.asyncio
async def test_multifile_chunk_clamps_to_100_and_bounds(tmp_path: Path):
    _patch_multifile_data_dir(tmp_path)
    csv = b"a,b,c\n1,2,3\n4,5,6\n7,8,9\n"

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        up = await ac.post(
            "/multifile/upload",
            files={"file": ("tiny.csv", io.BytesIO(csv), "text/csv")},
        )
        assert up.status_code == 200
        dataset_id = up.json()["dataset_id"]

        r = await ac.get(
            "/multifile/chunk",
            params={
                "dataset_id": dataset_id,
                "row_start": 0,
                "col_start": 0,
                "n_rows": 999,
                "n_cols": 999,
            },
        )
        assert r.status_code == 200
        j = r.json()

    assert j["n_rows"] <= 100
    assert j["n_cols"] <= 100
    assert j["n_rows"] == 3
    assert j["n_cols"] == 3
    assert j["columns"] == ["a", "b", "c"]
    assert j["rows"] == [[1, 2, 3], [4, 5, 6], [7, 8, 9]]


@pytest.mark.asyncio
async def test_multifile_chunk_row_start_beyond_end_clamps(tmp_path: Path):
    _patch_multifile_data_dir(tmp_path)
    csv = b"a,b,c\n1,2,3\n4,5,6\n7,8,9\n"

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        up = await ac.post(
            "/multifile/upload",
            files={"file": ("tiny.csv", io.BytesIO(csv), "text/csv")},
        )
        assert up.status_code == 200
        dataset_id = up.json()["dataset_id"]

        r = await ac.get(
            "/multifile/chunk",
            params={
                "dataset_id": dataset_id,
                "row_start": 999,
                "col_start": 0,
                "n_rows": 2,
                "n_cols": 2,
            },
        )
        assert r.status_code == 200
        j = r.json()

    assert j["row_start"] == 2
    assert j["n_rows"] == 1
    assert j["columns"] == ["a", "b"]
    assert j["rows"] == [[7, 8]]


@pytest.mark.asyncio
async def test_multifile_sql_no_datasets(tmp_path: Path):
    _patch_multifile_data_dir(tmp_path)  # use your helper name
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        r = await ac.post("/multifile/sql", json={"query": "select 1"})
        assert r.status_code == 400
        assert r.json()["detail"]["code"] == "NO_DATASETS"


@pytest.mark.asyncio
async def test_multifile_sql_join_two_tables(tmp_path: Path):
    _patch_multifile_data_dir(tmp_path)
    transport = ASGITransport(app=app)

    csv1 = b"id,a\n1,10\n2,20\n3,30\n"
    csv2 = b"id,b\n1,100\n3,300\n4,400\n"

    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        await ac.post("/multifile/upload", files={"file": ("t1.csv", io.BytesIO(csv1), "text/csv")})
        await ac.post("/multifile/upload", files={"file": ("t2.csv", io.BytesIO(csv2), "text/csv")})

        q = """
        SELECT t1.id, t1.a, t2.b
        FROM t1
        JOIN t2 USING (id)
        ORDER BY id
        """
        r = await ac.post("/multifile/sql", json={"query": q})
        assert r.status_code == 200
        j = r.json()
        assert j["truncated"] is False
        assert j["columns"] == ["id", "a", "b"]
        assert j["rows"] == [[1, 10, 100], [3, 30, 300]]


@pytest.mark.asyncio
async def test_multifile_sql_truncation_flag(tmp_path: Path):
    _patch_multifile_data_dir(tmp_path)
    transport = ASGITransport(app=app)

    rows = "\n".join(str(i) for i in range(10))
    csv = ("x\n" + rows + "\n").encode()

    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        await ac.post("/multifile/upload", files={"file": ("big.csv", io.BytesIO(csv), "text/csv")})

        r = await ac.post(
            "/multifile/sql",
            json={"query": "SELECT * FROM t1 ORDER BY x", "max_rows": 3},
        )
        assert r.status_code == 200
        j = r.json()
        assert j["truncated"] is True
        assert len(j["rows"]) == 3
        assert j["note"] is not None


@pytest.mark.asyncio
async def test_multifile_ops_merge_inner(tmp_path: Path):
    _patch_multifile_data_dir(tmp_path)
    transport = ASGITransport(app=app)

    csv1 = b"id,a\n1,10\n2,20\n3,30\n"
    csv2 = b"id,b\n1,100\n3,300\n4,400\n"

    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        await ac.post(
            "/multifile/upload",
            files={"file": ("t1.csv", io.BytesIO(csv1), "text/csv")},
        )
        await ac.post(
            "/multifile/upload",
            files={"file": ("t2.csv", io.BytesIO(csv2), "text/csv")},
        )

        payload = {
            "steps": [
                {"op": "source", "table": "t1"},
                {
                    "op": "merge",
                    "right_table": "t2",
                    "how": "inner",
                    "left_on": ["id"],
                    "right_on": ["id"],
                },
                {"op": "sort", "by": ["id"], "ascending": [True]},
            ]
        }

        r = await ac.post("/multifile/ops", json=payload)
        assert r.status_code == 200
        j = r.json()
        assert j["truncated"] is False
        assert j["columns"] == ["id", "a", "b"]
        assert j["rows"] == [[1, 10, 100], [3, 30, 300]]


@pytest.mark.asyncio
async def test_multifile_ops_validation_error_missing_column(tmp_path: Path):
    _patch_multifile_data_dir(tmp_path)
    transport = ASGITransport(app=app)

    csv1 = b"id,a\n1,10\n2,20\n"

    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        await ac.post(
            "/multifile/upload",
            files={"file": ("t1.csv", io.BytesIO(csv1), "text/csv")},
        )

        payload = {
            "steps": [
                {"op": "source", "table": "t1"},
                {"op": "filter", "conditions": [{"column": "does_not_exist", "cmp": "==", "value": 1}]},
            ]
        }

        r = await ac.post("/multifile/ops", json=payload)
        assert r.status_code == 400
        assert r.json()["detail"]["code"] == "OPS_VALIDATION_ERROR"
