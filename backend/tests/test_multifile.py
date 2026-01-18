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
