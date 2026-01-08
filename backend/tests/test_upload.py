import io
from pathlib import Path
import pytest
from httpx import AsyncClient, ASGITransport
from app.main import app
from app.config import settings


def _patch_data_dir(tmp_path: Path):
    """Make the app read/write datasets under the pytest tmp dir."""
    settings.DATA_DIR = tmp_path
    tmp_path.mkdir(parents=True, exist_ok=True)


@pytest.mark.asyncio
async def test_upload_csv():
    csv = b"a,b\n1,2\n3,4\n"
    transport = ASGITransport(app=app)  # <-- new: mount FastAPI app via ASGI transport
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        files = {"file": ("tiny.csv", io.BytesIO(csv), "text/csv")}
        r = await ac.post("/upload", files=files)

    assert r.status_code == 200
    j = r.json()
    assert j["n_rows"] == 2
    assert j["n_cols"] == 2
    assert j["columns"] == ["a", "b"]
    assert "display_name" in j


@pytest.mark.asyncio
async def test_upload_deduplication(tmp_path):
    """Uploading the same file twice should return the same dataset_id and not create duplicate storage."""
    _patch_data_dir(tmp_path)
    csv = b"x,y\n10,20\n30,40\n"
    transport = ASGITransport(app=app)

    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        # First upload
        files1 = {"file": ("data.csv", io.BytesIO(csv), "text/csv")}
        r1 = await ac.post("/upload", files=files1)
        assert r1.status_code == 200
        j1 = r1.json()
        dataset_id_1 = j1["dataset_id"]

        # Second upload of the same content
        files2 = {"file": ("data.csv", io.BytesIO(csv), "text/csv")}
        r2 = await ac.post("/upload", files=files2)
        assert r2.status_code == 200
        j2 = r2.json()
        dataset_id_2 = j2["dataset_id"]

    # Same content should return the same dataset_id
    assert dataset_id_1 == dataset_id_2

    # Verify only one directory was created (no duplicates)
    subdirs = [d for d in tmp_path.iterdir() if d.is_dir()]
    assert len(subdirs) == 1


@pytest.mark.asyncio
async def test_upload_different_files_different_ids(tmp_path):
    """Uploading different files should create different dataset_ids."""
    _patch_data_dir(tmp_path)
    csv1 = b"a,b\n1,2\n"
    csv2 = b"a,b\n3,4\n"
    transport = ASGITransport(app=app)

    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        files1 = {"file": ("data1.csv", io.BytesIO(csv1), "text/csv")}
        r1 = await ac.post("/upload", files=files1)
        assert r1.status_code == 200
        dataset_id_1 = r1.json()["dataset_id"]

        files2 = {"file": ("data2.csv", io.BytesIO(csv2), "text/csv")}
        r2 = await ac.post("/upload", files=files2)
        assert r2.status_code == 200
        dataset_id_2 = r2.json()["dataset_id"]

    # Different content should have different dataset_ids
    assert dataset_id_1 != dataset_id_2

    # Verify two directories were created
    subdirs = [d for d in tmp_path.iterdir() if d.is_dir()]
    assert len(subdirs) == 2


@pytest.mark.asyncio
async def test_upload_duplicate_filename_macos_naming(tmp_path):
    """Uploading different files with the same filename should use macOS-style naming."""
    _patch_data_dir(tmp_path)
    csv1 = b"a,b\n1,2\n"
    csv2 = b"a,b\n3,4\n"
    csv3 = b"a,b\n5,6\n"
    transport = ASGITransport(app=app)

    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        # Upload three different files with the same filename
        files1 = {"file": ("data.csv", io.BytesIO(csv1), "text/csv")}
        r1 = await ac.post("/upload", files=files1)
        assert r1.status_code == 200
        display_name_1 = r1.json()["display_name"]

        files2 = {"file": ("data.csv", io.BytesIO(csv2), "text/csv")}
        r2 = await ac.post("/upload", files=files2)
        assert r2.status_code == 200
        display_name_2 = r2.json()["display_name"]

        files3 = {"file": ("data.csv", io.BytesIO(csv3), "text/csv")}
        r3 = await ac.post("/upload", files=files3)
        assert r3.status_code == 200
        display_name_3 = r3.json()["display_name"]

    # First file keeps original name, subsequent get (1), (2), etc.
    assert display_name_1 == "data.csv"
    assert display_name_2 == "data (1).csv"
    assert display_name_3 == "data (2).csv"


@pytest.mark.asyncio
async def test_list_datasets(tmp_path):
    """Test listing all uploaded datasets."""
    _patch_data_dir(tmp_path)
    csv1 = b"a,b\n1,2\n"
    csv2 = b"x,y\n3,4\n"
    transport = ASGITransport(app=app)

    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        # Upload two files
        files1 = {"file": ("file1.csv", io.BytesIO(csv1), "text/csv")}
        await ac.post("/upload", files=files1)

        files2 = {"file": ("file2.csv", io.BytesIO(csv2), "text/csv")}
        await ac.post("/upload", files=files2)

        # List datasets
        r = await ac.get("/datasets")
        assert r.status_code == 200
        j = r.json()

    assert "datasets" in j
    assert len(j["datasets"]) == 2
    display_names = {ds["display_name"] for ds in j["datasets"]}
    assert "file1.csv" in display_names
    assert "file2.csv" in display_names


@pytest.mark.asyncio
async def test_get_dataset_by_id(tmp_path):
    """Test loading a specific dataset by ID."""
    _patch_data_dir(tmp_path)
    csv = b"a,b\n1,2\n3,4\n"
    transport = ASGITransport(app=app)

    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        # Upload a file
        files = {"file": ("test.csv", io.BytesIO(csv), "text/csv")}
        r1 = await ac.post("/upload", files=files)
        assert r1.status_code == 200
        dataset_id = r1.json()["dataset_id"]

        # Load by ID
        r2 = await ac.get(f"/datasets/{dataset_id}")
        assert r2.status_code == 200
        j = r2.json()

    assert j["dataset_id"] == dataset_id
    assert j["display_name"] == "test.csv"
    assert j["n_rows"] == 2
    assert j["n_cols"] == 2


@pytest.mark.asyncio
async def test_get_dataset_not_found(tmp_path):
    """Test loading a non-existent dataset returns 404."""
    _patch_data_dir(tmp_path)
    transport = ASGITransport(app=app)

    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        r = await ac.get("/datasets/nonexistent123")
        assert r.status_code == 404
