import io
import pytest
from httpx import AsyncClient, ASGITransport
from app.main import app


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
