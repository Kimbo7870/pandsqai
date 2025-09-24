import io
from fastapi.testclient import TestClient
from app.main import app


def test_profile_flow():
    """Test upload then profile"""
    csv = b"a,b,c,d\n1,2,x,2020-01-01\n3,4,y,2020-01-02\n5,6,x,2020-01-03\n"

    client = TestClient(app)

    # Upload
    files = {"file": ("test.csv", io.BytesIO(csv), "text/csv")}
    r = client.post("/upload", files=files)
    assert r.status_code == 200
    dataset_id = r.json()["dataset_id"]

    # Profile
    r = client.get(f"/profile?dataset_id={dataset_id}")
    assert r.status_code == 200
    j = r.json()

    assert j["n_rows"] == 3
    assert j["n_cols"] == 4
    assert len(j["columns"]) == 4
    assert j["features"]["has_numeric"] is True
    assert j["features"]["has_categorical"] is True

    # Check column details
    col_a = next(c for c in j["columns"] if c["name"] == "a")
    assert col_a["unique_count"] == 3
    assert "min" in col_a
    assert "max" in col_a


def test_profile_not_found():
    """Test profile with invalid dataset_id"""
    client = TestClient(app)

    r = client.get("/profile?dataset_id=nonexistent")
    assert r.status_code == 404
