import io
from fastapi.testclient import TestClient
from app.main import app


def _upload_minimal_csv(client: TestClient) -> str:
    csv = b"a,b,c,d\n1,2,x,2020-01-01\n3,4,y,2020-01-02\n5,6,x,2020-01-03\n"
    files = {"file": ("t.csv", io.BytesIO(csv), "text/csv")}
    r = client.post("/upload", files=files)
    assert r.status_code == 200
    return r.json()["dataset_id"]


def test_questions_deterministic_and_nonempty():
    client = TestClient(app)
    ds = _upload_minimal_csv(client)

    r1 = client.get(f"/questions?dataset_id={ds}&limit=8&seed=42")
    r2 = client.get(f"/questions?dataset_id={ds}&limit=8&seed=42")
    assert r1.status_code == 200 and r2.status_code == 200
    j1, j2 = r1.json(), r2.json()

    assert j1["count"] > 0
    assert j1["questions"] == j2["questions"]  # same seed -> identical


def test_questions_changes_with_seed():
    client = TestClient(app)
    ds = _upload_minimal_csv(client)

    r1 = client.get(f"/questions?dataset_id={ds}&limit=6&seed=1")
    r2 = client.get(f"/questions?dataset_id={ds}&limit=6&seed=2")
    assert r1.status_code == 200 and r2.status_code == 200
    q1, q2 = r1.json()["questions"], r2.json()["questions"]
    assert q1 != q2  # likely differs
