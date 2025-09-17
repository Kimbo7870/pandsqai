.PHONY: run lint test

run:
	uvicorn app.main:app --reload --host 0.0.0.0 --port 8000

lint:
	ruff check --fix .
	black .
	mypy app

test:
	PYTHONPATH=. pytest -q
