.PHONY: dev setup backend-setup frontend-setup dev-backend dev-frontend

PYTHON ?= python3

setup: backend-setup frontend-setup

backend-setup:
	cd backend && $(PYTHON) -m venv .venv
	cd backend && .venv/bin/pip install -U pip
	cd backend && .venv/bin/pip install -r requirements-dev.txt

dev-backend: backend-setup
	cd backend && .venv/bin/python -m uvicorn app.main:app --reload --port 8000

frontend-setup:
	cd frontend && npm ci

dev-frontend: frontend-setup
	cd frontend && npm run dev

dev:
	$(MAKE) -j2 dev-backend dev-frontend
