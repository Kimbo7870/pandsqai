.PHONY: dev dev-backend dev-frontend

dev:
	(cd backend && . .venv/bin/activate && uvicorn app.main:app --reload --port 8000) &
	(cd frontend && npm run dev)

dev-backend:
	(cd backend && . .venv/bin/activate && uvicorn app.main:app --reload --port 8000)

dev-frontend:
	(cd frontend && npm run dev)
