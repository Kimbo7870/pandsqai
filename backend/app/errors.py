# backend/app/errors.py
from fastapi import HTTPException


def api_error(status: int, code: str, detail: str) -> HTTPException:
    # Raise with a dict so the client always sees {code, detail}
    return HTTPException(status_code=status, detail={"code": code, "detail": detail})
