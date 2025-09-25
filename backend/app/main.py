from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from .config import settings
from .api import router as api_router
from .profile import router as profile_router
from .questions import router as questions_router

app = FastAPI(title="PandasQuiz API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[o.strip() for o in settings.CORS_ORIGINS.split(",")],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# endpoints registered
app.include_router(api_router)
app.include_router(profile_router)
app.include_router(questions_router)


# Optional root
@app.get("/")
def root():
    return {"name": "PandasQuiz API"}
