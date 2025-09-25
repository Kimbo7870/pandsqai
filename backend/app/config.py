from pydantic_settings import BaseSettings, SettingsConfigDict

from pathlib import Path


class Settings(BaseSettings):
    APP_ENV: str = "dev"
    CORS_ORIGINS: str = "http://localhost:5173"
    OPENAI_API_KEY: str | None = None
    DATA_DIR: Path = Path("data")

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


settings = Settings()
settings.DATA_DIR.mkdir(parents=True, exist_ok=True)
