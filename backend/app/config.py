from __future__ import annotations

from functools import lru_cache
import json
from pathlib import Path

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        # Resolve backend/.env reliably even when running uvicorn from repo root.
        env_file=str(Path(__file__).resolve().parents[1] / ".env"),
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    PROJECT_NAME: str = "AI Data Analyst API"
    ENVIRONMENT: str = "development"  # development|staging|production
    LOG_LEVEL: str = "INFO"

    # CORS
    # In production, set this explicitly (comma-separated or JSON array).
    # In development, we default to common localhost origins and also allow an origin regex.
    CORS_ORIGINS: list[str] = [
        "http://localhost:5173",
        "http://localhost:5174",
        "http://127.0.0.1:5173",
        "http://127.0.0.1:5174",
    ]
    CORS_ORIGIN_REGEX: str | None = r"^https?://(localhost|127\\.0\\.0\\.1)(:\\d+)?$"
    CORS_ALLOW_CREDENTIALS: bool = False

    @field_validator("CORS_ORIGINS", mode="before")
    @classmethod
    def _parse_cors_origins(cls, value):
        if value is None:
            return value

        if isinstance(value, str):
            raw = value.strip()
            if not raw:
                return []

            # Accept JSON array syntax: ["http://...", "http://..."]
            if raw.startswith("["):
                return json.loads(raw)

            # Accept comma-separated values.
            return [item.strip() for item in raw.split(",") if item.strip()]

        return value

    # Database
    # Prefer DATABASE_URL; keep DB_URL and URL_SUPABASE as backwards-compatible aliases.
    DB_URL: str = ""
    DATABASE_URL: str = ""
    URL_SUPABASE: str = ""

    # LLM / integrations
    GROQ_API_KEY: str = ""
    # Note: Groq model IDs change over time; this default matches current Groq /models.
    GROQ_MODEL: str = "llama-3.3-70b-versatile"

    # Embeddings / semantic search
    EMBEDDING_MODEL: str = "all-MiniLM-L6-v2"

    # Local storage
    UPLOAD_DIR: str = "uploaded_datasets"

    # Upload limits
    MAX_UPLOAD_MB: int = 50

    @property
    def SQLALCHEMY_DATABASE_URI(self) -> str:
        return self.DATABASE_URL or self.DB_URL or self.URL_SUPABASE

    @property
    def MAX_UPLOAD_BYTES(self) -> int:
        return int(self.MAX_UPLOAD_MB) * 1024 * 1024


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
