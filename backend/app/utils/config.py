from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    PROJECT_NAME: str = "AI Data Analyst API"
    ENVIRONMENT: str = "development"  # development|staging|production
    LOG_LEVEL: str = "INFO"

    # CORS
    CORS_ORIGINS: list[str] = ["*"]

    # Database
    # Prefer DATABASE_URL; keep URL_SUPABASE as a backwards-compatible alias.
    DATABASE_URL: str = ""
    URL_SUPABASE: str = ""

    # LLM / integrations (kept for later steps)
    GROQ_API_KEY: str = ""

    # Local storage (kept for later steps)
    UPLOAD_DIR: str = "uploaded_datasets"

    @property
    def SQLALCHEMY_DATABASE_URI(self) -> str:
        return self.DATABASE_URL or self.URL_SUPABASE


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
