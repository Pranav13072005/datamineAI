"""
database.py — SQLAlchemy engine, session factory, and ORM Base.

All models import Base from here. All routes/services get a DB session
via the get_db() dependency.
"""

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

from app.utils.config import settings

# Create synchronous SQLAlchemy engine pointing to Supabase (PostgreSQL)
engine = create_engine(
    settings.URL_SUPABASE,
    pool_pre_ping=True,   # Reconnect automatically if connection drops
    echo=False,           # Set True to log all SQL for debugging
)

# Session factory — each request gets its own session
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Declarative base that all ORM models extend
Base = declarative_base()


def get_db():
    """
    FastAPI dependency that yields a database session per request,
    then closes it afterwards. Use with Depends(get_db).
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
