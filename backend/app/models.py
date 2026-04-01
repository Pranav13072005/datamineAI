"""
models.py — SQLAlchemy ORM models for the AI Data Analyst Agent.

Tables:
  - datasets  : Metadata for each uploaded CSV file.
  - history   : Query/answer log linked to a dataset.
"""

import uuid
from datetime import datetime

from sqlalchemy import Column, String, Text, DateTime, ForeignKey
from sqlalchemy.orm import relationship

from app.utils.database import Base


def _uuid() -> str:
    """Generate a new UUID4 string — used as default primary key."""
    return str(uuid.uuid4())


class Dataset(Base):
    """Stores metadata about an uploaded CSV dataset."""

    __tablename__ = "datasets"

    id = Column(String, primary_key=True, default=_uuid)
    name = Column(String, nullable=False)                   # Original filename
    created_at = Column(DateTime, default=datetime.utcnow)  # Upload timestamp

    # One dataset → many history entries
    history = relationship("History", back_populates="dataset", cascade="all, delete")


class History(Base):
    """Stores every question asked against a dataset and the AI answer."""

    __tablename__ = "history"

    id = Column(String, primary_key=True, default=_uuid)
    dataset_id = Column(String, ForeignKey("datasets.id"), nullable=False)
    question = Column(Text, nullable=False)   # User's natural-language question
    answer = Column(Text, nullable=False)     # AI-generated answer / result
    created_at = Column(DateTime, default=datetime.utcnow)

    # Back-reference to the parent dataset
    dataset = relationship("Dataset", back_populates="history")
