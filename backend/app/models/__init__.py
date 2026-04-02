"""SQLAlchemy ORM models."""

from app.models.dataset import Dataset
from app.models.query_history import QueryHistory

# Backwards-compatible alias (older code/tests referenced History)
History = QueryHistory

__all__ = [
    "Dataset",
    "QueryHistory",
    "History",
]
