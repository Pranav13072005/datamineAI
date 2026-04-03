from __future__ import annotations

import uuid

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import relationship

from app.utils.database import Base


try:
    from pgvector.sqlalchemy import Vector as _Vector  # type: ignore
except Exception:  # pragma: no cover
    _Vector = None


class QueryHistory(Base):
    """Stores past questions and results for a dataset."""

    __tablename__ = "query_history"

    id = sa.Column(sa.Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    dataset_id = sa.Column(
        sa.Uuid(as_uuid=True),
        sa.ForeignKey("datasets.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    question = sa.Column(sa.Text(), nullable=False)

    # Postgres: vector(384) via pgvector
    # SQLite/dev: store as JSON array for compatibility
    question_embedding = sa.Column(
        (sa.JSON().with_variant(_Vector(384), "postgresql") if _Vector is not None else sa.JSON()),
        nullable=True,
    )

    # First 200 chars of the answer for quick previews.
    answer_summary = sa.Column(sa.Text(), nullable=True)
    response_json = sa.Column(
        sa.JSON().with_variant(postgresql.JSONB, "postgresql"),
        nullable=False,
    )
    query_type = sa.Column(sa.Text(), nullable=False)
    created_at = sa.Column(
        sa.DateTime(timezone=True),
        nullable=False,
        server_default=sa.func.now(),
        index=True,
    )

    dataset = relationship("Dataset", back_populates="history")
