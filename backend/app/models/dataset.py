from __future__ import annotations

import uuid

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import relationship

from app.utils.database import Base


class Dataset(Base):
    """Stores metadata about an uploaded dataset."""

    __tablename__ = "datasets"

    id = sa.Column(sa.Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = sa.Column(sa.Text(), nullable=False)
    file_path = sa.Column(sa.Text(), nullable=False)
    row_count = sa.Column(sa.Integer(), nullable=False)
    col_count = sa.Column(sa.Integer(), nullable=False)
    schema_json = sa.Column(
        sa.JSON().with_variant(postgresql.JSONB, "postgresql"),
        nullable=True,
    )
    fact_cache = sa.Column(
        sa.JSON().with_variant(postgresql.JSONB, "postgresql"),
        nullable=True,
    )
    created_at = sa.Column(
        sa.DateTime(timezone=True),
        nullable=False,
        server_default=sa.func.now(),
    )

    history = relationship("QueryHistory", back_populates="dataset", cascade="all, delete-orphan")
