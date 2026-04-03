"""pgvector column registry

Revision ID: 0002_pgvector_column_registry
Revises: 0001_initial
Create Date: 2026-04-03

"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# pgvector SQLAlchemy type
from pgvector.sqlalchemy import Vector


# revision identifiers, used by Alembic.
revision = "0002_pgvector_column_registry"
down_revision = "0001_initial"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Vector extension for pgvector
    op.execute("CREATE EXTENSION IF NOT EXISTS vector;")

    # gen_random_uuid() is provided by pgcrypto on many Postgres installations.
    # Supabase typically has this enabled, but keep it safe.
    op.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto;")

    op.create_table(
        "column_registry",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            nullable=False,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column(
            "dataset_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("datasets.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("column_name", sa.Text(), nullable=False),
        sa.Column("column_dtype", sa.Text(), nullable=True),
        sa.Column(
            "description",
            sa.Text(),
            nullable=True,
            comment='human-readable: "column_name (dtype): sample values"',
        ),
        sa.Column("embedding", Vector(384), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
    )

    op.create_index("ix_column_registry_dataset_id", "column_registry", ["dataset_id"])
    op.create_index("ix_column_registry_column_name", "column_registry", ["column_name"])

    # Fast ANN search index (cosine distance operator <=>).
    # Note: ivfflat requires setting lists; tune as needed.
    op.execute(
        "CREATE INDEX ix_column_registry_embedding_ivfflat "
        "ON column_registry USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_column_registry_embedding_ivfflat;")
    op.drop_index("ix_column_registry_column_name", table_name="column_registry")
    op.drop_index("ix_column_registry_dataset_id", table_name="column_registry")
    op.drop_table("column_registry")
    # Keep extensions (vector/pgcrypto) installed; dropping them can break other objects.
