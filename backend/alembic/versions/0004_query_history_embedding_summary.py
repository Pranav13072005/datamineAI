"""query_history embedding + answer summary

Revision ID: 0004_query_history_embedding_summary
Revises: 0003_column_registry_unique
Create Date: 2026-04-03

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from pgvector.sqlalchemy import Vector


# revision identifiers, used by Alembic.
revision = "0004_query_history_embedding_summary"
down_revision = "0003_column_registry_unique"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Ensure pgvector extension exists (safe if already installed).
    op.execute("CREATE EXTENSION IF NOT EXISTS vector;")

    op.add_column("query_history", sa.Column("answer_summary", sa.Text(), nullable=True))
    op.add_column("query_history", sa.Column("question_embedding", Vector(384), nullable=True))

    # ANN index for semantic search over past questions.
    # NOTE: ivfflat requires `vector_*_ops` operator class.
    op.execute(
        "CREATE INDEX ix_query_history_question_embedding_ivfflat "
        "ON query_history USING ivfflat (question_embedding vector_cosine_ops) WITH (lists = 100);"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_query_history_question_embedding_ivfflat;")
    op.drop_column("query_history", "question_embedding")
    op.drop_column("query_history", "answer_summary")
