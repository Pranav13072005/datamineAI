"""column_registry unique constraint

Revision ID: 0003_column_registry_unique
Revises: 0002_pgvector_column_registry
Create Date: 2026-04-03

"""

from __future__ import annotations

from alembic import op


# revision identifiers, used by Alembic.
revision = "0003_column_registry_unique"
down_revision = "0002_pgvector_column_registry"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Required for ON CONFLICT (dataset_id, column_name) upserts.
    op.create_unique_constraint(
        "uq_column_registry_dataset_id_column_name",
        "column_registry",
        ["dataset_id", "column_name"],
    )


def downgrade() -> None:
    op.drop_constraint(
        "uq_column_registry_dataset_id_column_name",
        "column_registry",
        type_="unique",
    )
