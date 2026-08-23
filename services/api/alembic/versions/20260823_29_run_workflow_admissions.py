"""Pin template and policy authority before product-specification review.

Revision ID: 20260823_29
Revises: 20260823_28
Create Date: 2026-08-23
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260823_29"
down_revision = "20260823_28"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Persist immutable admission contracts independently of mutable bindings."""

    op.create_table(
        "run_workflow_admissions",
        sa.Column("run_id", sa.String(length=64), primary_key=True),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("payload_sha256", sa.String(length=64), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )


def downgrade() -> None:
    """Supervisor migrations are intentionally forward-only."""

    raise RuntimeError("Cogito supervisor migrations are forward-only")
