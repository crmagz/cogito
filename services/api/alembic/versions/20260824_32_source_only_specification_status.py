"""Allow persisted source-only intake before any optional model drafting.

Revision ID: 20260824_32
Revises: 20260824_31
Create Date: 2026-08-24
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260824_32"
down_revision = "20260824_31"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Make source-only intake durable even when no model draft is requested."""

    op.add_column(
        "source_only_specifications",
        sa.Column("status", sa.String(length=32), nullable=False, server_default="source_recorded"),
    )
    op.alter_column("source_only_specifications", "product_specification_artifact_ref", nullable=True)
    op.alter_column("source_only_specifications", "product_specification_artifact_sha256", nullable=True)
    op.alter_column("source_only_specifications", "planner_model", nullable=True)


def downgrade() -> None:
    """Supervisor migrations are intentionally forward-only."""

    raise RuntimeError("Cogito supervisor migrations are forward-only")
