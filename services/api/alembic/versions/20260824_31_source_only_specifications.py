"""Persist source-only product drafts outside planning runs.

Revision ID: 20260824_31
Revises: 20260823_30
Create Date: 2026-08-24
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260824_31"
down_revision = "20260823_30"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Create a deliberately non-promotable source-only draft projection."""

    op.create_table(
        "source_only_specifications",
        sa.Column("specification_id", sa.String(length=64), primary_key=True),
        sa.Column("project_id", sa.String(length=256), nullable=False),
        sa.Column("source_artifact_ref", sa.Text(), nullable=False),
        sa.Column("source_artifact_sha256", sa.String(length=64), nullable=False),
        sa.Column("product_specification_artifact_ref", sa.Text(), nullable=False),
        sa.Column("product_specification_artifact_sha256", sa.String(length=64), nullable=False),
        sa.Column("submitted_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("submitted_by", sa.String(length=512), nullable=False),
        sa.Column("planner_model", sa.String(length=256), nullable=False),
    )
    op.create_index(
        "source_only_specifications_project_submitted_at",
        "source_only_specifications",
        ["project_id", "submitted_at"],
    )


def downgrade() -> None:
    """Supervisor migrations are intentionally forward-only."""

    raise RuntimeError("Cogito supervisor migrations are forward-only")
