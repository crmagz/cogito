"""Persist API-native workflow templates, policies, bindings, and resolutions.

Revision ID: 20260822_27
Revises: 20260822_26
Create Date: 2026-08-22
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260822_27"
down_revision = "20260822_26"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Create append-only configuration storage and per-run immutable contracts."""

    op.create_table(
        "workflow_configuration_versions",
        sa.Column("kind", sa.String(length=32), nullable=False),
        sa.Column("identifier", sa.String(length=128), nullable=False),
        sa.Column("version", sa.String(length=32), nullable=False),
        sa.Column("state", sa.String(length=32), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("payload_sha256", sa.String(length=64), nullable=False),
        sa.Column("created_by", sa.String(length=256), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("kind", "identifier", "version"),
    )
    op.create_table(
        "project_workflow_bindings",
        sa.Column("project_id", sa.String(length=128), primary_key=True),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("updated_by", sa.String(length=256), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_table(
        "run_workflow_resolutions",
        sa.Column("run_id", sa.String(length=64), primary_key=True),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("payload_sha256", sa.String(length=64), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )


def downgrade() -> None:
    """Supervisor migrations are intentionally forward-only."""

    raise RuntimeError("Cogito supervisor migrations are forward-only")
