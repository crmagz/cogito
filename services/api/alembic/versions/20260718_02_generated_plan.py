"""Persist the immutable plan produced by the constrained planner.

Revision ID: 20260718_02
Revises: 20260718_01
Create Date: 2026-07-18
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "20260718_02"
down_revision = "20260718_01"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Add current plan identity to the mutable run projection."""

    op.add_column("supervisor_runs", sa.Column("plan_artifact_ref", sa.Text(), nullable=True))
    op.add_column("supervisor_runs", sa.Column("plan_artifact_sha256", sa.String(length=64), nullable=True))
    op.add_column("supervisor_runs", sa.Column("planner_model", sa.String(length=256), nullable=True))


def downgrade() -> None:
    """Refuse destructive schema rollback; restore through a compatible application release."""

    raise RuntimeError("Cogito supervisor migrations are forward-only and cannot be downgraded")
