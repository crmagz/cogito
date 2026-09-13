"""Retain the approved plan that a refinement must extend.

Revision ID: 20260912_35
Revises: 20260912_34
Create Date: 2026-09-12
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op


revision = "20260912_35"
down_revision = "20260912_34"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Keep an immutable base plan reference for additive replacement plans."""

    op.add_column("workflow_refinements", sa.Column("base_plan_artifact_ref", sa.Text(), nullable=True))
    op.add_column("workflow_refinements", sa.Column("base_plan_artifact_sha256", sa.String(length=64), nullable=True))
    op.add_column("workflow_refinements", sa.Column("base_plan_revision", sa.Integer(), nullable=True))
    op.create_check_constraint(
        "workflow_refinements_base_plan_pair",
        "workflow_refinements",
        "(base_plan_artifact_ref IS NULL) = (base_plan_artifact_sha256 IS NULL)",
    )


def downgrade() -> None:
    """Supervisor migrations are intentionally forward-only and cannot be downgraded."""

    raise RuntimeError("Cogito supervisor migrations are forward-only and cannot be downgraded")
