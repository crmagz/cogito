"""Bind implementation planning to an explicitly selected product specification.

Revision ID: 20260812_19
Revises: 20260812_18
Create Date: 2026-08-12
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260812_19"
down_revision = "20260812_18"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Add a nullable selected-revision pointer without rewriting legacy intake."""

    op.add_column("supervisor_runs", sa.Column("selected_product_specification_artifact_ref", sa.Text(), nullable=True))
    op.add_column(
        "supervisor_runs",
        sa.Column("selected_product_specification_artifact_sha256", sa.String(length=64), nullable=True),
    )
    op.add_column(
        "supervisor_runs",
        sa.Column("selected_product_specification_revision", sa.Integer(), nullable=True),
    )
    op.create_check_constraint(
        "supervisor_runs_selected_product_specification_identity",
        "supervisor_runs",
        "(selected_product_specification_artifact_ref IS NULL) = "
        "(selected_product_specification_artifact_sha256 IS NULL)",
    )
    op.create_check_constraint(
        "supervisor_runs_selected_product_specification_positive_revision",
        "supervisor_runs",
        "selected_product_specification_revision IS NULL OR selected_product_specification_revision > 0",
    )


def downgrade() -> None:
    """Supervisor migrations are intentionally forward-only."""

    raise RuntimeError("Cogito supervisor migrations are forward-only")
