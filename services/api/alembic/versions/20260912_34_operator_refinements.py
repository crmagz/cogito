"""Persist operator revision direction for replacement planning artifacts.

Revision ID: 20260912_34
Revises: 20260912_33
Create Date: 2026-09-12
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260912_34"
down_revision = "20260912_33"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Add immutable refinement evidence and a mutable current-input pointer."""

    op.create_table(
        "workflow_refinements",
        sa.Column("refinement_id", sa.String(length=36), primary_key=True),
        sa.Column("run_id", sa.String(length=36), nullable=False),
        sa.Column("source_gate", sa.String(length=32), nullable=False),
        sa.Column("artifact_sha256", sa.String(length=64), nullable=False),
        sa.Column("actor_id", sa.String(length=512), nullable=False),
        sa.Column("comment", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["run_id"], ["supervisor_runs.run_id"], ondelete="RESTRICT"),
        sa.CheckConstraint("source_gate IN ('plan', 'implementation')", name="workflow_refinements_valid_source_gate"),
    )
    op.create_index("workflow_refinements_run_created", "workflow_refinements", ["run_id", "created_at"])
    op.add_column("supervisor_runs", sa.Column("active_refinement_id", sa.String(length=36), nullable=True))
    op.create_foreign_key(
        "supervisor_runs_active_refinement",
        "supervisor_runs",
        "workflow_refinements",
        ["active_refinement_id"],
        ["refinement_id"],
        ondelete="RESTRICT",
    )


def downgrade() -> None:
    """Supervisor migrations are intentionally forward-only."""

    raise RuntimeError("Cogito supervisor migrations are forward-only and cannot be downgraded")
