"""Persist immutable specification evaluations and their selected evidence.

Revision ID: 20260814_22
Revises: 20260812_21
Create Date: 2026-08-14
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260814_22"
down_revision = "20260812_21"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Add only forward-compatible, immutable evaluation provenance."""

    op.add_column("supervisor_runs", sa.Column("specification_evaluation_artifact_ref", sa.Text(), nullable=True))
    op.add_column("supervisor_runs", sa.Column("specification_evaluation_artifact_sha256", sa.String(length=64), nullable=True))
    op.add_column("supervisor_runs", sa.Column("specification_evaluation_readiness", sa.String(length=32), nullable=True))
    op.add_column("supervisor_runs", sa.Column("selected_specification_evaluation_artifact_ref", sa.Text(), nullable=True))
    op.add_column("supervisor_runs", sa.Column("selected_specification_evaluation_artifact_sha256", sa.String(length=64), nullable=True))
    op.create_table(
        "specification_evaluations",
        sa.Column("run_id", sa.String(length=36), nullable=False),
        sa.Column("specification_revision", sa.Integer(), nullable=False),
        sa.Column("specification_sha256", sa.String(length=64), nullable=False),
        sa.Column("artifact_ref", sa.Text(), nullable=False),
        sa.Column("artifact_sha256", sa.String(length=64), nullable=False),
        sa.Column("readiness", sa.String(length=32), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["run_id"], ["supervisor_runs.run_id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("run_id", "specification_revision", name="specification_evaluations_pkey"),
        sa.CheckConstraint("specification_revision > 0", name="specification_evaluations_positive_revision"),
        sa.CheckConstraint("specification_sha256 ~ '^[a-f0-9]{64}$'", name="specification_evaluations_valid_spec_digest"),
        sa.CheckConstraint("artifact_sha256 ~ '^[a-f0-9]{64}$'", name="specification_evaluations_valid_artifact_digest"),
        sa.CheckConstraint("readiness IN ('ready', 'needs_revision', 'waived')", name="specification_evaluations_valid_readiness"),
    )


def downgrade() -> None:
    """Supervisor migrations are intentionally forward-only."""

    raise RuntimeError("Cogito supervisor migrations are forward-only")
