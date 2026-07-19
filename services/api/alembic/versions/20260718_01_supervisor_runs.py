"""Create the durable supervisor run and artifact projection tables.

Revision ID: 20260718_01
Revises:
Create Date: 2026-07-18
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "20260718_01"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Create append-safe records required before approval gates are introduced."""

    op.create_table(
        "supervisor_runs",
        sa.Column("run_id", sa.String(length=36), primary_key=True),
        sa.Column("status", sa.String(length=64), nullable=False),
        sa.Column("source_artifact_ref", sa.Text(), nullable=False),
        sa.Column("source_artifact_sha256", sa.String(length=64), nullable=False),
        sa.Column("target_repos", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("spec_set", sa.Text(), nullable=False),
        sa.Column("constraints", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("priority", sa.String(length=64), nullable=False),
        sa.Column("submitted_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("submitted_by", sa.String(length=256), nullable=False),
        sa.CheckConstraint(
            "status IN ('planning', 'awaiting_plan_approval', 'planning_failed', 'rejected', 'revision_requested')",
            name="supervisor_runs_valid_status",
        ),
    )
    op.create_table(
        "supervisor_artifacts",
        sa.Column("id", sa.BigInteger(), sa.Identity(), primary_key=True),
        sa.Column("run_id", sa.String(length=36), nullable=False),
        sa.Column("artifact_type", sa.String(length=32), nullable=False),
        sa.Column("ref", sa.Text(), nullable=False),
        sa.Column("sha256", sa.String(length=64), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["run_id"], ["supervisor_runs.run_id"], ondelete="RESTRICT"),
        sa.UniqueConstraint("run_id", "artifact_type", "sha256", name="supervisor_artifacts_identity"),
        sa.CheckConstraint("artifact_type IN ('source_spec', 'plan')", name="supervisor_artifacts_valid_type"),
    )


def downgrade() -> None:
    """Refuse destructive schema rollback; restore through a compatible application release."""

    raise RuntimeError("Cogito supervisor migrations are forward-only and cannot be downgraded")
