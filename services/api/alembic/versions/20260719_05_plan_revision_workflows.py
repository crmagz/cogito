"""Bind every generated plan version to its own Temporal workflow.

Revision ID: 20260719_05
Revises: 20260718_04
Create Date: 2026-07-19
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "20260719_05"
down_revision = "20260718_04"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Persist the current workflow identity for digest-bound approval delivery."""

    op.add_column("supervisor_runs", sa.Column("active_workflow_id", sa.String(length=128), nullable=True))
    op.add_column("temporal_outbox", sa.Column("workflow_id", sa.String(length=128), nullable=True))
    # Before this migration, each generated plan used the run ID as its
    # workflow ID. Preserve that delivery address for an in-flight approval
    # instead of stranding it after the application starts requiring an
    # explicit workflow identity.
    op.execute(
        "UPDATE supervisor_runs SET active_workflow_id = run_id "
        "WHERE plan_artifact_ref IS NOT NULL AND active_workflow_id IS NULL"
    )
    # Existing outbox rows were created before plan-version workflow IDs and
    # target the run ID. Retain that valid historical delivery address.
    op.execute("UPDATE temporal_outbox SET workflow_id = run_id WHERE workflow_id IS NULL")
    op.alter_column("temporal_outbox", "workflow_id", nullable=False)


def downgrade() -> None:
    """Refuse destructive schema rollback; restore through a compatible application release."""

    raise RuntimeError("Cogito supervisor migrations are forward-only and cannot be downgraded")
