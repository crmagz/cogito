"""Persist terminal execution failures separately from planning failures.

Revision ID: 20260823_30
Revises: 20260823_29
Create Date: 2026-08-23
"""

from __future__ import annotations

from alembic import op

revision = "20260823_30"
down_revision = "20260823_29"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Allow the execution worker to record a post-approval failure."""

    op.drop_constraint("supervisor_runs_valid_status", "supervisor_runs", type_="check")
    op.create_check_constraint(
        "supervisor_runs_valid_status",
        "supervisor_runs",
        "status IN ('planning', 'awaiting_plan_approval', 'implementing', "
        "'awaiting_implementation_approval', 'finalizing', 'completed', "
        "'planning_failed', 'implementation_failed', 'rejected', "
        "'revision_requested', 'cancelled')",
    )


def downgrade() -> None:
    """Supervisor migrations are intentionally forward-only."""

    raise RuntimeError("Cogito supervisor migrations are forward-only")
