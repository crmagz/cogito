"""Allow operators to terminally cancel a run before planning begins.

Revision ID: 20260815_25
Revises: 20260815_24
Create Date: 2026-08-15
"""

from __future__ import annotations

from alembic import op

revision = "20260815_25"
down_revision = "20260815_24"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_constraint("supervisor_runs_valid_status", "supervisor_runs", type_="check")
    op.create_check_constraint(
        "supervisor_runs_valid_status",
        "supervisor_runs",
        "status IN ('planning', 'awaiting_plan_approval', 'implementing', "
        "'awaiting_implementation_approval', 'finalizing', 'completed', "
        "'planning_failed', 'rejected', 'revision_requested', 'cancelled')",
    )


def downgrade() -> None:
    raise RuntimeError("Cogito supervisor migrations are forward-only")
