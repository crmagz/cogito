"""Create authoritative agent run state and append-only lifecycle events.

Revision ID: 20260719_08
Revises: 20260719_07
Create Date: 2026-07-19
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "20260719_08"
down_revision = "20260719_07"
branch_labels = None
depends_on = None

_STATUSES = "'PENDING', 'QUEUED', 'STARTING', 'RUNNING', 'WAITING_FOR_TOOL', 'WAITING_FOR_APPROVAL', 'SUCCEEDED', 'FAILED', 'CANCELLED', 'TIMED_OUT'"


def upgrade() -> None:
    op.create_table(
        "agent_runs",
        sa.Column("run_id", sa.String(length=36), primary_key=True),
        sa.Column("root_run_id", sa.String(length=36), nullable=False),
        sa.Column("parent_run_id", sa.String(length=36), nullable=True),
        sa.Column("agent_name", sa.String(length=128), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("trace_id", sa.String(length=32), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_heartbeat_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("worker_id", sa.String(length=256), nullable=True),
        sa.Column("result_artifact_uri", sa.Text(), nullable=True),
        sa.Column("error_summary", sa.String(length=4096), nullable=True),
        sa.ForeignKeyConstraint(["parent_run_id"], ["agent_runs.run_id"], ondelete="RESTRICT"),
        sa.CheckConstraint(f"status IN ({_STATUSES})", name="agent_runs_valid_status"),
        sa.CheckConstraint("root_run_id = run_id OR parent_run_id IS NOT NULL", name="agent_runs_root_or_parent"),
        sa.CheckConstraint("trace_id ~ '^[0-9a-f]{32}$'", name="agent_runs_valid_trace_id"),
    )
    op.create_index("agent_runs_root_run_id", "agent_runs", ["root_run_id"])
    op.create_index("agent_runs_parent_run_id", "agent_runs", ["parent_run_id"])
    op.create_index("agent_runs_status", "agent_runs", ["status"])
    op.create_table(
        "agent_run_events",
        sa.Column("event_id", sa.String(length=36), primary_key=True),
        sa.Column("run_id", sa.String(length=36), nullable=False),
        sa.Column("event_type", sa.String(length=64), nullable=False),
        sa.Column("from_status", sa.String(length=32), nullable=True),
        sa.Column("to_status", sa.String(length=32), nullable=True),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("metadata", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.ForeignKeyConstraint(["run_id"], ["agent_runs.run_id"], ondelete="RESTRICT"),
        sa.CheckConstraint(f"from_status IS NULL OR from_status IN ({_STATUSES})", name="agent_run_events_valid_from_status"),
        sa.CheckConstraint(f"to_status IS NULL OR to_status IN ({_STATUSES})", name="agent_run_events_valid_to_status"),
    )
    op.create_index("agent_run_events_run_order", "agent_run_events", ["run_id", "occurred_at"])


def downgrade() -> None:
    raise RuntimeError("Cogito supervisor migrations are forward-only and cannot be downgraded")
