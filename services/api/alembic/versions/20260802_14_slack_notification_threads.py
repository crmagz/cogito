"""Persist Slack run-thread and event-message identities.

Revision ID: 20260802_14
Revises: 20260802_13
Create Date: 2026-08-02
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "20260802_14"
down_revision = "20260802_13"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Record provider-local identities without changing Cogito workflow state."""

    op.create_table(
        "slack_notification_threads",
        sa.Column("run_id", sa.String(length=36), primary_key=True),
        sa.Column("channel_id", sa.String(length=64), nullable=False),
        sa.Column("root_message_ts", sa.String(length=32), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["run_id"], ["supervisor_runs.run_id"], ondelete="RESTRICT"),
    )
    op.create_table(
        "slack_notification_messages",
        sa.Column("event_id", sa.String(length=36), primary_key=True),
        sa.Column("run_id", sa.String(length=36), nullable=False),
        sa.Column("root_message_ts", sa.String(length=32), nullable=False),
        sa.Column("message_ts", sa.String(length=32), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["event_id"], ["coordination_events.event_id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["run_id"], ["supervisor_runs.run_id"], ondelete="RESTRICT"),
    )
    op.create_index("slack_notification_messages_run_order", "slack_notification_messages", ["run_id", "created_at"])


def downgrade() -> None:
    """Supervisor migrations are intentionally forward-only."""

    raise RuntimeError("Cogito supervisor migrations are forward-only")
