"""Add provider-neutral coordination events and leased notification deliveries.

Revision ID: 20260725_11
Revises: 20260725_10
Create Date: 2026-07-25
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "20260725_11"
down_revision = "20260725_10"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Create append-only events and leased deliveries without changing approval authority."""

    op.create_table(
        "coordination_events",
        sa.Column("event_id", sa.String(length=36), primary_key=True),
        sa.Column("run_id", sa.String(length=36), nullable=False),
        sa.Column("event_type", sa.String(length=96), nullable=False),
        sa.Column("dedupe_key", sa.String(length=64), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["run_id"], ["agent_runs.run_id"], ondelete="RESTRICT"),
        sa.UniqueConstraint("dedupe_key", name="coordination_events_dedupe_key"),
    )
    op.create_index("coordination_events_run_order", "coordination_events", ["run_id", "created_at"])
    op.create_table(
        "notification_outbox",
        sa.Column("event_id", sa.String(length=36), nullable=False),
        sa.Column("sink_id", sa.String(length=64), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("delivered_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("attempt_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("lease_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("last_error", sa.String(length=1024), nullable=True),
        sa.ForeignKeyConstraint(["event_id"], ["coordination_events.event_id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("event_id", "sink_id", name="notification_outbox_pkey"),
    )
    op.create_index(
        "notification_outbox_due",
        "notification_outbox",
        ["sink_id", "delivered_at", "next_attempt_at", "lease_until", "created_at"],
    )


def downgrade() -> None:
    """Supervisor migrations are intentionally forward-only."""

    raise RuntimeError("Cogito supervisor migrations are forward-only and cannot be downgraded")
