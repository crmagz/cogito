"""Persist and lease automatic planning generation.

Revision ID: 20260822_26
Revises: 20260815_25
Create Date: 2026-08-22
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260822_26"
down_revision = "20260815_25"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Make accepted product specifications recoverable planner work."""

    op.add_column("agent_runs", sa.Column("planning_generation_claim", sa.String(length=36), nullable=True))
    op.add_column("agent_runs", sa.Column("planning_generation_claimed_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("agent_runs", sa.Column("planning_generation_retry_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column(
        "agent_runs",
        sa.Column("planning_generation_attempt_count", sa.Integer(), nullable=False, server_default="0"),
    )
    op.create_index(
        "agent_runs_pending_planning_generation",
        "agent_runs",
        ["planning_generation_retry_at", "planning_generation_claimed_at"],
        postgresql_where=sa.text("planning_generation_claim IS NULL"),
    )


def downgrade() -> None:
    """Supervisor migrations are intentionally forward-only."""

    raise RuntimeError("Cogito supervisor migrations are forward-only")
