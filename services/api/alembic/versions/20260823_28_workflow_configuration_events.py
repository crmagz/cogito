"""Record auditable lifecycle transitions for workflow configuration.

Revision ID: 20260823_28
Revises: 20260822_27
Create Date: 2026-08-23
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260823_28"
down_revision = "20260822_27"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Keep configuration state transitions append-only and attributable."""

    op.create_table(
        "workflow_configuration_events",
        sa.Column("event_id", sa.String(length=64), primary_key=True),
        sa.Column("kind", sa.String(length=32), nullable=False),
        sa.Column("identifier", sa.String(length=128), nullable=False),
        sa.Column("version", sa.String(length=32), nullable=False),
        sa.Column("from_state", sa.String(length=32), nullable=False),
        sa.Column("to_state", sa.String(length=32), nullable=False),
        sa.Column("actor_id", sa.String(length=512), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "workflow_configuration_events_lookup",
        "workflow_configuration_events",
        ["kind", "identifier", "version", "created_at"],
    )


def downgrade() -> None:
    """Supervisor migrations are intentionally forward-only."""

    raise RuntimeError("Cogito supervisor migrations are forward-only")
