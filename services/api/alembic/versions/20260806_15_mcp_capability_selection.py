"""Persist immutable approved MCP capability selections.

Revision ID: 20260806_15
Revises: 20260804_14
Create Date: 2026-08-06
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "20260806_15"
down_revision = "20260804_14"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Add nullable immutable selection evidence without rewriting historic approvals."""

    op.add_column(
        "plan_approval_decisions",
        sa.Column("mcp_selection", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    )


def downgrade() -> None:
    """Supervisor migrations are intentionally forward-only."""

    raise RuntimeError("Cogito supervisor migrations are forward-only")
