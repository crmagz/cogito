"""Persist repository scope with every repository-scoped MCP grant.

Revision ID: 20260807_17
Revises: 20260806_16
Create Date: 2026-08-07
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "20260807_17"
down_revision = "20260806_16"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Store the immutable connector repository identity selected for a run."""

    op.add_column(
        "run_mcp_tool_resolutions",
        sa.Column("repository_scope", sa.String(length=256), nullable=True),
    )


def downgrade() -> None:
    """Supervisor migrations are intentionally forward-only."""

    raise RuntimeError("Cogito supervisor migrations are forward-only")
