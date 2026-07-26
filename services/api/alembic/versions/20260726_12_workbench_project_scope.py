"""Add default-deny project scope for Workbench reads.

Revision ID: 20260726_12
Revises: 20260725_11
Create Date: 2026-07-26
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "20260726_12"
down_revision = "20260725_11"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Persist scope for new runs without making historical records readable."""

    op.add_column("supervisor_runs", sa.Column("project_id", sa.String(length=128), nullable=True))
    op.create_index("supervisor_runs_project_submitted", "supervisor_runs", ["project_id", "submitted_at"])


def downgrade() -> None:
    """Supervisor migrations are intentionally forward-only."""

    raise RuntimeError("Cogito supervisor migrations are forward-only and cannot be downgraded")
