"""Retain immutable resolved workflows for every plan revision.

Revision ID: 20260912_33
Revises: 20260824_32
Create Date: 2026-09-12
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260912_33"
down_revision = "20260824_32"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Key an immutable workflow resolution by its immutable workflow identity."""

    op.add_column("run_workflow_resolutions", sa.Column("workflow_id", sa.String(length=128), nullable=True))
    op.execute(
        """
        UPDATE run_workflow_resolutions
        SET workflow_id = run_id || ':legacy:' || substr(payload_sha256, 1, 16)
        WHERE workflow_id IS NULL
        """
    )
    op.alter_column("run_workflow_resolutions", "workflow_id", nullable=False)
    op.drop_constraint("run_workflow_resolutions_pkey", "run_workflow_resolutions", type_="primary")
    op.create_primary_key("run_workflow_resolutions_pkey", "run_workflow_resolutions", ["run_id", "workflow_id"])
    op.create_index(
        "run_workflow_resolutions_latest",
        "run_workflow_resolutions",
        ["run_id", "created_at"],
    )


def downgrade() -> None:
    """Supervisor migrations are intentionally forward-only."""

    raise RuntimeError("Cogito supervisor migrations are forward-only")
