"""Persist immutable product-specification draft revisions.

Revision ID: 20260812_18
Revises: 20260807_17
Create Date: 2026-08-12
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260812_18"
down_revision = "20260807_17"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Add append-only product-specification provenance without rewriting legacy intake."""

    op.add_column("supervisor_runs", sa.Column("product_specification_artifact_ref", sa.Text(), nullable=True))
    op.add_column(
        "supervisor_runs", sa.Column("product_specification_artifact_sha256", sa.String(length=64), nullable=True)
    )
    op.add_column(
        "supervisor_runs",
        sa.Column("product_specification_revision", sa.Integer(), nullable=False, server_default="0"),
    )
    op.create_check_constraint(
        "supervisor_runs_product_specification_identity",
        "supervisor_runs",
        "(product_specification_artifact_ref IS NULL) = (product_specification_artifact_sha256 IS NULL)",
    )
    op.create_table(
        "product_specification_revisions",
        sa.Column("run_id", sa.String(length=36), nullable=False),
        sa.Column("revision", sa.Integer(), nullable=False),
        sa.Column("artifact_ref", sa.Text(), nullable=False),
        sa.Column("artifact_sha256", sa.String(length=64), nullable=False),
        sa.Column("planner_model", sa.String(length=128), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["run_id"], ["supervisor_runs.run_id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("run_id", "revision", name="product_specification_revisions_pkey"),
        sa.UniqueConstraint("run_id", "artifact_sha256", name="product_specification_revisions_identity"),
        sa.CheckConstraint("revision > 0", name="product_specification_revisions_positive_revision"),
        sa.CheckConstraint(
            "artifact_sha256 ~ '^[a-f0-9]{64}$'", name="product_specification_revisions_valid_digest"
        ),
    )
    op.create_index(
        "product_specification_revisions_run_created",
        "product_specification_revisions",
        ["run_id", "created_at"],
    )


def downgrade() -> None:
    """Supervisor migrations are intentionally forward-only."""

    raise RuntimeError("Cogito supervisor migrations are forward-only")
