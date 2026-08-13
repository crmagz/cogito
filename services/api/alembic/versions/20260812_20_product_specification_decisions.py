"""Audit product specification selections and serialize draft generation.

Revision ID: 20260812_20
Revises: 20260812_19
Create Date: 2026-08-12
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260812_20"
down_revision = "20260812_19"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Keep human promotion decisions auditable and prevent competing draft writes."""

    op.add_column("supervisor_runs", sa.Column("product_specification_generation_claim", sa.String(length=36), nullable=True))
    op.create_table(
        "product_specification_selection_decisions",
        sa.Column("decision_id", sa.String(length=36), primary_key=True),
        sa.Column("run_id", sa.String(length=36), nullable=False),
        sa.Column("revision", sa.Integer(), nullable=False),
        sa.Column("artifact_sha256", sa.String(length=64), nullable=False),
        sa.Column("actor_id", sa.String(length=512), nullable=False),
        sa.Column("idempotency_key", sa.String(length=256), nullable=False),
        sa.Column("request_sha256", sa.String(length=64), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["run_id"], ["supervisor_runs.run_id"], ondelete="RESTRICT"),
        sa.UniqueConstraint("run_id", "idempotency_key", name="product_specification_selection_idempotency"),
        sa.CheckConstraint("revision > 0", name="product_specification_selection_positive_revision"),
        sa.CheckConstraint("artifact_sha256 ~ '^[a-f0-9]{64}$'", name="product_specification_selection_valid_digest"),
    )
    op.create_table(
        "product_specification_revision_decisions",
        sa.Column("decision_id", sa.String(length=36), primary_key=True),
        sa.Column("run_id", sa.String(length=36), nullable=False),
        sa.Column("revision", sa.Integer(), nullable=False),
        sa.Column("parent_artifact_sha256", sa.String(length=64), nullable=False),
        sa.Column("actor_id", sa.String(length=512), nullable=False),
        sa.Column("idempotency_key", sa.String(length=256), nullable=False),
        sa.Column("request_sha256", sa.String(length=64), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["run_id"], ["supervisor_runs.run_id"], ondelete="RESTRICT"),
        sa.UniqueConstraint("run_id", "idempotency_key", name="product_specification_revision_idempotency"),
        sa.CheckConstraint("revision > 0", name="product_specification_revision_decision_positive_revision"),
        sa.CheckConstraint("parent_artifact_sha256 ~ '^[a-f0-9]{64}$'", name="product_specification_revision_decision_valid_digest"),
    )


def downgrade() -> None:
    """Supervisor migrations are intentionally forward-only."""

    raise RuntimeError("Cogito supervisor migrations are forward-only")
