"""Persist auditable approval exceptions for specification evaluations.

Revision ID: 20260815_23
Revises: 20260814_22
Create Date: 2026-08-15
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260815_23"
down_revision = "20260814_22"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("supervisor_runs", sa.Column("specification_evaluation_generation_claim", sa.String(length=36), nullable=True))
    op.add_column("supervisor_runs", sa.Column("specification_evaluation_generation_claimed_at", sa.DateTime(timezone=True), nullable=True))
    op.create_table(
        "specification_evaluation_waivers",
        sa.Column("decision_id", sa.String(length=36), primary_key=True),
        sa.Column("run_id", sa.String(length=36), nullable=False),
        sa.Column("artifact_sha256", sa.String(length=64), nullable=False),
        sa.Column("actor_id", sa.String(length=256), nullable=False),
        sa.Column("rationale", sa.Text(), nullable=False),
        sa.Column("idempotency_key", sa.String(length=256), nullable=False),
        sa.Column("request_sha256", sa.String(length=64), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["run_id"], ["supervisor_runs.run_id"], ondelete="RESTRICT"),
        sa.UniqueConstraint("run_id", "idempotency_key", name="specification_evaluation_waivers_idempotency"),
        sa.CheckConstraint("artifact_sha256 ~ '^[a-f0-9]{64}$'", name="specification_evaluation_waivers_valid_digest"),
    )


def downgrade() -> None:
    raise RuntimeError("Cogito supervisor migrations are forward-only")
