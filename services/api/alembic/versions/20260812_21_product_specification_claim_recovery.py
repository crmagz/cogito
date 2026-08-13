"""Recover claims and retain each plan's selected product specification.

Revision ID: 20260812_21
Revises: 20260812_20
Create Date: 2026-08-12
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260812_21"
down_revision = "20260812_20"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Permit exact reverts and preserve the immutable plan-input association."""

    op.add_column(
        "supervisor_runs",
        sa.Column("product_specification_generation_claimed_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.drop_constraint("product_specification_revisions_identity", "product_specification_revisions", type_="unique")
    op.create_table(
        "plan_product_specification_bindings",
        sa.Column("run_id", sa.String(length=36), nullable=False),
        sa.Column("plan_revision", sa.Integer(), nullable=False),
        sa.Column("product_specification_revision", sa.Integer(), nullable=False),
        sa.Column("artifact_ref", sa.Text(), nullable=False),
        sa.Column("artifact_sha256", sa.String(length=64), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["run_id"], ["supervisor_runs.run_id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("run_id", "plan_revision", name="plan_product_specification_bindings_pkey"),
        sa.CheckConstraint("plan_revision > 0", name="plan_product_specification_binding_positive_plan_revision"),
        sa.CheckConstraint(
            "product_specification_revision > 0",
            name="plan_product_specification_binding_positive_spec_revision",
        ),
        sa.CheckConstraint(
            "artifact_sha256 ~ '^[a-f0-9]{64}$'",
            name="plan_product_specification_binding_valid_digest",
        ),
    )


def downgrade() -> None:
    """Supervisor migrations are intentionally forward-only."""

    raise RuntimeError("Cogito supervisor migrations are forward-only")
