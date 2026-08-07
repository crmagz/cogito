"""Persist immutable agent gateway policy and per-run route resolutions.

Revision ID: 20260806_16
Revises: 20260806_15
Create Date: 2026-08-06
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "20260806_16"
down_revision = "20260806_15"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Add non-secret agent route policy without changing existing run pins."""

    op.create_table(
        "registry_agent_gateway_policy_revisions",
        sa.Column("policy_revision", sa.String(length=64), primary_key=True),
        sa.Column("bindings", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "policy_revision ~ '^[a-z][a-z0-9_-]{0,63}$'",
            name="registry_agent_gateway_policy_valid_revision",
        ),
    )
    op.create_table(
        "run_agent_gateway_resolutions",
        sa.Column("run_id", sa.String(length=36), nullable=False),
        sa.Column("role", sa.String(length=128), nullable=False),
        sa.Column("project_id", sa.String(length=128), nullable=False),
        sa.Column("registration_id", sa.String(length=128), nullable=False),
        sa.Column("registration_version", sa.String(length=32), nullable=False),
        sa.Column("manifest_sha256", sa.String(length=64), nullable=False),
        sa.Column("policy_revision", sa.String(length=64), nullable=False),
        sa.Column("model_alias", sa.String(length=128), nullable=False),
        sa.Column("max_budget_usd", sa.Numeric(precision=12, scale=4), nullable=False),
        sa.Column("toolset", sa.String(length=128), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["run_id"], ["agent_runs.run_id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(
            ["registration_id", "registration_version"],
            ["registry_registrations.registration_id", "registry_registrations.version"],
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["policy_revision"],
            ["registry_agent_gateway_policy_revisions.policy_revision"],
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("run_id", "role", name="run_agent_gateway_resolutions_pkey"),
        sa.CheckConstraint(
            "manifest_sha256 ~ '^[0-9a-f]{64}$'",
            name="run_agent_gateway_resolutions_valid_digest",
        ),
        sa.CheckConstraint("max_budget_usd > 0", name="run_agent_gateway_resolutions_positive_budget"),
    )
    op.create_index(
        "run_agent_gateway_resolutions_registration",
        "run_agent_gateway_resolutions",
        ["registration_id", "registration_version"],
    )


def downgrade() -> None:
    """Supervisor migrations are intentionally forward-only."""

    raise RuntimeError("Cogito supervisor migrations are forward-only and cannot be downgraded")
