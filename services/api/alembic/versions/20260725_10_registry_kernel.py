"""Create immutable registry releases, grants, policies, and run resolutions.

Revision ID: 20260725_10
Revises: 20260725_09
Create Date: 2026-07-25
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "20260725_10"
down_revision = "20260725_09"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Add forward-only, immutable registry records without changing run contracts."""

    op.create_table(
        "registry_registrations",
        sa.Column("registration_id", sa.String(length=128), nullable=False),
        sa.Column("version", sa.String(length=32), nullable=False),
        sa.Column("kind", sa.String(length=16), nullable=False),
        sa.Column("component_id", sa.String(length=128), nullable=False),
        sa.Column("component_version", sa.String(length=32), nullable=False),
        sa.Column("lifecycle", sa.String(length=16), nullable=False),
        sa.Column("maturity", sa.String(length=16), nullable=False),
        sa.Column("execution_class", sa.String(length=32), nullable=False),
        sa.Column("owner", sa.String(length=256), nullable=False),
        sa.Column("input_schema_id", sa.String(length=128), nullable=False),
        sa.Column("input_schema_version", sa.String(length=32), nullable=False),
        sa.Column("output_schema_id", sa.String(length=128), nullable=False),
        sa.Column("output_schema_version", sa.String(length=32), nullable=False),
        sa.Column("manifest", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("manifest_sha256", sa.String(length=64), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("registration_id", "version", name="registry_registrations_pkey"),
        sa.UniqueConstraint("component_id", "component_version", name="registry_registrations_component_release"),
        sa.UniqueConstraint("manifest_sha256", name="registry_registrations_manifest_sha256"),
        sa.CheckConstraint("kind IN ('agent', 'tool')", name="registry_registrations_valid_kind"),
        sa.CheckConstraint(
            "lifecycle IN ('draft', 'active', 'disabled', 'revoked')",
            name="registry_registrations_valid_lifecycle",
        ),
        sa.CheckConstraint(
            "maturity IN ('incubating', 'active', 'deprecated', 'retired')",
            name="registry_registrations_valid_maturity",
        ),
        sa.CheckConstraint(
            "execution_class IN ('adapter', 'worker_service', 'isolated_job')",
            name="registry_registrations_valid_execution_class",
        ),
        sa.CheckConstraint("manifest_sha256 ~ '^[0-9a-f]{64}$'", name="registry_registrations_valid_digest"),
    )
    op.create_table(
        "registry_grants",
        sa.Column("agent_registration_id", sa.String(length=128), nullable=False),
        sa.Column("agent_version", sa.String(length=32), nullable=False),
        sa.Column("tool_registration_id", sa.String(length=128), nullable=False),
        sa.Column("tool_version", sa.String(length=32), nullable=False),
        sa.Column("scope", sa.String(length=256), nullable=False),
        sa.ForeignKeyConstraint(
            ["agent_registration_id", "agent_version"],
            ["registry_registrations.registration_id", "registry_registrations.version"],
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["tool_registration_id", "tool_version"],
            ["registry_registrations.registration_id", "registry_registrations.version"],
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint(
            "agent_registration_id",
            "agent_version",
            "tool_registration_id",
            "tool_version",
            "scope",
            name="registry_grants_pkey",
        ),
    )
    op.create_table(
        "registry_policy_revisions",
        sa.Column("policy_revision", sa.String(length=64), primary_key=True),
        sa.Column("assignments", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint("policy_revision ~ '^[a-z][a-z0-9_-]{0,63}$'", name="registry_policy_valid_revision"),
    )
    op.create_table(
        "run_registration_resolutions",
        sa.Column("run_id", sa.String(length=36), nullable=False),
        sa.Column("role", sa.String(length=128), nullable=False),
        sa.Column("registration_id", sa.String(length=128), nullable=False),
        sa.Column("registration_version", sa.String(length=32), nullable=False),
        sa.Column("manifest_sha256", sa.String(length=64), nullable=False),
        sa.Column("component_id", sa.String(length=128), nullable=False),
        sa.Column("component_version", sa.String(length=32), nullable=False),
        sa.Column("policy_revision", sa.String(length=64), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["run_id"], ["agent_runs.run_id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(
            ["registration_id", "registration_version"],
            ["registry_registrations.registration_id", "registry_registrations.version"],
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(["policy_revision"], ["registry_policy_revisions.policy_revision"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("run_id", "role", name="run_registration_resolutions_pkey"),
        sa.CheckConstraint("manifest_sha256 ~ '^[0-9a-f]{64}$'", name="run_registration_resolutions_valid_digest"),
    )
    op.create_index("run_registration_resolutions_registration", "run_registration_resolutions", ["registration_id", "registration_version"])


def downgrade() -> None:
    """Supervisor migrations are intentionally forward-only."""

    raise RuntimeError("Cogito supervisor migrations are forward-only and cannot be downgraded")
