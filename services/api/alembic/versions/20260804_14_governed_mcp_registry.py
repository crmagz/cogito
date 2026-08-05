"""Persist immutable MCP policy and per-run tool resolutions.

Revision ID: 20260804_14
Revises: 20260802_13
Create Date: 2026-08-04
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "20260804_14"
down_revision = "20260802_13"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Add MCP authorization without changing historical registration pins."""

    op.drop_constraint("registry_registrations_valid_kind", "registry_registrations", type_="check")
    op.create_check_constraint(
        "registry_registrations_valid_kind",
        "registry_registrations",
        "kind IN ('agent', 'tool', 'mcp_server')",
    )
    op.add_column(
        "registry_policy_revisions",
        sa.Column(
            "mcp_bindings",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'[]'::jsonb"),
        ),
    )
    op.create_table(
        "run_mcp_tool_resolutions",
        sa.Column("run_id", sa.String(length=36), nullable=False),
        sa.Column("role", sa.String(length=128), nullable=False),
        sa.Column("server_registration_id", sa.String(length=128), nullable=False),
        sa.Column("server_version", sa.String(length=32), nullable=False),
        sa.Column("server_manifest_sha256", sa.String(length=64), nullable=False),
        sa.Column("tool_name", sa.String(length=128), nullable=False),
        sa.Column("input_schema_sha256", sa.String(length=64), nullable=False),
        sa.Column("policy_revision", sa.String(length=64), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["run_id"], ["agent_runs.run_id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(
            ["server_registration_id", "server_version"],
            ["registry_registrations.registration_id", "registry_registrations.version"],
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(["policy_revision"], ["registry_policy_revisions.policy_revision"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint(
            "run_id", "role", "server_registration_id", "server_version", "tool_name",
            name="run_mcp_tool_resolutions_pkey",
        ),
        sa.CheckConstraint(
            "server_manifest_sha256 ~ '^[0-9a-f]{64}$'",
            name="run_mcp_tool_resolutions_valid_server_digest",
        ),
        sa.CheckConstraint(
            "input_schema_sha256 ~ '^[0-9a-f]{64}$'",
            name="run_mcp_tool_resolutions_valid_schema_digest",
        ),
    )
    op.create_index(
        "run_mcp_tool_resolutions_run_role",
        "run_mcp_tool_resolutions",
        ["run_id", "role"],
    )


def downgrade() -> None:
    """Supervisor migrations are intentionally forward-only."""

    raise RuntimeError("Cogito supervisor migrations are forward-only")
