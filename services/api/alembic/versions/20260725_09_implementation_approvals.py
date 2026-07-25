"""Add immutable implementation approvals and their leased Temporal outbox.

Revision ID: 20260725_09
Revises: 20260719_08
Create Date: 2026-07-25
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "20260725_09"
down_revision = "20260719_08"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Create additive, forward-only records for the final human gate."""

    op.drop_constraint("supervisor_runs_valid_status", "supervisor_runs", type_="check")
    op.create_check_constraint(
        "supervisor_runs_valid_status",
        "supervisor_runs",
        "status IN ('planning', 'awaiting_plan_approval', 'implementing', "
        "'awaiting_implementation_approval', 'finalizing', 'completed', "
        "'planning_failed', 'rejected', 'revision_requested')",
    )
    op.drop_constraint("supervisor_artifacts_valid_type", "supervisor_artifacts", type_="check")
    op.create_check_constraint(
        "supervisor_artifacts_valid_type",
        "supervisor_artifacts",
        "artifact_type IN ('source_spec', 'plan', 'implementation_review')",
    )
    op.add_column("supervisor_runs", sa.Column("implementation_artifact_ref", sa.Text(), nullable=True))
    op.add_column("supervisor_runs", sa.Column("implementation_artifact_sha256", sa.String(length=64), nullable=True))
    op.add_column(
        "supervisor_runs",
        sa.Column("implementation_revision", sa.Integer(), nullable=False, server_default="0"),
    )
    op.create_table(
        "implementation_approval_decisions",
        sa.Column("decision_id", sa.String(length=36), primary_key=True),
        sa.Column("run_id", sa.String(length=36), nullable=False),
        sa.Column("decision", sa.String(length=32), nullable=False),
        sa.Column("artifact_sha256", sa.String(length=64), nullable=False),
        sa.Column("actor_id", sa.String(length=512), nullable=False),
        sa.Column("comment", sa.Text(), nullable=True),
        sa.Column("idempotency_key", sa.String(length=256), nullable=False),
        sa.Column("request_sha256", sa.String(length=64), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("delivered_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("implementation_revision", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(["run_id"], ["supervisor_runs.run_id"], ondelete="RESTRICT"),
        sa.UniqueConstraint(
            "run_id", "implementation_revision", "idempotency_key", name="implementation_approval_revision_idempotency"
        ),
        sa.CheckConstraint(
            "decision IN ('approve', 'reject', 'request_revision')", name="implementation_approval_valid_decision"
        ),
    )
    op.create_table(
        "implementation_temporal_outbox",
        sa.Column("decision_id", sa.String(length=36), primary_key=True),
        sa.Column("run_id", sa.String(length=36), nullable=False),
        sa.Column("workflow_id", sa.String(length=128), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("delivered_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("attempt_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("lease_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("last_error", sa.String(length=1024), nullable=True),
        sa.ForeignKeyConstraint(
            ["decision_id"], ["implementation_approval_decisions.decision_id"], ondelete="RESTRICT"
        ),
        sa.ForeignKeyConstraint(["run_id"], ["supervisor_runs.run_id"], ondelete="RESTRICT"),
    )
    op.create_table(
        "implementation_pull_requests",
        sa.Column("run_id", sa.String(length=36), primary_key=True),
        sa.Column("artifact_sha256", sa.String(length=64), nullable=False),
        sa.Column("repository", sa.String(length=256), nullable=False),
        sa.Column("number", sa.Integer(), nullable=False),
        sa.Column("url", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["run_id"], ["supervisor_runs.run_id"], ondelete="RESTRICT"),
        sa.UniqueConstraint("repository", "number", name="implementation_pull_requests_repository_number"),
    )


def downgrade() -> None:
    """Supervisor migrations are intentionally forward-only."""

    raise RuntimeError("Cogito supervisor migrations are forward-only and cannot be downgraded")
