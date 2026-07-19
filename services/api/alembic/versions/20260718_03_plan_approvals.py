"""Create immutable human approvals and transactional Temporal delivery records.

Revision ID: 20260718_03
Revises: 20260718_02
Create Date: 2026-07-18
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "20260718_03"
down_revision = "20260718_02"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Create idempotent decision records before sending Temporal updates."""

    op.create_table(
        "plan_approval_decisions",
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
        sa.ForeignKeyConstraint(["run_id"], ["supervisor_runs.run_id"], ondelete="RESTRICT"),
        sa.UniqueConstraint("run_id", "idempotency_key", name="plan_approval_idempotency"),
        sa.CheckConstraint(
            "decision IN ('approve', 'reject', 'request_revision')", name="plan_approval_valid_decision"
        ),
    )
    op.create_table(
        "temporal_outbox",
        sa.Column("decision_id", sa.String(length=36), primary_key=True),
        sa.Column("run_id", sa.String(length=36), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("delivered_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(
            ["decision_id"], ["plan_approval_decisions.decision_id"], ondelete="RESTRICT"
        ),
        sa.ForeignKeyConstraint(["run_id"], ["supervisor_runs.run_id"], ondelete="RESTRICT"),
    )


def downgrade() -> None:
    """Refuse destructive schema rollback; restore through a compatible application release."""

    raise RuntimeError("Cogito supervisor migrations are forward-only and cannot be downgraded")
