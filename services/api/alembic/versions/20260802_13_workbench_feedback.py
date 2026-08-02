"""Add append-only, digest-bound product-owner feedback.

Revision ID: 20260802_13
Revises: 20260726_12
Create Date: 2026-08-02
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "20260802_13"
down_revision = "20260726_12"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Persist idempotent, non-executable notes without mutating run state."""

    op.create_table(
        "workbench_feedback",
        sa.Column("feedback_id", sa.String(length=36), primary_key=True),
        sa.Column("run_id", sa.String(length=36), nullable=False),
        sa.Column("intent", sa.String(length=32), nullable=False),
        sa.Column("artifact_sha256", sa.String(length=64), nullable=False),
        sa.Column("stage_id", sa.String(length=128), nullable=False),
        sa.Column("actor_id", sa.String(length=256), nullable=False),
        sa.Column("comment", sa.Text(), nullable=False),
        sa.Column("idempotency_key", sa.String(length=256), nullable=False),
        sa.Column("request_sha256", sa.String(length=64), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["run_id"], ["supervisor_runs.run_id"], ondelete="RESTRICT"),
        sa.CheckConstraint("intent = 'note'", name="workbench_feedback_valid_intent"),
        sa.CheckConstraint("artifact_sha256 ~ '^[a-f0-9]{64}$'", name="workbench_feedback_valid_digest"),
        sa.UniqueConstraint("run_id", "idempotency_key", name="workbench_feedback_run_idempotency"),
    )
    op.create_index("workbench_feedback_run_order", "workbench_feedback", ["run_id", "created_at"])


def downgrade() -> None:
    """Supervisor migrations are intentionally forward-only."""

    raise RuntimeError("Cogito supervisor migrations are forward-only")
