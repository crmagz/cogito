"""Permit identical plan content in distinct immutable revisions.

Revision ID: 20260719_07
Revises: 20260719_06
Create Date: 2026-07-19
"""

from __future__ import annotations

from alembic import op

revision = "20260719_07"
down_revision = "20260719_06"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Identify a run artifact by its immutable object reference, not only its digest."""

    op.drop_constraint("supervisor_artifacts_identity", "supervisor_artifacts", type_="unique")
    op.create_unique_constraint(
        "supervisor_artifacts_reference_identity",
        "supervisor_artifacts",
        ["run_id", "artifact_type", "ref"],
    )


def downgrade() -> None:
    """Refuse destructive schema rollback; restore through a compatible application release."""

    raise RuntimeError("Cogito supervisor migrations are forward-only and cannot be downgraded")
