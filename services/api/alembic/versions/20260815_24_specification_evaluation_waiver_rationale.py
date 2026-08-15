"""Reject blank persisted specification-evaluation waiver rationales.

Revision ID: 20260815_24
Revises: 20260815_23
Create Date: 2026-08-15
"""

from __future__ import annotations

from alembic import op

revision = "20260815_24"
down_revision = "20260815_23"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Revision 23 accepted whitespace-only rationale values. Preserve those
    # historical decisions while making their missing audit detail explicit
    # before validating the new invariant.
    op.execute(
        """UPDATE specification_evaluation_waivers
           SET rationale = '[legacy waiver rationale unavailable]'
           WHERE btrim(rationale) = ''"""
    )
    op.create_check_constraint(
        "specification_evaluation_waivers_nonblank_rationale",
        "specification_evaluation_waivers",
        "btrim(rationale) <> ''",
    )


def downgrade() -> None:
    raise RuntimeError("Cogito supervisor migrations are forward-only")
