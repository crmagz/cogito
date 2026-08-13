from pathlib import Path

from alembic.config import Config
from alembic.script import ScriptDirectory


def test_migration_chain_includes_supported_kind_baseline() -> None:
    """A chart upgrade must recognize the database revision used by Kind."""

    api_root = Path(__file__).parents[1]
    config = Config(str(api_root / "alembic.ini"))
    config.set_main_option("script_location", str(api_root / "alembic"))

    assert ScriptDirectory.from_config(config).get_current_head() == "20260812_19"


def test_workbench_feedback_migration_matches_supervisor_actor_identifier_limit() -> None:
    """OIDC subjects need the same storage bound as existing approval actors."""

    migration = (Path(__file__).parents[1] / "alembic/versions/20260802_13_workbench_feedback.py").read_text()

    assert 'sa.Column("actor_id", sa.String(length=512), nullable=False)' in migration
