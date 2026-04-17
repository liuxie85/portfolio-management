from src.migrations import MigrationRunner
from src.migrations.feishu import get_migrations
from src.migrations.schema_state import SchemaStateStore


def test_migration_runner_plans_pending_migrations(tmp_path):
    runner = MigrationRunner(get_migrations(), state_store=SchemaStateStore(tmp_path / "state.json"))

    plan = runner.plan()

    assert plan["pending_count"] >= 3
    assert plan["pending"][0]["id"] == "0001_baseline"


def test_migration_runner_marks_migrations_applied(tmp_path):
    state = SchemaStateStore(tmp_path / "state.json")
    runner = MigrationRunner(get_migrations(), state_store=state)

    result = runner.apply()

    assert result["success"] is True
    assert runner.pending() == []
