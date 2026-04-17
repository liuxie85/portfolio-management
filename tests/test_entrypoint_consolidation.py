from __future__ import annotations

from scripts import nav_history_repair
from scripts.migrate_schema import schema_expectations


def test_nav_history_repair_forwards_backfill_args(monkeypatch):
    captured = {}

    def fake_main(argv=None):
        captured["argv"] = argv

    import scripts.backfill_nav_history_bulk as backfill

    monkeypatch.setattr(backfill, "main", fake_main)

    assert nav_history_repair.main(["backfill", "--account", "lx", "--from", "2025-01-01", "--to", "2025-01-02", "--dry-run"]) == 0
    assert captured["argv"] == [
        "--account", "lx",
        "--from", "2025-01-01",
        "--to", "2025-01-02",
        "--mode", "replace",
        "--dry-run",
    ]


def test_nav_history_repair_forwards_patch_args(monkeypatch):
    captured = {}

    def fake_main(argv=None):
        captured["argv"] = argv

    import scripts.nav_history_patch as patch

    monkeypatch.setattr(patch, "main", fake_main)

    assert nav_history_repair.main(["patch", "--patch-file", "audit/x.json", "--apply"]) == 0
    assert captured["argv"] == [
        "--patch-file", "audit/x.json",
        "--mode", "strong-consistency-gap",
        "--apply",
        "--validate-level", "basic",
        "--validate-scope", "changed",
    ]


def test_schema_expectations_are_available_from_migrate_schema():
    result = schema_expectations()

    assert result["success"] is True
    assert "holdings" in result["tables"]
    assert "nav_history" in result["tables"]
    assert "quantity" in result["tables"]["holdings"]["numeric_fields"]
