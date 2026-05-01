from __future__ import annotations

import ast
from pathlib import Path
import sys
import types

from scripts import publish_daily_report


REPO_ROOT = Path(__file__).resolve().parents[1]


def _module_ast(path: str) -> ast.Module:
    return ast.parse((REPO_ROOT / path).read_text(encoding="utf-8"))


class _SysModulesPatch:
    def __init__(self, name, value):
        self.name = name
        self.value = value
        self.old = None
        self.had_old = False

    def __enter__(self):
        self.had_old = self.name in sys.modules
        self.old = sys.modules.get(self.name)
        sys.modules[self.name] = self.value
        return self

    def __exit__(self, exc_type, exc, tb):
        if self.had_old:
            sys.modules[self.name] = self.old
        else:
            sys.modules.pop(self.name, None)


def test_generate_daily_report_html_is_renderer_only():
    tree = _module_ast("scripts/generate_daily_report_html.py")

    imported_names = set()
    forbidden_calls = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            imported_names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.Import):
            imported_names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.Call):
            func = node.func
            if isinstance(func, ast.Name):
                forbidden_calls.add(func.id)
            elif isinstance(func, ast.Attribute):
                forbidden_calls.add(func.attr)

    assert "PortfolioSkill" not in imported_names
    assert "build_snapshot" not in forbidden_calls
    assert "generate_report" not in forbidden_calls
    assert "full_report" not in forbidden_calls
    assert "get_nav_history" not in forbidden_calls


def test_publish_daily_report_returns_renderer_bundle_shape():
    tree = _module_ast("scripts/publish_daily_report.py")
    build_report = next(
        node for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == "build_report_data"
    )
    return_dicts = [node.value for node in ast.walk(build_report) if isinstance(node, ast.Return) and isinstance(node.value, ast.Dict)]
    keys = {
        key.value
        for ret in return_dicts
        for key in ret.keys
        if isinstance(key, ast.Constant)
    }

    assert {"snapshot", "report", "nav_result", "nav_snapshot"}.issubset(keys)


def test_publish_daily_report_build_report_data_passes_account():
    calls = []

    class FakeStorage:
        def get_nav_history(self, account, days):
            calls.append(("get_nav_history", account, days))
            return [{"date": "2026-04-20", "nav": 1.0}]

    class FakeSkill:
        def __init__(self, account):
            self.account = account
            self.storage = FakeStorage()

        def build_snapshot(self):
            calls.append(("build_snapshot", self.account))
            return {"valuation": None}

        def record_nav(self, **kwargs):
            calls.append(("record_nav", self.account, kwargs["dry_run"]))
            return {"success": True}

        def generate_report(self, **kwargs):
            calls.append(("generate_report", self.account, kwargs["report_type"]))
            return {"success": True, "date": "2026-04-20"}

        def get_nav(self, **kwargs):
            calls.append(("get_nav", self.account, kwargs["days"]))
            return {"success": True}

    fake_skill_api = types.SimpleNamespace(
        get_skill=lambda account=None: FakeSkill(account or "default")
    )
    with _SysModulesPatch("skill_api", fake_skill_api):
        bundle = publish_daily_report.build_report_data(
            price_timeout=5,
            dry_run=True,
            account="alice",
        )

    assert bundle["account"] == "alice"
    assert ("build_snapshot", "alice") in calls
    assert ("record_nav", "alice", True) in calls
    assert ("generate_report", "alice", "daily") in calls
    assert ("get_nav", "alice", 2) in calls


def test_publish_daily_report_futu_sync_defaults_to_dry_run():
    calls = []

    class FakeStorage:
        def get_nav_history(self, account, days):
            return []

    class FakeSkill:
        def __init__(self, account):
            self.account = account
            self.storage = FakeStorage()

        def sync_futu_cash_mmf(self, dry_run):
            calls.append(("sync_futu_cash_mmf", dry_run))
            return {"success": True, "dry_run": dry_run}

        def build_snapshot(self):
            return {"valuation": None}

        def record_nav(self, **kwargs):
            return {"success": True}

        def generate_report(self, **kwargs):
            return {"success": True, "date": "2026-04-20"}

        def get_nav(self, **kwargs):
            return {"success": True}

    fake_skill_api = types.SimpleNamespace(
        get_skill=lambda account=None: FakeSkill(account or "default")
    )
    with _SysModulesPatch("skill_api", fake_skill_api):
        bundle = publish_daily_report.build_report_data(
            price_timeout=5,
            dry_run=True,
            sync_futu_cash_mmf=True,
            account="alice",
        )

    assert bundle["futu_sync_result"] == {"success": True, "dry_run": True}
    assert calls == [("sync_futu_cash_mmf", True)]
