from __future__ import annotations

import json
import io
import sys
import types
from contextlib import redirect_stdout

from scripts import pm


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


def test_pm_report_requires_preview_flag():
    try:
        pm.main(["report", "daily", "--json"])
    except SystemExit as exc:
        assert "preview-only" in str(exc)
    else:
        raise AssertionError("expected SystemExit")


def test_pm_report_preview_marks_noncanonical_output():
    fake_skill_api = types.SimpleNamespace(
        generate_report=lambda **kwargs: {
            "success": True,
            "report_type": kwargs["report_type"],
            "account": kwargs["account"],
        }
    )
    stdout = io.StringIO()
    with _SysModulesPatch("skill_api", fake_skill_api), redirect_stdout(stdout):
        assert pm.main(["report", "daily", "--preview", "--account", "alice", "--json"]) == 0

    out = json.loads(stdout.getvalue())
    assert out["success"] is True
    assert out["report_type"] == "daily"
    assert out["account"] == "alice"
    assert out["preview_only"] is True
    assert out["canonical_entrypoint"] == "scripts/publish_daily_report.py"


def test_pm_cash_passes_account():
    fake_skill_api = types.SimpleNamespace(
        get_cash=lambda **kwargs: {
            "success": True,
            "account": kwargs["account"],
        }
    )
    stdout = io.StringIO()
    with _SysModulesPatch("skill_api", fake_skill_api), redirect_stdout(stdout):
        assert pm.main(["cash", "--account", "bob", "--json"]) == 0

    out = json.loads(stdout.getvalue())
    assert out["success"] is True
    assert out["account"] == "bob"


def test_pm_init_nav_passes_account_and_write_flags():
    fake_skill_api = types.SimpleNamespace(
        init_nav_history=lambda **kwargs: {
            "success": True,
            "account": kwargs["account"],
            "date": kwargs["date_str"],
            "dry_run": kwargs["dry_run"],
            "confirm": kwargs["confirm"],
        }
    )
    stdout = io.StringIO()
    with _SysModulesPatch("skill_api", fake_skill_api), redirect_stdout(stdout):
        assert pm.main([
            "init-nav",
            "--account", "sy",
            "--date", "2026-04-20",
            "--write",
            "--confirm",
            "--json",
        ]) == 0

    out = json.loads(stdout.getvalue())
    assert out["success"] is True
    assert out["account"] == "sy"
    assert out["date"] == "2026-04-20"
    assert out["dry_run"] is False
    assert out["confirm"] is True


def test_pm_init_nav_write_requires_confirm():
    try:
        pm.main(["init-nav", "--account", "hb", "--write"])
    except SystemExit as exc:
        assert "--confirm" in str(exc)
    else:
        raise AssertionError("expected SystemExit")
