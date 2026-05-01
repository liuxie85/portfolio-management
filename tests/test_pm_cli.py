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
        assert pm.main(["report", "daily", "--preview", "--account", "alice", "--no-service", "--json"]) == 0

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
        assert pm.main(["cash", "--account", "bob", "--no-service", "--json"]) == 0

    out = json.loads(stdout.getvalue())
    assert out["success"] is True
    assert out["account"] == "bob"


def test_pm_accounts_lists_discovered_accounts():
    fake_skill_api = types.SimpleNamespace(
        list_accounts=lambda **kwargs: {
            "success": True,
            "include_default": kwargs["include_default"],
            "accounts": ["alice"],
        }
    )
    stdout = io.StringIO()
    with _SysModulesPatch("skill_api", fake_skill_api), redirect_stdout(stdout):
        assert pm.main(["accounts", "--exclude-default", "--no-service", "--json"]) == 0

    out = json.loads(stdout.getvalue())
    assert out["success"] is True
    assert out["include_default"] is False
    assert out["accounts"] == ["alice"]


def test_pm_overview_passes_accounts_and_timeout():
    fake_skill_api = types.SimpleNamespace(
        multi_account_overview=lambda **kwargs: {
            "success": True,
            "accounts": kwargs["accounts"],
            "price_timeout": kwargs["price_timeout"],
            "include_details": kwargs["include_details"],
        }
    )
    stdout = io.StringIO()
    with _SysModulesPatch("skill_api", fake_skill_api), redirect_stdout(stdout):
        assert pm.main(["overview", "--accounts", "alice,bob", "--timeout", "7", "--details", "--no-service", "--json"]) == 0

    out = json.loads(stdout.getvalue())
    assert out["success"] is True
    assert out["accounts"] == "alice,bob"
    assert out["price_timeout"] == 7
    assert out["include_details"] is True


def test_pm_cash_prefers_service_when_available():
    import src.service.client as client_module

    calls = []

    class FakeClient:
        def __init__(self, base_url=None, timeout=0.5):
            calls.append(("init", base_url, timeout))

        def get_cash(self, *, account):
            calls.append(("get_cash", account))
            return {"success": True, "account": account, "source": "service"}

    old_client = client_module.PortfolioServiceClient
    try:
        client_module.PortfolioServiceClient = FakeClient
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            assert pm.main(["cash", "--account", "bob", "--service-url", "http://local", "--service-timeout", "1", "--json"]) == 0
    finally:
        client_module.PortfolioServiceClient = old_client

    out = json.loads(stdout.getvalue())
    assert out["source"] == "service"
    assert out["account"] == "bob"
    assert calls == [("init", "http://local", 1.0), ("get_cash", "bob")]


def test_pm_service_response_error_does_not_fallback():
    import src.service.client as client_module
    from src.service.client import PortfolioServiceResponseError

    calls = []

    class FakeClient:
        def __init__(self, base_url=None, timeout=0.5):
            calls.append(("init", base_url, timeout))

        def get_cash(self, *, account):
            calls.append(("get_cash", account))
            raise PortfolioServiceResponseError("bad service payload")

    fake_skill_api = types.SimpleNamespace(
        get_cash=lambda **_kwargs: calls.append(("fallback", None))
    )
    old_client = client_module.PortfolioServiceClient
    try:
        client_module.PortfolioServiceClient = FakeClient
        with _SysModulesPatch("skill_api", fake_skill_api):
            try:
                pm.main(["cash", "--account", "bob", "--service-url", "http://local", "--json"])
            except SystemExit as exc:
                assert "bad service payload" in str(exc)
            else:
                raise AssertionError("expected SystemExit")
    finally:
        client_module.PortfolioServiceClient = old_client

    assert calls == [("init", "http://local", 0.5), ("get_cash", "bob")]


def test_pm_require_service_fails_instead_of_fallback():
    import src.service.client as client_module
    from src.service.client import PortfolioServiceUnavailable

    calls = []

    class FakeClient:
        def __init__(self, base_url=None, timeout=0.5):
            calls.append(("init", base_url, timeout))

        def get_cash(self, *, account):
            calls.append(("get_cash", account))
            raise PortfolioServiceUnavailable("down")

    fake_skill_api = types.SimpleNamespace(
        get_cash=lambda **_kwargs: calls.append(("fallback", None))
    )
    old_client = client_module.PortfolioServiceClient
    try:
        client_module.PortfolioServiceClient = FakeClient
        with _SysModulesPatch("skill_api", fake_skill_api):
            try:
                pm.main(["cash", "--account", "bob", "--service-url", "http://local", "--require-service", "--json"])
            except SystemExit as exc:
                assert "--require-service" in str(exc)
            else:
                raise AssertionError("expected SystemExit")
    finally:
        client_module.PortfolioServiceClient = old_client

    assert calls == [("init", "http://local", 0.5), ("get_cash", "bob")]


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
