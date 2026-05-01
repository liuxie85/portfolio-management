from __future__ import annotations

from types import SimpleNamespace

from src.service import PortfolioService


def test_portfolio_service_delegates_read_use_cases_to_backend():
    calls = []

    backend = SimpleNamespace(
        list_accounts=lambda **kwargs: calls.append(("list_accounts", kwargs)) or {"success": True},
        multi_account_overview=lambda **kwargs: calls.append(("overview", kwargs)) or {"success": True},
        get_holdings=lambda **kwargs: calls.append(("holdings", kwargs)) or {"success": True},
        get_cash=lambda **kwargs: calls.append(("cash", kwargs)) or {"success": True},
        get_nav=lambda **kwargs: calls.append(("nav", kwargs)) or {"success": True},
        full_report=lambda **kwargs: calls.append(("full_report", kwargs)) or {"success": True},
        generate_report=lambda **kwargs: calls.append(("generate_report", kwargs)) or {"success": True},
    )

    service = PortfolioService(backend=backend)

    assert service.health()["status"] == "ok"
    service.list_accounts(include_default=False)
    service.multi_account_overview(accounts="alice,bob", price_timeout=5, include_details=True)
    service.get_holdings(account="alice", include_cash=False, group_by_market=True, include_price=True)
    service.get_cash(account="alice")
    service.get_nav(account="alice", days=7)
    service.full_report(account="alice", price_timeout=9)
    service.generate_report(account="alice", report_type="monthly", price_timeout=11)

    assert calls == [
        ("list_accounts", {"include_default": False}),
        ("overview", {"accounts": "alice,bob", "price_timeout": 5, "include_details": True}),
        ("holdings", {"account": "alice", "include_cash": False, "group_by_market": True, "include_price": True}),
        ("cash", {"account": "alice"}),
        ("nav", {"account": "alice", "days": 7}),
        ("full_report", {"account": "alice", "price_timeout": 9}),
        ("generate_report", {"account": "alice", "report_type": "monthly", "record_nav": False, "price_timeout": 11}),
    ]
