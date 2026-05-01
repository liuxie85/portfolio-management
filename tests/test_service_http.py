from __future__ import annotations

from fastapi.testclient import TestClient

from src.service.http import create_app


class FakePortfolioService:
    def __init__(self):
        self.calls = []

    def health(self):
        self.calls.append(("health", {}))
        return {"success": True, "status": "ok"}

    def list_accounts(self, **kwargs):
        self.calls.append(("list_accounts", kwargs))
        return {"success": True, "accounts": ["alice"]}

    def multi_account_overview(self, **kwargs):
        self.calls.append(("overview", kwargs))
        return {"success": True, "accounts": kwargs["accounts"]}

    def get_holdings(self, **kwargs):
        self.calls.append(("holdings", kwargs))
        return {"success": True, "account": kwargs["account"]}

    def get_cash(self, **kwargs):
        self.calls.append(("cash", kwargs))
        return {"success": True, "account": kwargs["account"]}

    def get_nav(self, **kwargs):
        self.calls.append(("nav", kwargs))
        return {"success": True, "days": kwargs["days"]}

    def full_report(self, **kwargs):
        self.calls.append(("full_report", kwargs))
        return {"success": True, "account": kwargs["account"]}

    def generate_report(self, **kwargs):
        self.calls.append(("generate_report", kwargs))
        return {"success": True, "report_type": kwargs["report_type"]}


def test_http_service_routes_delegate_to_portfolio_service():
    service = FakePortfolioService()
    client = TestClient(create_app(service=service))

    assert client.get("/health").json()["status"] == "ok"
    assert client.get("/accounts", params={"include_default": False}).json()["accounts"] == ["alice"]
    assert client.get("/accounts/overview", params={"accounts": "alice,bob", "price_timeout": 7}).json()["accounts"] == "alice,bob"
    assert client.get("/holdings", params={"account": "alice/bob", "include_cash": False, "group_by_market": True, "include_price": True}).json()["account"] == "alice/bob"
    assert client.get("/cash", params={"account": "alice/bob"}).json()["account"] == "alice/bob"
    assert client.get("/nav", params={"account": "alice/bob", "days": 14}).json()["days"] == 14
    assert client.get("/report/full", params={"account": "alice/bob", "price_timeout": 9}).json()["account"] == "alice/bob"
    assert client.get("/report/monthly", params={"account": "alice/bob", "price_timeout": 11}).json()["report_type"] == "monthly"
    assert client.get("/accounts/alice/holdings", params={"include_cash": False, "group_by_market": True, "include_price": True}).json()["account"] == "alice"
    assert client.get("/accounts/alice/cash").json()["account"] == "alice"
    assert client.get("/accounts/alice/nav", params={"days": 14}).json()["days"] == 14
    assert client.get("/accounts/alice/report/full", params={"price_timeout": 9}).json()["account"] == "alice"
    assert client.get("/accounts/alice/report/monthly", params={"price_timeout": 11}).json()["report_type"] == "monthly"

    assert service.calls == [
        ("health", {}),
        ("list_accounts", {"include_default": False}),
        ("overview", {"accounts": "alice,bob", "price_timeout": 7, "include_details": False}),
        ("holdings", {"account": "alice/bob", "include_cash": False, "group_by_market": True, "include_price": True}),
        ("cash", {"account": "alice/bob"}),
        ("nav", {"account": "alice/bob", "days": 14}),
        ("full_report", {"account": "alice/bob", "price_timeout": 9}),
        ("generate_report", {"account": "alice/bob", "report_type": "monthly", "price_timeout": 11}),
        ("holdings", {"account": "alice", "include_cash": False, "group_by_market": True, "include_price": True}),
        ("cash", {"account": "alice"}),
        ("nav", {"account": "alice", "days": 14}),
        ("full_report", {"account": "alice", "price_timeout": 9}),
        ("generate_report", {"account": "alice", "report_type": "monthly", "price_timeout": 11}),
    ]


def test_http_service_rejects_unknown_report_type():
    client = TestClient(create_app(service=FakePortfolioService()))

    response = client.get("/accounts/alice/report/weekly")
    query_response = client.get("/report/weekly", params={"account": "alice"})

    assert response.status_code == 400
    assert "unsupported report_type=weekly" in response.json()["detail"]
    assert query_response.status_code == 400
    assert "unsupported report_type=weekly" in query_response.json()["detail"]
