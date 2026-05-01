from __future__ import annotations

from types import SimpleNamespace

import skill_api


class FakeClient:
    def __init__(self):
        self.records = {
            "transactions": [{"record_id": "tx1", "fields": {"account": "bob"}}],
            "cash_flow": [{"record_id": "cf1", "fields": {"account": [{"text": "carol"}]}}],
            "nav_history": [{"record_id": "nav1", "fields": {"account": {"value": "dave"}}}],
        }

    def list_records(self, table, **_kwargs):
        return list(self.records.get(table) or [])


class FakeStorage:
    def __init__(self):
        self.client = FakeClient()

    def get_holdings(self, account=None, include_empty=False):
        assert account is None
        assert include_empty is True
        return [
            SimpleNamespace(account="alice"),
            SimpleNamespace(account="bob"),
            SimpleNamespace(account=""),
        ]

    def _from_feishu_fields(self, fields, _table):
        return dict(fields)


def test_list_accounts_discovers_accounts_across_read_models():
    skill = skill_api.PortfolioSkill.__new__(skill_api.PortfolioSkill)
    skill.account = "default"
    skill.storage = FakeStorage()

    result = skill.list_accounts()

    assert result["success"] is True
    assert result["default_account"] == "default"
    assert result["accounts"] == ["alice", "bob", "carol", "dave", "default"]
    assert result["sources"]["holdings"] == ["alice", "bob"]
    assert result["sources"]["cash_flow"] == ["carol"]


def test_multi_account_overview_aggregates_successful_accounts():
    class FakeSkill:
        def __init__(self, account):
            self.account = account

        def full_report(self, price_timeout=30):
            values = {
                "alice": {"total_value": 100, "cash_ratio": 0.2, "stock_ratio": 0.7, "fund_ratio": 0.1},
                "bob": {"total_value": 200, "cash_ratio": 0.5, "stock_ratio": 0.25, "fund_ratio": 0.25},
            }
            return {
                "success": True,
                "overview": values[self.account],
                "nav": {"nav": 1.0},
                "returns": {"monthly": 0.01},
            }

    old_get_skill = skill_api.get_skill
    try:
        skill_api.get_skill = lambda account=None: FakeSkill(account)
        result = skill_api.multi_account_overview(accounts=["alice", "bob"], price_timeout=5)
    finally:
        skill_api.get_skill = old_get_skill

    assert result["success"] is True
    assert result["status"] == "ok"
    assert result["accounts"] == ["alice", "bob"]
    assert result["successful_count"] == 2
    assert result["summary"]["total_value"] == 300
    assert result["summary"]["cash_value"] == 120
    assert result["summary"]["stock_value"] == 120
    assert result["summary"]["fund_value"] == 60
