from __future__ import annotations

import json
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace

from pytest import MonkeyPatch

from src import config
from src.app import FutuBalanceSnapshot, FutuBalanceSyncService
from src.app.futu_balance_sync_service import FutuOpenApiBalanceProvider
from src.models import AssetType, Holding
from skill_api import PortfolioSkill


class FakeProvider:
    def __init__(self, cash=100.126, mmf=200.334):
        self.cash = cash
        self.mmf = mmf

    def fetch_balances(self):
        return FutuBalanceSnapshot(cash=self.cash, mmf=self.mmf, source="fake")


class FakeStorage:
    def __init__(self):
        self.holdings = {}
        self.updates = []
        self.creates = []

    def get_holding(self, asset_id, account, broker=None):
        return self.holdings.get((asset_id, account, broker))

    def update_holding_quantity(self, asset_id, account, quantity_change, broker=None):
        self.updates.append((asset_id, account, quantity_change, broker))
        holding = self.holdings[(asset_id, account, broker)]
        holding.quantity += quantity_change

    def upsert_holding(self, holding):
        self.creates.append(holding)
        self.holdings[(holding.asset_id, holding.account, holding.broker)] = holding
        return holding


def test_sync_cash_and_mmf_updates_existing_holdings_by_delta():
    storage = FakeStorage()
    storage.holdings[("CNY-CASH", "lx", "富途")] = Holding(
        asset_id="CNY-CASH",
        asset_name="人民币现金",
        asset_type=AssetType.CASH,
        account="lx",
        broker="富途",
        quantity=20,
        currency="CNY",
    )
    storage.holdings[("CNY-MMF", "lx", "富途")] = Holding(
        asset_id="CNY-MMF",
        asset_name="货币基金",
        asset_type=AssetType.MMF,
        account="lx",
        broker="富途",
        quantity=50,
        currency="CNY",
    )

    result = FutuBalanceSyncService(storage, FakeProvider()).sync_cash_and_mmf(account="lx")

    assert result["success"] is True
    assert result["updated"] == 2
    assert result["created"] == 0
    assert storage.updates == [
        ("CNY-CASH", "lx", 80.13, "富途"),
        ("CNY-MMF", "lx", 150.33, "富途"),
    ]
    assert storage.holdings[("CNY-CASH", "lx", "富途")].quantity == 100.13
    assert storage.holdings[("CNY-MMF", "lx", "富途")].quantity == 200.33


def test_sync_cash_and_mmf_creates_missing_holdings():
    storage = FakeStorage()

    result = FutuBalanceSyncService(storage, FakeProvider(cash=10, mmf=0)).sync_cash_and_mmf(account="lx")

    assert result["created"] == 2
    assert storage.updates == []
    assert [h.asset_id for h in storage.creates] == ["CNY-CASH", "CNY-MMF"]
    assert storage.holdings[("CNY-CASH", "lx", "富途")].quantity == 10.0
    assert storage.holdings[("CNY-MMF", "lx", "富途")].quantity == 0.0


def test_sync_cash_and_mmf_dry_run_does_not_write():
    storage = FakeStorage()
    storage.holdings[("CNY-CASH", "lx", "富途")] = Holding(
        asset_id="CNY-CASH",
        asset_name="人民币现金",
        asset_type=AssetType.CASH,
        account="lx",
        broker="富途",
        quantity=20,
        currency="CNY",
    )

    result = FutuBalanceSyncService(storage, FakeProvider(cash=100, mmf=None)).sync_cash_and_mmf(account="lx", dry_run=True)

    assert result["items"][0]["delta"] == 80.0
    assert storage.updates == []
    assert storage.creates == []
    assert storage.holdings[("CNY-CASH", "lx", "富途")].quantity == 20


def test_sync_cash_and_mmf_accepts_manual_balances_without_provider():
    storage = FakeStorage()

    result = FutuBalanceSyncService(storage).sync_cash_and_mmf(account="lx", cash_balance=1.235, mmf_balance=None)

    assert result["items"] == [
        {
            "asset_id": "CNY-CASH",
            "asset_name": "人民币现金",
            "current": 0.0,
            "target": 1.24,
            "delta": 1.24,
            "created": True,
            "updated": True,
        }
    ]
    assert storage.holdings[("CNY-CASH", "lx", "富途")].quantity == 1.24


def test_futu_openapi_provider_reads_mmf_from_accinfo_fund_assets():
    class FakeCtx:
        def __init__(self):
            self.position_called = False

        def accinfo_query(self, **kwargs):
            return 0, [{"cash": "12.345", "fund_assets": "345.678"}]

        def position_list_query(self, **kwargs):
            self.position_called = True
            raise AssertionError("MMF should be read from accinfo.fund_assets")

    futu_sdk = SimpleNamespace(RET_OK=0, TrdEnv=SimpleNamespace(REAL="REAL"), Currency=SimpleNamespace(CNH="CNH"))
    ctx = FakeCtx()
    provider = FutuOpenApiBalanceProvider()

    assert provider._fetch_cash(futu_sdk, ctx) == 12.345
    assert provider._fetch_mmf(futu_sdk, ctx) == 345.68
    assert ctx.position_called is False


def test_futu_openapi_provider_reads_defaults_from_config_file():
    with TemporaryDirectory() as tmp:
        config_file = Path(tmp) / "config.json"
        config_file.write_text(
            json.dumps({
                "futu": {
                    "opend": {"host": "10.0.0.2", "port": 22222},
                    "trd_env": "SIMULATE",
                    "acc_id": 123456,
                    "trd_market": "US",
                    "cash_currency": "USD",
                }
            }),
            encoding="utf-8",
        )

        patch = MonkeyPatch()
        try:
            patch.setattr(config, "_CONFIG_FILE", config_file)
            for name in (
                "FUTU_OPEND_HOST",
                "FUTU_OPEND_PORT",
                "FUTU_TRD_ENV",
                "FUTU_ACC_ID",
                "FUTU_TRD_MARKET",
                "FUTU_CASH_CURRENCY",
            ):
                patch.delenv(name, raising=False)
            config.reload_config()

            provider = FutuOpenApiBalanceProvider()
            assert provider.host == "10.0.0.2"
            assert provider.port == 22222
            assert provider.trd_env == "SIMULATE"
            assert provider.acc_id == 123456
            assert provider.trd_market == "US"
            assert provider.cash_currency == "USD"
        finally:
            patch.undo()
            config.reload_config()


def test_portfolio_skill_futu_sync_defaults_to_dry_run(monkeypatch):
    calls = []

    class FakeService:
        def __init__(self, storage):
            self.storage = storage

        def sync_cash_and_mmf(self, **kwargs):
            calls.append(kwargs)
            return {"success": True, "dry_run": kwargs["dry_run"]}

    import skill_api

    monkeypatch.setattr(skill_api, "FutuBalanceSyncService", FakeService)
    skill = PortfolioSkill.__new__(PortfolioSkill)
    skill.account = "lx"
    skill.storage = object()

    result = skill.sync_futu_cash_mmf()

    assert result == {"success": True, "dry_run": True}
    assert calls[0]["dry_run"] is True
