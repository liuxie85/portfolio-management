from __future__ import annotations

from src.app import FutuBalanceSnapshot, FutuBalanceSyncService
from src.models import AssetType, Holding


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

    def get_holding(self, asset_id, account, market=None):
        return self.holdings.get((asset_id, account, market))

    def update_holding_quantity(self, asset_id, account, quantity_change, market=None):
        self.updates.append((asset_id, account, quantity_change, market))
        holding = self.holdings[(asset_id, account, market)]
        holding.quantity += quantity_change

    def upsert_holding(self, holding):
        self.creates.append(holding)
        self.holdings[(holding.asset_id, holding.account, holding.market)] = holding
        return holding


def test_sync_cash_and_mmf_updates_existing_holdings_by_delta():
    storage = FakeStorage()
    storage.holdings[("CNY-CASH", "lx", "富途")] = Holding(
        asset_id="CNY-CASH",
        asset_name="人民币现金",
        asset_type=AssetType.CASH,
        account="lx",
        market="富途",
        quantity=20,
        currency="CNY",
    )
    storage.holdings[("CNY-MMF", "lx", "富途")] = Holding(
        asset_id="CNY-MMF",
        asset_name="货币基金",
        asset_type=AssetType.MMF,
        account="lx",
        market="富途",
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
        market="富途",
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
