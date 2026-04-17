from unittest.mock import Mock

from src.app.cash_service import CashService
from src.models import AssetType, Holding


def test_cash_service_update_existing_cny_holding():
    storage = Mock()
    storage.get_holding.return_value = Holding(
        asset_id="CNY-CASH",
        asset_name="人民币现金",
        asset_type=AssetType.CASH,
        account="a",
        quantity=100.0,
        currency="CNY",
    )
    service = CashService(storage)

    service.update_cash_holding("a", 1.005, "CNY", 1.005)

    storage.update_holding_quantity.assert_called_once_with("CNY-CASH", "a", 1.01)


def test_cash_service_creates_foreign_cash_holding():
    storage = Mock()
    storage.get_holding.return_value = None
    service = CashService(storage)

    service.update_cash_holding("a", 1000, "USD", 7200)

    holding = storage.upsert_holding.call_args[0][0]
    assert holding.asset_id == "USD-CASH"
    assert holding.asset_type == AssetType.CASH
    assert holding.quantity == 1000.0
    assert holding.currency == "USD"


def test_cash_service_deducts_cash_then_mmf():
    storage = Mock()
    storage.get_holding.side_effect = [
        Holding(asset_id="CNY-CASH", asset_name="人民币现金", asset_type=AssetType.CASH, account="a", quantity=3000, currency="CNY"),
        Holding(asset_id="CNY-MMF", asset_name="货币基金", asset_type=AssetType.MMF, account="a", quantity=10000, currency="CNY"),
    ]
    service = CashService(storage)

    assert service.deduct_cash("a", 5000) is True

    assert storage.update_holding_quantity.call_args_list[0].args == ("CNY-CASH", "a", -3000.0)
    assert storage.update_holding_quantity.call_args_list[1].args == ("CNY-MMF", "a", -2000.0)


def test_cash_service_insufficient_cash_returns_false():
    storage = Mock()
    storage.get_holding.side_effect = [
        Holding(asset_id="CNY-CASH", asset_name="人民币现金", asset_type=AssetType.CASH, account="a", quantity=1000, currency="CNY"),
        None,
    ]
    service = CashService(storage)

    assert service.deduct_cash("a", 5000) is False


def test_cash_service_add_cash_creates_when_missing():
    storage = Mock()
    storage.get_holding.return_value = None
    service = CashService(storage)

    assert service.add_cash("a", 1.005) is True

    holding = storage.upsert_holding.call_args[0][0]
    assert holding.asset_id == "CNY-CASH"
    assert holding.quantity == 1.01


def test_cash_service_sync_cash_like_balance_uses_absolute_target_delta():
    storage = Mock()
    storage.get_holding.return_value = Holding(
        asset_id="CNY-MMF",
        asset_name="货币基金",
        asset_type=AssetType.MMF,
        account="a",
        market="富途",
        quantity=10,
        currency="CNY",
    )
    service = CashService(storage)

    result = service.sync_cash_like_balance(
        account="a",
        asset_id="CNY-MMF",
        asset_name="货币基金",
        asset_type=AssetType.MMF,
        target=15.005,
        market="富途",
    )

    assert result["current"] == 10.0
    assert result["target"] == 15.01
    assert result["delta"] == 5.01
    assert result["created"] is False
    assert result["updated"] is True
    storage.update_holding_quantity.assert_called_once_with("CNY-MMF", "a", 5.01, "富途")


def test_cash_service_sync_cash_like_balance_dry_run_does_not_write():
    storage = Mock()
    storage.get_holding.return_value = None
    service = CashService(storage)

    result = service.sync_cash_like_balance(
        account="a",
        asset_id="CNY-CASH",
        asset_name="人民币现金",
        asset_type=AssetType.CASH,
        target=20,
        market="富途",
        dry_run=True,
    )

    assert result["created"] is True
    assert result["delta"] == 20.0
    storage.update_holding_quantity.assert_not_called()
    storage.upsert_holding.assert_not_called()
