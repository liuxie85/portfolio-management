from datetime import date
from unittest.mock import Mock

from src.app.trade_service import TradeService
from src.models import AssetType, Holding
from src.portfolio import PortfolioManager


def _manager(storage):
    manager = PortfolioManager(storage=storage, price_fetcher=Mock())
    manager._get_asset_name = Mock(side_effect=lambda asset_id, asset_type, fallback: fallback)
    return manager


def test_trade_service_buy_records_transaction_and_holding():
    storage = Mock()
    storage.add_transaction.side_effect = lambda tx: tx
    storage.upsert_holding.side_effect = lambda holding: holding
    manager = _manager(storage)
    service = TradeService(manager=manager, storage=storage)

    tx = service.buy(
        tx_date=date(2025, 3, 14),
        asset_id="000001",
        asset_name="平安银行",
        asset_type=AssetType.A_STOCK,
        account="a",
        quantity=1.005,
        price=1.005,
        currency="CNY",
        fee=0.005,
        auto_deduct_cash=False,
    )

    holding = storage.upsert_holding.call_args[0][0]
    assert tx.quantity == 1.005
    assert tx.price == 1.01
    assert tx.amount == 1.02
    assert holding.quantity == 1.005


def test_trade_service_buy_records_compensation_when_cash_deduct_fails():
    storage = Mock()
    storage.add_transaction.side_effect = lambda tx: tx
    storage.upsert_holding.side_effect = lambda holding: holding
    manager = _manager(storage)
    manager._has_sufficient_cash = Mock(return_value=True)
    manager._deduct_cash = Mock(return_value=False)
    manager._record_compensation = Mock()
    service = TradeService(manager=manager, storage=storage)

    service.buy(
        tx_date=date(2025, 3, 14),
        asset_id="000001",
        asset_name="平安银行",
        asset_type=AssetType.A_STOCK,
        account="a",
        quantity=1,
        price=10,
        currency="CNY",
        auto_deduct_cash=True,
    )

    manager._record_compensation.assert_called()
    assert manager._record_compensation.call_args.kwargs["operation_type"] == "BUY_CASH_DEDUCT_FAILED"


def test_trade_service_sell_uses_manager_add_cash_patch_point():
    storage = Mock()
    storage.get_holding.return_value = Holding(
        asset_id="000001",
        asset_name="平安银行",
        asset_type=AssetType.A_STOCK,
        account="a",
        quantity=100,
        currency="CNY",
    )
    storage.add_transaction.side_effect = lambda tx: tx
    manager = _manager(storage)
    manager._add_cash = Mock()
    service = TradeService(manager=manager, storage=storage)

    tx = service.sell(
        tx_date=date(2025, 3, 14),
        asset_id="000001",
        account="a",
        quantity=1,
        price=10,
        currency="CNY",
        fee=1,
        auto_add_cash=True,
    )

    assert tx.quantity == -1.0
    manager._add_cash.assert_called_once_with("a", 9.0)


def test_trade_service_deposit_uses_cash_holding_patch_point():
    storage = Mock()
    storage.add_cash_flow.side_effect = lambda cf: cf
    manager = _manager(storage)
    manager._update_cash_holding = Mock()
    service = TradeService(manager=manager, storage=storage)

    cf = service.deposit(
        flow_date=date(2025, 3, 14),
        account="a",
        amount=1.005,
        currency="CNY",
        cny_amount=1.005,
    )

    assert cf.amount == 1.01
    manager._update_cash_holding.assert_called_once_with("a", 1.01, "CNY", 1.01)
