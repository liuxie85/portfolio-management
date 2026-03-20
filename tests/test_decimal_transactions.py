from datetime import date
from unittest.mock import Mock, patch

from src.portfolio import PortfolioManager
from src.models import Transaction, TransactionType, Holding, AssetType


@patch.object(PortfolioManager, '_get_asset_name', return_value='平安银行')
def test_buy_uses_decimal_quantized_total_cost(mock_get_name):
    storage = Mock()
    fetcher = Mock()
    manager = PortfolioManager(storage=storage, price_fetcher=fetcher)

    storage.add_transaction.return_value = Transaction(
        tx_date=date(2025, 3, 14),
        tx_type=TransactionType.BUY,
        asset_id='000001',
        asset_name='平安银行',
        account='测试账户',
        quantity=1,
        price=1.005,
        currency='CNY',
    )
    storage.upsert_holding.return_value = Holding(
        asset_id='000001',
        asset_name='平安银行',
        asset_type=AssetType.A_STOCK,
        account='测试账户',
        quantity=1,
        currency='CNY',
    )
    manager._has_sufficient_cash = Mock(return_value=True)
    manager._deduct_cash = Mock(return_value=True)

    manager.buy(
        tx_date=date(2025, 3, 14),
        asset_id='000001',
        asset_name='平安银行',
        asset_type=AssetType.A_STOCK,
        account='测试账户',
        quantity=1,
        price=1.005,
        currency='CNY',
        fee=0.005,
        auto_deduct_cash=True,
    )

    manager._has_sufficient_cash.assert_called_once_with('测试账户', 1.02)
    manager._deduct_cash.assert_called_once_with('测试账户', 1.02)


def test_sell_uses_decimal_quantized_proceeds():
    storage = Mock()
    fetcher = Mock()
    manager = PortfolioManager(storage=storage, price_fetcher=fetcher)
    storage.get_holding.return_value = Holding(
        asset_id='000001',
        asset_name='平安银行',
        asset_type=AssetType.A_STOCK,
        account='测试账户',
        quantity=1000,
        currency='CNY',
    )
    storage.add_transaction.return_value = Mock()
    manager._add_cash = Mock()

    manager.sell(
        tx_date=date(2025, 3, 14),
        asset_id='000001',
        account='测试账户',
        quantity=1,
        price=1.005,
        currency='CNY',
        fee=0.005,
        auto_add_cash=True,
    )

    manager._add_cash.assert_called_once_with('测试账户', 1.0)
