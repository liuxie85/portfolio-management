from datetime import date
from unittest.mock import Mock, patch

from src.portfolio import PortfolioManager
from src.models import Transaction, TransactionType, Holding, AssetType, CashFlow


def test_normalize_payload_helpers():
    manager = PortfolioManager(storage=Mock(), price_fetcher=Mock())

    tx = manager._normalize_transaction_payload(quantity=1, price=1.005, fee=0.005)
    assert tx['price'] == 1.01
    assert tx['fee'] == 0.01
    assert tx['amount'] == 1.01

    cf = manager._normalize_cash_flow_payload(amount=1.005, cny_amount=1.005, exchange_rate=7.1234)
    assert cf['amount'] == 1.01
    assert cf['cny_amount'] == 1.01
    assert cf['exchange_rate'] == 7.1234

    holding_cash = manager._normalize_holding_payload(quantity=1.005, cash_like=True)
    holding_stock = manager._normalize_holding_payload(quantity=1.005, cash_like=False)
    assert holding_cash['quantity'] == 1.01
    assert holding_stock['quantity'] == 1.005


@patch.object(PortfolioManager, '_get_asset_name', return_value='平安银行')
def test_buy_creates_quantized_transaction_and_holding(mock_get_name):
    storage = Mock()
    fetcher = Mock()
    manager = PortfolioManager(storage=storage, price_fetcher=fetcher)
    storage.add_transaction.side_effect = lambda tx: tx
    storage.upsert_holding.side_effect = lambda holding: holding

    manager.buy(
        tx_date=date(2025, 3, 14),
        asset_id='000001',
        asset_name='平安银行',
        asset_type=AssetType.A_STOCK,
        account='测试账户',
        quantity=1.005,
        price=1.005,
        currency='CNY',
        fee=0.005,
        auto_deduct_cash=False,
    )

    tx = storage.add_transaction.call_args[0][0]
    holding = storage.upsert_holding.call_args[0][0]
    assert tx.price == 1.01
    assert tx.fee == 0.01
    assert tx.amount == 1.02
    assert holding.quantity == 1.005


def test_deposit_creates_quantized_cashflow_and_cash_holding():
    storage = Mock()
    fetcher = Mock()
    manager = PortfolioManager(storage=storage, price_fetcher=fetcher)
    storage.add_cash_flow.side_effect = lambda cf: cf
    storage.get_holding.return_value = None
    storage.upsert_holding.side_effect = lambda holding: holding

    manager.deposit(
        flow_date=date(2025, 3, 14),
        account='测试账户',
        amount=1.005,
        currency='CNY',
        cny_amount=1.005,
    )

    cf = storage.add_cash_flow.call_args[0][0]
    holding = storage.upsert_holding.call_args[0][0]
    assert cf.amount == 1.01
    assert cf.cny_amount == 1.01
    assert holding.quantity == 1.01


def test_foreign_cash_flow_requires_cny_amount_or_exchange_rate():
    manager = PortfolioManager(storage=Mock(), price_fetcher=Mock())

    with patch.object(PortfolioManager, '_update_cash_holding'):
        try:
            manager.deposit(
                flow_date=date(2025, 3, 14),
                account='测试账户',
                amount=1000,
                currency='USD',
            )
            assert False, 'expected ValueError'
        except ValueError as e:
            assert '外币现金流必须显式提供 cny_amount 或 exchange_rate' in str(e)


def test_sell_creates_quantized_transaction_boundary():
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
    storage.add_transaction.side_effect = lambda tx: tx
    manager._add_cash = Mock()

    manager.sell(
        tx_date=date(2025, 3, 14),
        asset_id='000001',
        account='测试账户',
        quantity=1.005,
        price=1.005,
        currency='CNY',
        fee=0.005,
        auto_add_cash=True,
    )

    tx = storage.add_transaction.call_args[0][0]
    assert tx.quantity == -1.005
    assert tx.price == 1.01
    assert tx.fee == 0.01
    assert tx.amount == -1.02
