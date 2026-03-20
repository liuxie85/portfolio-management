from src.models import Holding, Transaction, CashFlow, NAVHistory, PortfolioValuation, AssetType, TransactionType


def test_transaction_model_quantizes_money_fields():
    tx = Transaction(
        tx_date='2025-03-14',
        tx_type=TransactionType.BUY,
        asset_id='000001',
        account='测试账户',
        quantity=1.005,
        price=1.005,
        fee=0.005,
        currency='CNY',
    )
    assert tx.price == 1.01
    assert tx.fee == 0.01
    assert tx.amount == 1.02


def test_cashflow_model_quantizes_money_fields():
    cf = CashFlow(
        flow_date='2025-03-14',
        account='测试账户',
        amount=1.005,
        currency='CNY',
        cny_amount=1.005,
        flow_type='DEPOSIT',
    )
    assert cf.amount == 1.01
    assert cf.cny_amount == 1.01


def test_holding_model_quantizes_avg_cost_only():
    holding = Holding(
        asset_id='000001',
        asset_name='平安银行',
        asset_type=AssetType.A_STOCK,
        account='测试账户',
        quantity=1.005,
        avg_cost=1.005,
        currency='CNY',
    )
    assert holding.quantity == 1.005
    assert holding.avg_cost == 1.01


def test_navhistory_model_quantizes_core_fields():
    nav = NAVHistory(
        date='2025-03-14',
        account='测试账户',
        total_value=1.005,
        cash_value=0.335,
        stock_value=0.67,
        fund_value=0.0,
        cn_stock_value=0.67,
        us_stock_value=0.0,
        hk_stock_value=0.0,
        stock_weight=0.6666666,
        cash_weight=0.3333336,
        shares=1.005,
        nav=1.1234567,
        cash_flow=0.335,
        share_change=0.335,
        mtd_nav_change=0.1234567,
        ytd_nav_change=0.1234567,
        pnl=0.335,
        mtd_pnl=0.335,
        ytd_pnl=0.335,
        details={},
    )
    assert nav.total_value == 1.01
    assert nav.cash_value == 0.34
    assert nav.stock_weight == 0.666667
    assert nav.cash_weight == 0.333334
    assert nav.nav == 1.123457
    assert nav.mtd_nav_change == 0.123457
    assert nav.pnl == 0.34


def test_portfolio_valuation_model_quantizes_fields():
    valuation = PortfolioValuation(
        account='测试账户',
        total_value_cny=2.005,
        cash_value_cny=1.005,
        stock_value_cny=1.0,
        fund_value_cny=0.0,
        cn_asset_value=1.0,
        us_asset_value=0.0,
        hk_asset_value=0.0,
        shares=10.005,
        nav=0.2005006,
    )
    assert valuation.total_value_cny == 2.01
    assert valuation.cash_value_cny == 1.01
    assert valuation.shares == 10.01
    assert valuation.nav == 0.200501
