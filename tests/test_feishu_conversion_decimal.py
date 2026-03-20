from unittest.mock import Mock

from src.feishu_storage import FeishuStorage


def test_to_feishu_fields_quantizes_numeric_payloads():
    storage = FeishuStorage(client=Mock())

    tx_fields = storage._to_feishu_fields({
        'price': 1.005,
        'fee': 0.005,
        'amount': 1.015,
        'quantity': 1.005,
    }, 'transactions')
    assert tx_fields['price'] == '1.01'
    assert tx_fields['fee'] == '0.01'
    assert tx_fields['amount'] == '1.02'
    assert tx_fields['quantity'] == '1.005'

    nav_fields = storage._to_feishu_fields({
        'mtd_nav_change': 0.1234567,
        'mtd_pnl': 1.005,
        'stock_weight': 0.3333336,
    }, 'nav_history', preserve_none=True)
    assert nav_fields['mtd_nav_change'] == 0.123457
    assert nav_fields['mtd_pnl'] == 1.01
    assert nav_fields['stock_weight'] == 0.333334


def test_from_feishu_fields_preserves_none_and_quantizes_values():
    storage = FeishuStorage(client=Mock())

    data = storage._from_feishu_fields({
        'total_value': '1.005',
        'mtd_nav_change': '0.1234567',
        'mtd_pnl': '1.005',
        'ytd_nav_change': None,
        'ytd_pnl': None,
        'stock_weight': '0.3333336',
    }, 'nav_history')

    assert data['total_value'] == 1.01
    assert data['mtd_nav_change'] == 0.123457
    assert data['mtd_pnl'] == 1.01
    assert data['ytd_nav_change'] is None
    assert data['ytd_pnl'] is None
    assert data['stock_weight'] == 0.333334
