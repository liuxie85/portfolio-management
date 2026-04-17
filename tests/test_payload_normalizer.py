from decimal import Decimal

import pytest

from src.domain.payload_normalizer import PayloadNormalizer
from src.portfolio import PortfolioManager


def test_payload_normalizer_quantizes_values():
    assert PayloadNormalizer.to_decimal(None) == Decimal("0")
    assert PayloadNormalizer.quantize_money("1.005") == Decimal("1.01")
    assert PayloadNormalizer.quantize_nav("1.1234567") == Decimal("1.123457")
    assert PayloadNormalizer.quantize_weight("0.3333336") == Decimal("0.333334")


def test_payload_normalizer_normalizes_transaction_payload():
    result = PayloadNormalizer.normalize_transaction_payload(quantity=1.005, price=1.005, fee=0.005)

    assert result == {
        "quantity": 1.005,
        "price": 1.01,
        "fee": 0.01,
        "amount": 1.02,
    }


def test_payload_normalizer_normalizes_cash_flow_payload():
    result = PayloadNormalizer.normalize_cash_flow_payload(amount=10, currency="USD", exchange_rate=7.1234)

    assert result == {
        "amount": 10.0,
        "cny_amount": 71.23,
        "exchange_rate": 7.1234,
    }


def test_payload_normalizer_requires_foreign_cash_flow_fx_context():
    with pytest.raises(ValueError, match="外币现金流必须显式提供"):
        PayloadNormalizer.normalize_cash_flow_payload(amount=10, currency="USD")


def test_payload_normalizer_normalizes_holding_payload():
    assert PayloadNormalizer.normalize_holding_payload(quantity=1.005, cash_like=True)["quantity"] == 1.01
    assert PayloadNormalizer.normalize_holding_payload(quantity=1.005, cash_like=False)["quantity"] == 1.005
    assert PayloadNormalizer.normalize_holding_payload(quantity=1, avg_cost=1.005)["avg_cost"] == 1.01


def test_portfolio_payload_helpers_delegate_to_payload_normalizer():
    assert PortfolioManager._quantize_money("1.005") == Decimal("1.01")
    assert PortfolioManager._normalize_transaction_payload(quantity=1, price=1.005, fee=0.005)["amount"] == 1.01
    assert PortfolioManager._normalize_holding_payload(quantity=1.005, cash_like=True)["quantity"] == 1.01
