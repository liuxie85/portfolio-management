"""Decimal-safe payload normalization helpers."""
from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, Optional


class PayloadNormalizer:
    MONEY_QUANT = Decimal("0.01")
    NAV_QUANT = Decimal("0.000001")
    WEIGHT_QUANT = Decimal("0.000001")

    @staticmethod
    def to_decimal(value: Any) -> Decimal:
        if value is None:
            return Decimal("0")
        if isinstance(value, Decimal):
            return value
        return Decimal(str(value))

    @classmethod
    def quantize_money(cls, value: Any) -> Decimal:
        return cls.to_decimal(value).quantize(cls.MONEY_QUANT, rounding=ROUND_HALF_UP)

    @classmethod
    def quantize_nav(cls, value: Any) -> Decimal:
        return cls.to_decimal(value).quantize(cls.NAV_QUANT, rounding=ROUND_HALF_UP)

    @classmethod
    def quantize_weight(cls, value: Any) -> Decimal:
        return cls.to_decimal(value).quantize(cls.WEIGHT_QUANT, rounding=ROUND_HALF_UP)

    @classmethod
    def normalize_transaction_payload(cls, *, quantity: Any, price: Any, fee: Any = 0.0) -> Dict[str, float]:
        quantity_dec = cls.to_decimal(quantity)
        price_dec = cls.quantize_money(price)
        fee_dec = cls.quantize_money(fee)
        amount_dec = cls.quantize_money(quantity_dec * price_dec)
        return {
            "quantity": float(quantity_dec),
            "price": float(price_dec),
            "fee": float(fee_dec),
            "amount": float(amount_dec),
        }

    @classmethod
    def normalize_cash_flow_payload(
        cls,
        *,
        amount: Any,
        currency: str = "CNY",
        cny_amount: Any = None,
        exchange_rate: Any = None,
    ) -> Dict[str, Optional[float]]:
        amount_dec = cls.quantize_money(amount)

        normalized_currency = (currency or "CNY").upper()
        if normalized_currency != "CNY" and cny_amount is None and exchange_rate is None:
            raise ValueError(f"外币现金流必须显式提供 cny_amount 或 exchange_rate: currency={normalized_currency}")

        rate_dec = cls.to_decimal(exchange_rate) if exchange_rate is not None else Decimal("1")
        if cny_amount is not None:
            cny_amount_dec = cls.quantize_money(cny_amount)
        else:
            cny_amount_dec = cls.quantize_money(amount_dec * rate_dec)

        return {
            "amount": float(amount_dec),
            "cny_amount": float(cny_amount_dec),
            "exchange_rate": float(rate_dec),
        }

    @classmethod
    def normalize_holding_payload(
        cls,
        *,
        quantity: Any,
        avg_cost: Any = None,
        cash_like: bool = False,
    ) -> Dict[str, Optional[float]]:
        quantity_dec = cls.quantize_money(quantity) if cash_like else cls.to_decimal(quantity)
        avg_cost_dec = cls.quantize_money(avg_cost) if avg_cost is not None else None
        return {
            "quantity": float(quantity_dec),
            "avg_cost": float(avg_cost_dec) if avg_cost_dec is not None else None,
        }
