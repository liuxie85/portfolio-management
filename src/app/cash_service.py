"""Cash holding side-effect service."""
from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP
from typing import Any

from src.models import (
    AssetClass,
    AssetType,
    CASH_ASSET_ID,
    Currency,
    HKD_CASH_ASSET_ID,
    MMF_ASSET_ID,
    USD_CASH_ASSET_ID,
    Holding,
)


class CashService:
    MONEY_QUANT = Decimal("0.01")

    def __init__(self, storage: Any):
        self.storage = storage

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
    def cash_asset_id_for_currency(cls, currency: str) -> str:
        if currency == Currency.CNY:
            return CASH_ASSET_ID
        if currency == Currency.USD:
            return USD_CASH_ASSET_ID
        if currency == Currency.HKD:
            return HKD_CASH_ASSET_ID
        return f"{currency}-CASH"

    def update_cash_holding(self, account: str, amount: float, currency: str, cny_amount: float = None) -> None:
        asset_id = self.cash_asset_id_for_currency(currency)
        cash_holding = self.storage.get_holding(asset_id, account)
        quantity = float(self.quantize_money(amount))

        if cash_holding:
            self.storage.update_holding_quantity(asset_id, account, quantity)
            return

        holding = Holding(
            asset_id=asset_id,
            asset_name=f"{currency}现金",
            asset_type=AssetType.CASH,
            account=account,
            quantity=quantity,
            currency=currency,
            asset_class=AssetClass.CASH,
            industry="现金",
        )
        self.storage.upsert_holding(holding)

    def get_cash_like_holdings(self, account: str):
        cash_holding = self.storage.get_holding(CASH_ASSET_ID, account)
        mmf_holding = self.storage.get_holding(MMF_ASSET_ID, account)
        return cash_holding, mmf_holding

    def has_sufficient_cash(self, account: str, amount: float) -> bool:
        if amount <= 0:
            return True

        cash_holding, mmf_holding = self.get_cash_like_holdings(account)
        total_cash = Decimal("0")
        if cash_holding and cash_holding.quantity > 0:
            total_cash += self.to_decimal(cash_holding.quantity)
        if mmf_holding and mmf_holding.quantity > 0:
            total_cash += self.to_decimal(mmf_holding.quantity)
        return total_cash >= self.to_decimal(amount)

    def deduct_cash(self, account: str, amount: float) -> bool:
        if amount <= 0:
            return True

        remaining = self.to_decimal(amount)
        cash_holding, mmf_holding = self.get_cash_like_holdings(account)

        if cash_holding and cash_holding.quantity > 0:
            cash_qty = self.to_decimal(cash_holding.quantity)
            deduct_from_cash = min(cash_qty, remaining)
            self.storage.update_holding_quantity(CASH_ASSET_ID, account, float(-self.quantize_money(deduct_from_cash)))
            remaining -= deduct_from_cash
            print(f"  从 {CASH_ASSET_ID} 扣除: ¥{float(self.quantize_money(deduct_from_cash)):,.2f}")

        if remaining > 0 and mmf_holding and mmf_holding.quantity > 0:
            mmf_qty = self.to_decimal(mmf_holding.quantity)
            deduct_from_mmf = min(mmf_qty, remaining)
            self.storage.update_holding_quantity(MMF_ASSET_ID, account, float(-self.quantize_money(deduct_from_mmf)))
            remaining -= deduct_from_mmf
            print(f"  从 {MMF_ASSET_ID} 扣除: ¥{float(self.quantize_money(deduct_from_mmf)):,.2f}")

        if remaining > 0:
            print(f"  ✗ 现金不足，还需: ¥{float(self.quantize_money(remaining)):,.2f}")
            return False

        return True

    def add_cash(self, account: str, amount: float) -> bool:
        if amount <= 0:
            return True

        amount_dec = self.quantize_money(amount)
        cash_holding = self.storage.get_holding(CASH_ASSET_ID, account)

        if cash_holding:
            self.storage.update_holding_quantity(CASH_ASSET_ID, account, float(amount_dec))
        else:
            holding = Holding(
                asset_id=CASH_ASSET_ID,
                asset_name="人民币现金",
                asset_type=AssetType.CASH,
                account=account,
                quantity=float(amount_dec),
                currency="CNY",
                asset_class=AssetClass.CASH,
                industry="现金",
            )
            self.storage.upsert_holding(holding)

        print(f"  增加到 {CASH_ASSET_ID}: ¥{float(amount_dec):,.2f}")
        return True
