"""Trade and cash-flow application service."""
from __future__ import annotations

from datetime import date
from typing import Any, Optional

from src.models import (
    AssetClass,
    AssetType,
    CashFlow,
    Holding,
    Transaction,
    TransactionType,
)


class TradeService:
    """Coordinate transaction/cash-flow writes and repair-task recording.

    ``manager`` is intentionally used as a compatibility facade so existing
    PortfolioManager helper patch points keep working while the orchestration
    code moves out of ``portfolio.py``.
    """

    def __init__(self, manager: Any, storage: Any):
        self.manager = manager
        self.storage = storage

    def buy(
        self,
        tx_date: date,
        asset_id: str,
        asset_name: str,
        asset_type: AssetType,
        account: str,
        quantity: float,
        price: float,
        currency: str,
        market: Optional[str] = None,
        fee: float = 0,
        remark: str = "",
        asset_class: Optional[AssetClass] = None,
        industry: Optional[str] = None,
        auto_deduct_cash: bool = True,
        request_id: str = None,
    ) -> Transaction:
        full_asset_name = self.manager._get_asset_name(asset_id, asset_type, asset_name)
        if full_asset_name != asset_name:
            print(f"[名称自动补全] {asset_name} -> {full_asset_name}")

        tx_payload = self.manager._normalize_transaction_payload(quantity=quantity, price=price, fee=fee)
        total_cost = float(
            self.manager._quantize_money(
                self.manager._to_decimal(tx_payload["amount"]) + self.manager._to_decimal(tx_payload["fee"])
            )
        )

        if auto_deduct_cash and currency == "CNY":
            if not self.manager._has_sufficient_cash(account, total_cost):
                raise ValueError(f"账户 {account} 现金不足，需要 ¥{total_cost:,.2f}")

        tx = Transaction(
            tx_date=tx_date,
            tx_type=TransactionType.BUY,
            asset_id=asset_id,
            asset_name=full_asset_name,
            asset_type=asset_type,
            account=account,
            market=market,
            quantity=tx_payload["quantity"],
            price=tx_payload["price"],
            amount=tx_payload["amount"],
            currency=currency,
            fee=tx_payload["fee"],
            remark=remark,
            request_id=request_id,
        )

        try:
            tx = self.storage.add_transaction(tx)
        except Exception as exc:
            print(f"[买入失败] 记录交易失败: {exc}")
            raise

        holding_payload = self.manager._normalize_holding_payload(quantity=quantity)
        holding = Holding(
            asset_id=asset_id,
            asset_name=full_asset_name,
            asset_type=asset_type,
            account=account,
            market=market,
            quantity=holding_payload["quantity"],
            currency=currency,
            asset_class=asset_class,
            industry=industry,
        )

        try:
            self.storage.upsert_holding(holding)
        except Exception as exc:
            print(f"[警告] 持仓更新失败，但交易已记录: {exc}")
            self.manager._record_compensation(
                operation_type="BUY_HOLDING_UPSERT_FAILED",
                account=account,
                related_record_id=tx.record_id,
                payload={
                    "transaction": tx.model_dump(mode="json"),
                    "holding_delta": holding.model_dump(mode="json"),
                },
                error=exc,
            )

        if auto_deduct_cash and currency == "CNY":
            try:
                cash_deducted = self.manager._deduct_cash(account, total_cost)
                if not cash_deducted:
                    print(f"[警告] 买入交易已记录，但现金扣减失败。请手动调整账户 {account} 的现金余额 ¥{total_cost:,.2f}")
                    self.manager._record_compensation(
                        operation_type="BUY_CASH_DEDUCT_FAILED",
                        account=account,
                        related_record_id=tx.record_id,
                        payload={
                            "transaction": tx.model_dump(mode="json"),
                            "cash_delta": -total_cost,
                            "currency": currency,
                        },
                        error="cash deduction returned False",
                    )
            except Exception as exc:
                print(f"[警告] 现金扣减异常: {exc}")
                self.manager._record_compensation(
                    operation_type="BUY_CASH_DEDUCT_EXCEPTION",
                    account=account,
                    related_record_id=tx.record_id,
                    payload={
                        "transaction": tx.model_dump(mode="json"),
                        "cash_delta": -total_cost,
                        "currency": currency,
                    },
                    error=exc,
                )

        return tx

    def sell(
        self,
        tx_date: date,
        asset_id: str,
        account: str,
        quantity: float,
        price: float,
        currency: str,
        market: Optional[str] = None,
        fee: float = 0,
        remark: str = "",
        auto_add_cash: bool = True,
        request_id: str = None,
    ) -> Transaction:
        holding = self.storage.get_holding(asset_id, account, market)
        if holding:
            asset_name = holding.asset_name
            asset_type = holding.asset_type
        else:
            asset_type = None
            asset_name = self.manager._get_asset_name(asset_id, asset_type, asset_id)
            print(f"[警告] 未找到持仓记录，尝试查询名称: {asset_id} -> {asset_name}")

        tx_payload = self.manager._normalize_transaction_payload(quantity=-quantity, price=price, fee=fee)
        tx = Transaction(
            tx_date=tx_date,
            tx_type=TransactionType.SELL,
            asset_id=asset_id,
            asset_name=asset_name,
            asset_type=asset_type,
            account=account,
            market=market,
            quantity=tx_payload["quantity"],
            price=tx_payload["price"],
            amount=tx_payload["amount"],
            currency=currency,
            fee=tx_payload["fee"],
            remark=remark,
            request_id=request_id,
        )
        tx = self.storage.add_transaction(tx)

        sell_holding_payload = self.manager._normalize_holding_payload(quantity=-quantity)
        try:
            self.storage.update_holding_quantity(asset_id, account, sell_holding_payload["quantity"], market)
        except Exception as exc:
            self.manager._record_compensation(
                operation_type="SELL_HOLDING_UPDATE_FAILED",
                account=account,
                related_record_id=tx.record_id,
                payload={
                    "transaction": tx.model_dump(mode="json"),
                    "asset_id": asset_id,
                    "market": market,
                    "quantity_delta": sell_holding_payload["quantity"],
                },
                error=exc,
            )
            raise

        try:
            self.storage.delete_holding_if_zero(asset_id, account, market)
        except Exception as exc:
            self.manager._record_compensation(
                operation_type="SELL_ZERO_HOLDING_DELETE_FAILED",
                account=account,
                related_record_id=tx.record_id,
                payload={
                    "transaction": tx.model_dump(mode="json"),
                    "asset_id": asset_id,
                    "market": market,
                },
                error=exc,
            )
            raise

        if auto_add_cash and currency == "CNY":
            gross_proceeds = self.manager._quantize_money(
                self.manager._to_decimal(abs(quantity)) * self.manager._to_decimal(price)
            )
            total_proceeds = float(
                self.manager._quantize_money(
                    self.manager._to_decimal(gross_proceeds) - self.manager._to_decimal(tx_payload["fee"])
                )
            )
            try:
                self.manager._add_cash(account, total_proceeds)
            except Exception as exc:
                self.manager._record_compensation(
                    operation_type="SELL_CASH_ADD_FAILED",
                    account=account,
                    related_record_id=tx.record_id,
                    payload={
                        "transaction": tx.model_dump(mode="json"),
                        "cash_delta": total_proceeds,
                        "currency": currency,
                    },
                    error=exc,
                )
                raise

        return tx

    def deposit(
        self,
        flow_date: date,
        account: str,
        amount: float,
        currency: str,
        cny_amount: Optional[float] = None,
        exchange_rate: Optional[float] = None,
        source: str = "",
        remark: str = "",
    ) -> CashFlow:
        cf_payload = self.manager._normalize_cash_flow_payload(
            amount=amount,
            currency=currency,
            cny_amount=cny_amount,
            exchange_rate=exchange_rate,
        )
        cf = CashFlow(
            flow_date=flow_date,
            account=account,
            amount=cf_payload["amount"],
            currency=currency,
            cny_amount=cf_payload["cny_amount"],
            exchange_rate=cf_payload["exchange_rate"],
            flow_type="DEPOSIT",
            source=source,
            remark=remark,
        )
        cf = self.storage.add_cash_flow(cf)

        try:
            self.manager._update_cash_holding(account, cf_payload["amount"], currency, cf_payload["cny_amount"])
        except Exception as exc:
            self.manager._record_compensation(
                operation_type="DEPOSIT_CASH_HOLDING_UPDATE_FAILED",
                account=account,
                related_record_id=cf.record_id,
                payload={
                    "cash_flow": cf.model_dump(mode="json"),
                    "cash_delta": cf_payload["amount"],
                    "currency": currency,
                    "cny_amount": cf_payload["cny_amount"],
                },
                error=exc,
            )
            raise

        return cf

    def withdraw(
        self,
        flow_date: date,
        account: str,
        amount: float,
        currency: str,
        cny_amount: Optional[float] = None,
        exchange_rate: Optional[float] = None,
        remark: str = "",
    ) -> CashFlow:
        cf_payload = self.manager._normalize_cash_flow_payload(
            amount=amount,
            currency=currency,
            cny_amount=cny_amount,
            exchange_rate=exchange_rate,
        )
        cf = CashFlow(
            flow_date=flow_date,
            account=account,
            amount=-cf_payload["amount"],
            currency=currency,
            cny_amount=-cf_payload["cny_amount"],
            exchange_rate=cf_payload["exchange_rate"],
            flow_type="WITHDRAW",
            remark=remark,
        )
        cf = self.storage.add_cash_flow(cf)

        try:
            self.manager._update_cash_holding(account, -cf_payload["amount"], currency, -cf_payload["cny_amount"])
        except Exception as exc:
            self.manager._record_compensation(
                operation_type="WITHDRAW_CASH_HOLDING_UPDATE_FAILED",
                account=account,
                related_record_id=cf.record_id,
                payload={
                    "cash_flow": cf.model_dump(mode="json"),
                    "cash_delta": -cf_payload["amount"],
                    "currency": currency,
                    "cny_amount": -cf_payload["cny_amount"],
                },
                error=exc,
            )
            raise

        return cf
