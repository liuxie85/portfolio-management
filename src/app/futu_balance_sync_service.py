"""Sync Futu cash-like balances into holdings."""
from __future__ import annotations

import os
from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Iterable, Optional, Protocol

from src.models import (
    AssetType,
    CASH_ASSET_ID,
    MMF_ASSET_ID,
)
from .cash_service import CashService


@dataclass(frozen=True)
class FutuBalanceSnapshot:
    """Cash-like balances fetched from Futu.

    Values are absolute balances. The sync service converts them to deltas when
    writing holdings so repeated runs do not double count.
    """

    cash: Optional[float] = None
    mmf: Optional[float] = None
    currency: str = "CNY"
    source: str = "futu"


class FutuBalanceProvider(Protocol):
    def fetch_balances(self) -> FutuBalanceSnapshot:
        """Return absolute Futu cash/MMF balances."""


@dataclass(frozen=True)
class FutuBalanceSyncItem:
    asset_id: str
    asset_name: str
    current: float
    target: float
    delta: float
    created: bool
    updated: bool


class FutuOpenApiBalanceProvider:
    """Minimal Futu OpenAPI adapter.

    The ``futu`` SDK and Futu OpenD are optional runtime dependencies. Tests
    should inject ``FutuBalanceProvider`` instead of constructing this adapter.
    """

    CASH_COLUMNS = ("cash", "available_funds", "withdraw_cash", "power")
    MMF_NAME_KEYWORDS = ("货币", "现金", "money market", "money fund", "mmf")

    def __init__(
        self,
        *,
        host: Optional[str] = None,
        port: Optional[int] = None,
        trd_env: Optional[str] = None,
        acc_id: Optional[int] = None,
        market: Optional[str] = None,
        cash_currency: Optional[str] = None,
        mmf_codes: Optional[Iterable[str]] = None,
    ):
        self.host = host or os.environ.get("FUTU_OPEND_HOST", "127.0.0.1")
        self.port = int(port or os.environ.get("FUTU_OPEND_PORT", "11111"))
        self.trd_env = trd_env or os.environ.get("FUTU_TRD_ENV", "REAL")
        self.acc_id = int(acc_id) if acc_id is not None else _env_int("FUTU_ACC_ID")
        self.market = market or os.environ.get("FUTU_TRD_MARKET", "HK")
        self.cash_currency = cash_currency or os.environ.get("FUTU_CASH_CURRENCY", "CNH")
        env_codes = os.environ.get("FUTU_MMF_CODES", "")
        codes = mmf_codes if mmf_codes is not None else [c.strip() for c in env_codes.split(",")]
        self.mmf_codes = {str(c).upper() for c in codes if c}

    def fetch_balances(self) -> FutuBalanceSnapshot:
        try:
            import futu as futu_sdk
        except ImportError as exc:
            try:
                import moomoo as futu_sdk
            except ImportError:
                raise RuntimeError("未安装 futu/moomoo SDK；请安装 Futu OpenAPI SDK 并启动 OpenD，或注入自定义 provider") from exc

        ctx = self._open_trade_context(futu_sdk)
        try:
            cash = self._fetch_cash(futu_sdk, ctx)
            mmf = self._fetch_mmf(futu_sdk, ctx)
        finally:
            close = getattr(ctx, "close", None)
            if callable(close):
                close()

        return FutuBalanceSnapshot(cash=cash, mmf=mmf, currency="CNY", source="futu-openapi")

    def _fetch_cash(self, futu_sdk: Any, ctx: Any) -> Optional[float]:
        kwargs = self._accinfo_kwargs(futu_sdk)
        try:
            ret, data = ctx.accinfo_query(**kwargs)
        except TypeError:
            kwargs.pop("currency", None)
            ret, data = ctx.accinfo_query(**kwargs)
        self._ensure_ok(futu_sdk, ret, data, "accinfo_query")
        row = _first_row(data)
        for column in self.CASH_COLUMNS:
            if column in row and row[column] is not None:
                return float(row[column])
        return None

    def _fetch_mmf(self, futu_sdk: Any, ctx: Any) -> Optional[float]:
        kwargs = self._position_kwargs(futu_sdk)
        try:
            ret, data = ctx.position_list_query(**kwargs)
        except TypeError:
            kwargs["trd_market"] = kwargs.pop("position_market", None)
            ret, data = ctx.position_list_query(**kwargs)
        self._ensure_ok(futu_sdk, ret, data, "position_list_query")

        total = Decimal("0")
        for row in _rows(data):
            code = str(row.get("code") or row.get("stock_code") or "").upper()
            name = str(row.get("stock_name") or row.get("name") or "").lower()
            if not self._is_mmf_position(code, name):
                continue
            value = (
                row.get("market_val")
                or row.get("market_value")
                or row.get("nominal_price")
                or row.get("qty")
                or row.get("quantity")
            )
            if value is not None:
                total += Decimal(str(value))
        return float(total.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))

    def _open_trade_context(self, futu_sdk: Any) -> Any:
        kwargs = {"host": self.host, "port": self.port}
        trd_market = self._enum_value(futu_sdk, "TrdMarket", self.market)
        if trd_market is not None:
            kwargs["filter_trdmarket"] = trd_market
        return futu_sdk.OpenSecTradeContext(**kwargs)

    def _accinfo_kwargs(self, futu_sdk: Any) -> dict[str, Any]:
        kwargs: dict[str, Any] = {}
        kwargs["trd_env"] = self._enum_value(futu_sdk, "TrdEnv", self.trd_env)
        kwargs["currency"] = self._enum_value(futu_sdk, "Currency", self.cash_currency)
        if self.acc_id is not None:
            kwargs["acc_id"] = self.acc_id
        return kwargs

    def _position_kwargs(self, futu_sdk: Any) -> dict[str, Any]:
        kwargs: dict[str, Any] = {}
        kwargs["trd_env"] = self._enum_value(futu_sdk, "TrdEnv", self.trd_env)
        kwargs["position_market"] = self._enum_value(futu_sdk, "TrdMarket", self.market)
        if self.acc_id is not None:
            kwargs["acc_id"] = self.acc_id
        return kwargs

    @staticmethod
    def _enum_value(futu_sdk: Any, enum_name: str, value: str) -> Any:
        enum_type = getattr(futu_sdk, enum_name, None)
        return getattr(enum_type, value, value) if enum_type is not None else value

    def _is_mmf_position(self, code: str, name: str) -> bool:
        if self.mmf_codes and code in self.mmf_codes:
            return True
        return any(keyword in name for keyword in self.MMF_NAME_KEYWORDS)

    @staticmethod
    def _ensure_ok(futu_sdk: Any, ret: Any, data: Any, op: str) -> None:
        ok = getattr(futu_sdk, "RET_OK", 0)
        if ret != ok:
            raise RuntimeError(f"Futu {op} failed: {data}")


class FutuBalanceSyncService:
    MONEY_QUANT = Decimal("0.01")

    def __init__(self, storage: Any, provider: Optional[FutuBalanceProvider] = None):
        self.storage = storage
        self.provider = provider
        self.cash_service = CashService(storage)

    @classmethod
    def quantize_money(cls, value: Any) -> float:
        return float(Decimal(str(value or 0)).quantize(cls.MONEY_QUANT, rounding=ROUND_HALF_UP))

    def sync_cash_and_mmf(
        self,
        *,
        account: str,
        market: str = "富途",
        dry_run: bool = False,
        cash_balance: Optional[float] = None,
        mmf_balance: Optional[float] = None,
    ) -> dict[str, Any]:
        snapshot = (
            FutuBalanceSnapshot(cash=cash_balance, mmf=mmf_balance)
            if cash_balance is not None or mmf_balance is not None
            else self._fetch_balances()
        )

        items = []
        items.extend(self._sync_asset(
            account=account,
            market=market,
            asset_id=CASH_ASSET_ID,
            asset_name="人民币现金",
            asset_type=AssetType.CASH,
            target=snapshot.cash,
            dry_run=dry_run,
        ))
        items.extend(self._sync_asset(
            account=account,
            market=market,
            asset_id=MMF_ASSET_ID,
            asset_name="货币基金",
            asset_type=AssetType.MMF,
            target=snapshot.mmf,
            dry_run=dry_run,
        ))

        return {
            "success": True,
            "account": account,
            "market": market,
            "dry_run": dry_run,
            "source": snapshot.source,
            "items": [item.__dict__ for item in items],
            "updated": sum(1 for item in items if item.updated),
            "created": sum(1 for item in items if item.created),
        }

    def _fetch_balances(self) -> FutuBalanceSnapshot:
        provider = self.provider or FutuOpenApiBalanceProvider()
        return provider.fetch_balances()

    def _sync_asset(
        self,
        *,
        account: str,
        market: str,
        asset_id: str,
        asset_name: str,
        asset_type: AssetType,
        target: Optional[float],
        dry_run: bool,
    ) -> list[FutuBalanceSyncItem]:
        if target is None:
            return []

        synced = self.cash_service.sync_cash_like_balance(
            account=account,
            asset_id=asset_id,
            asset_name=asset_name,
            asset_type=asset_type,
            target=target,
            market=market,
            dry_run=dry_run,
        )

        return [FutuBalanceSyncItem(
            asset_id=synced["asset_id"],
            asset_name=synced["asset_name"],
            current=synced["current"],
            target=synced["target"],
            delta=synced["delta"],
            created=synced["created"],
            updated=synced["updated"],
        )]


def _env_int(name: str) -> Optional[int]:
    value = os.environ.get(name)
    return int(value) if value else None


def _rows(data: Any) -> list[dict[str, Any]]:
    if hasattr(data, "to_dict"):
        return data.to_dict("records")
    if isinstance(data, list):
        return [dict(row) for row in data]
    if isinstance(data, dict):
        return [data]
    return []


def _first_row(data: Any) -> dict[str, Any]:
    rows = _rows(data)
    return rows[0] if rows else {}
