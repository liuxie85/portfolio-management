"""Sync Futu cash-like balances into holdings."""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Iterable, Optional, Protocol

from src import config
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
    MMF_COLUMNS = ("fund_assets",)

    def __init__(
        self,
        *,
        host: Optional[str] = None,
        port: Optional[int] = None,
        trd_env: Optional[str] = None,
        acc_id: Optional[int] = None,
        trd_market: Optional[str] = None,
        cash_currency: Optional[str] = None,
        mmf_codes: Optional[Iterable[str]] = None,
    ):
        self.host = host or config.get("futu.opend.host", "127.0.0.1")
        self.port = int(port if port is not None else (config.get_int("futu.opend.port", 11111) or 11111))
        self.trd_env = trd_env or config.get("futu.trd_env", "REAL")
        self.acc_id = int(acc_id) if acc_id is not None else config.get_int("futu.acc_id")
        self.trd_market = trd_market or config.get("futu.trd_market", "HK")
        self.cash_currency = cash_currency or config.get("futu.cash_currency", "CNH")
        # Kept for constructor compatibility. MMF balance is authoritative from
        # accinfo.fund_assets, not position code matching.
        self._legacy_mmf_codes = tuple(mmf_codes or ())

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
        row = self._fetch_accinfo_row(futu_sdk, ctx)
        for column in self.CASH_COLUMNS:
            if column in row and row[column] is not None:
                return float(row[column])
        return None

    def _fetch_mmf(self, futu_sdk: Any, ctx: Any) -> Optional[float]:
        row = self._fetch_accinfo_row(futu_sdk, ctx)
        for column in self.MMF_COLUMNS:
            if column in row and row[column] is not None:
                return float(Decimal(str(row[column])).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))
        return None

    def _fetch_accinfo_row(self, futu_sdk: Any, ctx: Any) -> dict[str, Any]:
        kwargs = self._accinfo_kwargs(futu_sdk)
        try:
            ret, data = ctx.accinfo_query(**kwargs)
        except TypeError:
            kwargs.pop("currency", None)
            ret, data = ctx.accinfo_query(**kwargs)
        self._ensure_ok(futu_sdk, ret, data, "accinfo_query")
        return _first_row(data)

    def _open_trade_context(self, futu_sdk: Any) -> Any:
        kwargs = {"host": self.host, "port": self.port}
        trd_market = self._enum_value(futu_sdk, "TrdMarket", self.trd_market)
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

    @staticmethod
    def _enum_value(futu_sdk: Any, enum_name: str, value: str) -> Any:
        enum_type = getattr(futu_sdk, enum_name, None)
        return getattr(enum_type, value, value) if enum_type is not None else value

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
        broker: str = "富途",
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
            broker=broker,
            asset_id=CASH_ASSET_ID,
            asset_name="人民币现金",
            asset_type=AssetType.CASH,
            target=snapshot.cash,
            dry_run=dry_run,
        ))
        items.extend(self._sync_asset(
            account=account,
            broker=broker,
            asset_id=MMF_ASSET_ID,
            asset_name="货币基金",
            asset_type=AssetType.MMF,
            target=snapshot.mmf,
            dry_run=dry_run,
        ))

        return {
            "success": True,
            "account": account,
            "broker": broker,
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
        broker: str,
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
            broker=broker,
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
