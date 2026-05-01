"""Service facade for portfolio-management use cases.

This layer is intentionally thin during the migration from Skill-first to
service-first architecture. It gives HTTP, CLI, MCP, and future workers one
application boundary while the existing skill_api facade remains compatible.
"""
from __future__ import annotations

from typing import Any, Dict, Optional


class PortfolioService:
    """Application service boundary used by HTTP and other adapters."""

    def __init__(self, backend: Optional[Any] = None):
        self._backend = backend

    @property
    def backend(self) -> Any:
        if self._backend is None:
            import skill_api

            self._backend = skill_api
        return self._backend

    def health(self) -> Dict[str, Any]:
        return {
            "success": True,
            "status": "ok",
            "service": "portfolio-management",
        }

    def list_accounts(self, *, include_default: bool = True) -> Dict[str, Any]:
        return self.backend.list_accounts(include_default=include_default)

    def multi_account_overview(
        self,
        *,
        accounts: Any = None,
        price_timeout: int = 30,
        include_details: bool = False,
    ) -> Dict[str, Any]:
        return self.backend.multi_account_overview(
            accounts=accounts,
            price_timeout=price_timeout,
            include_details=include_details,
        )

    def get_holdings(
        self,
        *,
        account: Optional[str] = None,
        include_cash: bool = True,
        group_by_market: bool = False,
        include_price: bool = False,
    ) -> Dict[str, Any]:
        return self.backend.get_holdings(
            account=account,
            include_cash=include_cash,
            group_by_market=group_by_market,
            include_price=include_price,
        )

    def get_cash(self, *, account: Optional[str] = None) -> Dict[str, Any]:
        return self.backend.get_cash(account=account)

    def get_nav(self, *, account: Optional[str] = None, days: int = 30) -> Dict[str, Any]:
        return self.backend.get_nav(account=account, days=days)

    def full_report(self, *, account: Optional[str] = None, price_timeout: int = 30) -> Dict[str, Any]:
        return self.backend.full_report(account=account, price_timeout=price_timeout)

    def generate_report(
        self,
        *,
        account: Optional[str] = None,
        report_type: str = "daily",
        price_timeout: int = 30,
    ) -> Dict[str, Any]:
        return self.backend.generate_report(
            account=account,
            report_type=report_type,
            record_nav=False,
            price_timeout=price_timeout,
        )
