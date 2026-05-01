"""Portfolio read-model service for holdings, snapshot, and distributions."""
from __future__ import annotations

from typing import Any, Dict, Optional

from src.reporting_utils import normalize_asset_type, normalization_warning
from src.time_utils import bj_now_naive


class PortfolioReadService:
    def __init__(self, *, account: str, storage: Any, portfolio: Any, reporting_service: Any):
        self.account = account
        self.storage = storage
        self.portfolio = portfolio
        self.reporting_service = reporting_service

    def build_snapshot(self) -> Dict[str, Any]:
        valuation = self.portfolio.calculate_valuation(self.account)
        holdings = valuation.holdings or []
        holdings_list = []
        for h in holdings:
            holdings_list.append({
                "code": h.asset_id,
                "name": h.asset_name,
                "quantity": h.quantity,
                "type": h.asset_type.value if h.asset_type else None,
                "normalized_type": normalize_asset_type(h.asset_type, h.asset_id),
                "broker": h.broker,
                "currency": h.currency,
                "price": h.current_price,
                "cny_price": h.cny_price,
                "market_value": h.market_value_cny,
                "weight": h.weight,
            })
        holdings_list.sort(key=lambda x: x.get("market_value") or 0, reverse=True)

        return {
            "snapshot_time": bj_now_naive().isoformat(),
            "valuation": valuation,
            "holdings_data": {
                "success": True,
                "holdings": holdings_list,
                "count": len(holdings_list),
                "total_value": valuation.total_value_cny,
                "cash_value": valuation.cash_value_cny,
                "stock_value": valuation.stock_value_cny + valuation.fund_value_cny,
                "cash_ratio": valuation.cash_ratio,
                "warnings": valuation.warnings,
            },
            "position_data": {
                "cash_ratio": valuation.cash_ratio,
                "stock_ratio": valuation.stock_ratio,
                "fund_ratio": valuation.fund_ratio,
            },
        }

    def get_holdings(
        self,
        *,
        include_cash: bool = True,
        group_by_market: bool = False,
        include_price: bool = False,
        group_by_broker: Optional[bool] = None,
    ) -> Dict[str, Any]:
        if group_by_broker is not None:
            group_by_market = group_by_broker

        if include_price:
            snapshot = self.build_snapshot()
            holdings_data = snapshot.get("holdings_data") or {}
            result_holdings = [
                dict(h)
                for h in (holdings_data.get("holdings") or [])
                if include_cash or h.get("normalized_type") != "cash"
            ]

            result = {
                "success": True,
                "count": len(result_holdings),
                "total_value": holdings_data.get("total_value", 0),
                "cash_value": holdings_data.get("cash_value", 0),
                "stock_value": holdings_data.get("stock_value", 0),
                "cash_ratio": holdings_data.get("cash_ratio", 0),
            }
            warnings = holdings_data.get("warnings") or []
            if warnings:
                result["warnings"] = warnings

            return self._format_holdings_result(
                result=result,
                holdings=result_holdings,
                group_by_market=group_by_market,
                include_price=True,
            )

        holdings = self.storage.get_holdings(account=self.account)
        result_holdings = []
        normalization_warnings = []

        for h in holdings:
            normalized_type = normalize_asset_type(h.asset_type, h.asset_id)
            warn = normalization_warning(h.asset_type, h.asset_id)
            if warn and warn not in normalization_warnings:
                normalization_warnings.append(warn)

            if include_cash or normalized_type != "cash":
                result_holdings.append({
                    "code": h.asset_id,
                    "name": h.asset_name,
                    "quantity": h.quantity,
                    "type": h.asset_type.value if h.asset_type else None,
                    "normalized_type": normalized_type,
                    "broker": h.broker,
                    "currency": h.currency,
                })

        result = {"success": True, "count": len(result_holdings)}
        if normalization_warnings:
            result["warnings"] = [f"分类兜底: {w}" for w in normalization_warnings]

        return self._format_holdings_result(
            result=result,
            holdings=result_holdings,
            group_by_market=group_by_market,
            include_price=False,
        )

    def get_position(self, holdings_data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        snapshot = self._snapshot_from_holdings_data(holdings_data) if holdings_data is not None else self.build_snapshot()
        return self.reporting_service.build_position(snapshot)

    def get_distribution(self, holdings_data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        snapshot = self._snapshot_from_holdings_data(holdings_data) if holdings_data is not None else self.build_snapshot()
        return self.reporting_service.build_distribution(snapshot)

    @staticmethod
    def _format_holdings_result(
        *,
        result: Dict[str, Any],
        holdings: list,
        group_by_market: bool,
        include_price: bool,
    ) -> Dict[str, Any]:
        if not group_by_market:
            result["holdings"] = holdings
            return result

        by_market = {}
        for holding in holdings:
            broker = holding.get("broker") or "未指定券商"
            by_market.setdefault(broker, []).append(holding)

        if include_price:
            market_values = {
                market: sum((item.get("market_value") or 0) for item in items)
                for market, items in by_market.items()
            }
            sorted_markets = sorted(by_market.keys(), key=lambda m: market_values[m], reverse=True)
            result["by_market"] = {m: by_market[m] for m in sorted_markets}
            result["market_values"] = {m: market_values[m] for m in sorted_markets}
        else:
            result["by_market"] = by_market

        result["market_count"] = len(by_market)
        return result

    @staticmethod
    def _snapshot_from_holdings_data(holdings_data: Dict[str, Any]) -> Dict[str, Any]:
        if holdings_data is None:
            return {}
        if "holdings_data" in holdings_data or "valuation" in holdings_data:
            return holdings_data
        return {"holdings_data": holdings_data}
