"""Portfolio reporting read service."""
from __future__ import annotations

from typing import Any, Dict

from src.reporting_utils import normalize_asset_type


class ReportingService:
    """Build lightweight portfolio distribution reports.

    ``manager`` is used as a compatibility facade so runtime changes to
    ``manager.price_fetcher`` and ``manager.calculate_valuation`` are respected.
    """

    def __init__(self, manager: Any, storage: Any):
        self.manager = manager
        self.storage = storage

    def get_asset_distribution(self, account: str) -> Dict[str, float]:
        valuation = self.manager.calculate_valuation(account)
        if valuation.total_value_cny == 0:
            return {}

        return {
            "现金": valuation.cash_value_cny / valuation.total_value_cny,
            "股票": valuation.stock_value_cny / valuation.total_value_cny,
            "基金": valuation.fund_value_cny / valuation.total_value_cny,
            "中国资产": valuation.cn_asset_value / valuation.total_value_cny,
            "美国资产": valuation.us_asset_value / valuation.total_value_cny,
            "港股资产": valuation.hk_asset_value / valuation.total_value_cny,
        }

    def get_industry_distribution(self, account: str) -> Dict[str, float]:
        holdings = self.storage.get_holdings(account=account)

        prices = {}
        price_fetcher = getattr(self.manager, "price_fetcher", None)
        if price_fetcher and holdings:
            name_map = {holding.asset_id: holding.asset_name for holding in holdings}
            import threading
            fetch_result = {"prices": None}

            def _fetch():
                try:
                    fetch_result["prices"] = price_fetcher.fetch_batch(
                        [holding.asset_id for holding in holdings],
                        name_map=name_map,
                        use_concurrent=True,
                        skip_us=False,
                    )
                except Exception:
                    pass

            t = threading.Thread(target=_fetch, daemon=True)
            t.start()
            t.join(timeout=30)
            if fetch_result["prices"] is not None:
                prices = fetch_result["prices"]

        industry_values = {}
        total_value = 0.0
        for holding in holdings:
            price_data = prices.get(holding.asset_id, {})
            if price_data and "cny_price" in price_data:
                cny_price = price_data["cny_price"]
            else:
                cny_price = 1.0 if holding.currency == "CNY" else None

            market_value = holding.quantity * cny_price if cny_price else 0
            industry = holding.industry.value if holding.industry else "其他"
            industry_values[industry] = industry_values.get(industry, 0) + market_value
            total_value += market_value

        if total_value == 0:
            return {}

        return {industry: value / total_value for industry, value in industry_values.items()}

    def build_position(self, snapshot: Dict[str, Any]) -> Dict[str, Any]:
        valuation = snapshot.get("valuation")
        holdings_data = snapshot.get("holdings_data") or {}
        position_data = snapshot.get("position_data") or {}

        if valuation is not None:
            total_value = valuation.total_value_cny
            stock_value = valuation.stock_value_cny
            fund_value = valuation.fund_value_cny
            cash_value = valuation.cash_value_cny
            return {
                "success": True,
                "total_value": total_value,
                "stock_value": stock_value,
                "fund_value": fund_value,
                "cash_value": cash_value,
                "stock_ratio": valuation.stock_ratio,
                "fund_ratio": valuation.fund_ratio,
                "cash_ratio": valuation.cash_ratio,
            }

        total_value = holdings_data.get("total_value", 0) or 0
        stock_value = holdings_data.get("stock_value", 0) or 0
        cash_value = holdings_data.get("cash_value", 0) or 0
        holdings = holdings_data.get("holdings") or []
        fund_value = sum((h.get("market_value") or 0) for h in holdings if h.get("normalized_type") == "fund")
        stock_value = max(0, stock_value - fund_value)

        return {
            "success": True,
            "total_value": total_value,
            "stock_value": stock_value,
            "fund_value": fund_value,
            "cash_value": cash_value,
            "stock_ratio": position_data.get("stock_ratio", stock_value / total_value if total_value > 0 else 0),
            "fund_ratio": position_data.get("fund_ratio", fund_value / total_value if total_value > 0 else 0),
            "cash_ratio": position_data.get("cash_ratio", cash_value / total_value if total_value > 0 else 0),
        }

    def build_distribution(self, snapshot: Dict[str, Any]) -> Dict[str, Any]:
        valuation = snapshot.get("valuation")
        holdings_data = snapshot.get("holdings_data") or {}
        holdings = holdings_data.get("holdings") or []

        type_dist: Dict[str, float] = {}
        market_dist: Dict[str, float] = {}
        currency_dist: Dict[str, float] = {}

        for holding in holdings:
            normalized_type = holding.get("normalized_type") or normalize_asset_type(holding.get("type"), holding.get("code", ""))
            market_value = holding.get("market_value") or 0
            type_dist[normalized_type] = type_dist.get(normalized_type, 0) + market_value

            broker = holding.get("broker") or "未指定券商"
            market_dist[broker] = market_dist.get(broker, 0) + market_value

            currency = holding.get("currency") or "CNY"
            currency_dist[currency] = currency_dist.get(currency, 0) + market_value

        total = (
            valuation.total_value_cny
            if valuation is not None
            else holdings_data.get("total_value", 0)
        ) or 0

        def sort_by_value(items_dict):
            return sorted(items_dict.items(), key=lambda x: x[1], reverse=True)

        by_market = [{"broker": k, "value": v, "ratio": v / total if total > 0 else 0} for k, v in sort_by_value(market_dist)]

        return {
            "success": True,
            "total_value": total,
            "by_type": [{"type": k, "value": v, "ratio": v / total if total > 0 else 0} for k, v in sort_by_value(type_dist)],
            "by_market": by_market,
            "by_broker": by_market,
            "by_currency": [{"currency": k, "value": v, "ratio": v / total if total > 0 else 0} for k, v in sort_by_value(currency_dist)],
        }
