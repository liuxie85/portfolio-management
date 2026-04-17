"""Portfolio reporting read service."""
from __future__ import annotations

from typing import Any, Dict


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
            prices = price_fetcher.fetch_batch(
                [holding.asset_id for holding in holdings],
                name_map=name_map,
                use_concurrent=True,
                skip_us=False,
            )

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
