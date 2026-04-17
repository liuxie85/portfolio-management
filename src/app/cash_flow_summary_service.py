"""Cash-flow aggregation read service."""
from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any


class CashFlowSummaryService:
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
    def sum_cash_flows(cls, flows) -> float:
        total = Decimal("0")
        for flow in flows:
            if flow.cny_amount:
                total += cls.to_decimal(flow.cny_amount)
        return float(total)

    def summarize(self, account: str, today: date, start_year: int, last_nav=None) -> dict:
        self.storage.preload_cash_flow_aggs(account)
        agg = self.storage.get_cash_flow_aggs(account)

        daily_map = agg.get("daily") or {}
        monthly_map = agg.get("monthly") or {}
        yearly_map = agg.get("yearly") or {}

        daily = self.to_decimal(daily_map.get(today.strftime("%Y-%m-%d"), 0.0))
        monthly = self.to_decimal(monthly_map.get(today.strftime("%Y-%m"), 0.0))

        yearly = {}
        for year in range(start_year, today.year + 1):
            year_str = str(year)
            yearly[year_str] = float(self.to_decimal(yearly_map.get(year_str, 0.0)))

        cumulative = Decimal("0")
        for day_str, amount in daily_map.items():
            parsed = self._parse_day(day_str)
            if parsed is not None and date(start_year, 1, 1) <= parsed <= today:
                cumulative += self.to_decimal(amount)

        gap = Decimal("0")
        gap_start = last_nav.date if last_nav else None
        for day_str, amount in daily_map.items():
            parsed = self._parse_day(day_str)
            if parsed is None or parsed > today:
                continue
            if gap_start is None:
                if parsed == today:
                    gap += self.to_decimal(amount)
            elif parsed > gap_start:
                gap += self.to_decimal(amount)

        return {
            "daily": float(daily),
            "monthly": float(monthly),
            "yearly": yearly,
            "cumulative": float(cumulative),
            "gap": float(gap),
        }

    def daily(self, account: str, flow_date: date) -> float:
        self.storage.preload_cash_flow_aggs(account)
        agg = self.storage.get_cash_flow_aggs(account)
        return float(self.to_decimal((agg.get("daily") or {}).get(flow_date.strftime("%Y-%m-%d"), 0.0)))

    def yearly(self, account: str, year: str) -> float:
        self.storage.preload_cash_flow_aggs(account)
        agg = self.storage.get_cash_flow_aggs(account)
        return float(self.to_decimal((agg.get("yearly") or {}).get(str(year), 0.0)))

    def monthly(self, account: str, year: int, month: int) -> float:
        self.storage.preload_cash_flow_aggs(account)
        agg = self.storage.get_cash_flow_aggs(account)
        return float(self.to_decimal((agg.get("monthly") or {}).get(f"{year:04d}-{month:02d}", 0.0)))

    def period(self, account: str, start_date: date, end_date: date) -> float:
        self.storage.preload_cash_flow_aggs(account)
        agg = self.storage.get_cash_flow_aggs(account)
        total = Decimal("0")
        for day_str, amount in (agg.get("daily") or {}).items():
            parsed = self._parse_day(day_str)
            if parsed is not None and start_date <= parsed <= end_date:
                total += self.to_decimal(amount)
        return float(total)

    @staticmethod
    def _parse_day(day_str: str):
        try:
            return datetime.strptime(day_str[:10], "%Y-%m-%d").date()
        except Exception:
            return None
