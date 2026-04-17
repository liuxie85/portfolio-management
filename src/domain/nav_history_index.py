"""In-memory NAV history indexing helpers."""
from __future__ import annotations

import bisect
from datetime import date


class NavHistoryIndex:
    @staticmethod
    def build(navs: list) -> dict:
        year_end_map = {}
        year_first_map = {}
        month_end_map = {}
        sorted_navs = sorted(navs, key=lambda nav: nav.date)

        for nav in sorted_navs:
            year = nav.date.year
            month_key = (nav.date.year, nav.date.month)
            year_end_map[year] = nav
            year_first_map.setdefault(year, nav)
            month_end_map[month_key] = nav

        return {
            "sorted_navs": sorted_navs,
            "dates": [nav.date for nav in sorted_navs],
            "year_end_map": year_end_map,
            "year_first_map": year_first_map,
            "month_end_map": month_end_map,
        }

    @staticmethod
    def find_latest_before(navs: list, before_date: date, nav_index: dict = None):
        if nav_index:
            idx = bisect.bisect_left(nav_index["dates"], before_date) - 1
            if idx >= 0:
                return nav_index["sorted_navs"][idx]
            return None

        candidates = [nav for nav in navs if nav.date < before_date]
        return max(candidates, key=lambda nav: nav.date) if candidates else None

    @staticmethod
    def find_year_end(navs: list, year: str, nav_index: dict = None):
        year_int = int(year)
        if nav_index:
            return nav_index["year_end_map"].get(year_int)

        year_navs = [nav for nav in navs if nav.date.year == year_int]
        return max(year_navs, key=lambda nav: nav.date) if year_navs else None

    @staticmethod
    def find_prev_month_end(navs: list, year: int, month: int, nav_index: dict = None):
        if month == 1:
            prev_year, prev_month = year - 1, 12
        else:
            prev_year, prev_month = year, month - 1

        if nav_index:
            return nav_index["month_end_map"].get((prev_year, prev_month))

        prev_month_navs = [nav for nav in navs if nav.date.year == prev_year and nav.date.month == prev_month]
        return max(prev_month_navs, key=lambda nav: nav.date) if prev_month_navs else None
