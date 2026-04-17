"""NAV baseline lookup service."""
from __future__ import annotations

from datetime import date, timedelta
from typing import Any, Optional

from src import config


class NavBaselineService:
    def __init__(self, storage: Any):
        self.storage = storage

    def get_last_day_nav(self, account: str, current_date: date):
        yesterday = current_date - timedelta(days=1)
        return self.storage.get_nav_on_date(account, yesterday)

    def get_initial_value(self, account: str, all_navs: list = None) -> Optional[float]:
        navs = all_navs if all_navs is not None else self.storage.get_nav_history(account, days=365 * 2)
        if not navs:
            return config.get_initial_value() or None

        earliest_nav = min(navs, key=lambda nav: nav.date)
        if earliest_nav and earliest_nav.nav and abs(earliest_nav.nav - 1.0) < 0.01:
            return earliest_nav.total_value

        return config.get_initial_value() or None
