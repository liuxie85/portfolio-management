"""Share read/calculation service."""
from __future__ import annotations

from typing import Any, Optional


class ShareService:
    def __init__(self, storage: Any):
        self.storage = storage

    def get_shares(self, account: str) -> float:
        return self.storage.get_total_shares(account)

    def calculate_shares_change(self, account: str, cny_amount: float, nav: Optional[float] = None) -> float:
        if nav is None:
            latest_nav = self.storage.get_latest_nav(account)
            nav = latest_nav.nav if latest_nav else 1.0

        if nav <= 0:
            nav = 1.0

        return cny_amount / nav
