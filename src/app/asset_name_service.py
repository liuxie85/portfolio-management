"""Asset display-name lookup service."""
from __future__ import annotations

import threading
from typing import Any


class AssetNameService:
    """Resolve full asset names with bounded latency."""

    def __init__(self, manager: Any):
        self.manager = manager

    def get_asset_name(self, asset_id: str, asset_type, user_provided_name: str = None, timeout: float = 5.0) -> str:
        result = {"data": None, "error": None}

        def fetch_name():
            try:
                result["data"] = self.manager.price_fetcher.fetch(asset_id)
            except Exception as exc:
                result["error"] = exc

        thread = threading.Thread(target=fetch_name, daemon=True)
        thread.start()
        thread.join(timeout=timeout)

        if result["data"] and result["data"].get("name"):
            return result["data"]["name"]
        if thread.is_alive():
            print(f"[警告] 获取资产名称超时 {asset_id}，使用备选名称")
        elif result["error"]:
            print(f"[警告] 获取资产名称失败 {asset_id}: {result['error']}")

        if user_provided_name:
            return user_provided_name
        return asset_id
