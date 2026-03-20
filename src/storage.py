"""存储后端工厂：优先飞书，失败时优雅回退 SQLite。"""
from __future__ import annotations

from typing import Optional

from . import config
from .feishu_storage import FeishuStorage
from .sqlite_storage import SQLiteStorage


def _feishu_healthcheck(storage: FeishuStorage) -> None:
    """做一次最小化远程探活，确认资源权限可用。"""
    app_token, table_id = storage.client._get_table_config('holdings')
    storage.client._request(
        'GET',
        f'/bitable/v1/apps/{app_token}/tables/{table_id}/records',
        params={'page_size': 1},
    )


def create_storage(prefer: Optional[str] = None):
    backend = (prefer or config.get_storage_backend() or 'auto').lower()

    if backend == 'sqlite':
        return SQLiteStorage()

    if backend == 'feishu':
        return FeishuStorage()

    # auto 模式：先尝试飞书，失败则回退到 sqlite
    try:
        storage = FeishuStorage()
        _feishu_healthcheck(storage)
        return storage
    except Exception as e:
        print(f"[存储] Feishu 不可用，回退 SQLite: {e}")
        return SQLiteStorage()
