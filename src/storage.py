"""存储后端工厂：按配置创建后端；auto 模式下若 Feishu 不可用则显式失败，避免静默分裂写入。"""
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

    # auto 模式：优先飞书；若不可用则显式失败，避免静默回退造成数据写入分裂。
    try:
        storage = FeishuStorage()
        _feishu_healthcheck(storage)
        return storage
    except Exception as e:
        raise RuntimeError(
            f"[存储] auto 模式下 Feishu 不可用，已拒绝静默回退 SQLite 以避免数据分裂。"
            f"请修复 Feishu 配置/权限，或显式设置 storage.backend=sqlite。原始错误: {e}"
        ) from e
