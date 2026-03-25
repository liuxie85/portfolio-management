"""存储后端工厂：仅保留 Feishu 多维表作为唯一存储后端。"""
from __future__ import annotations

from typing import Optional

from . import config
from .feishu_storage import FeishuStorage


def _feishu_healthcheck(storage: FeishuStorage) -> None:
    """做一次最小化远程探活，确认资源权限可用。"""
    app_token, table_id = storage.client._get_table_config('holdings')
    storage.client._request(
        'GET',
        f'/bitable/v1/apps/{app_token}/tables/{table_id}/records',
        params={'page_size': 1},
    )


def create_storage(prefer: Optional[str] = None):
    """创建存储后端。

    兼容历史参数：prefer / storage.backend 仍可传入，但只接受 feishu/auto。
    传入 sqlite 将直接报错，避免误用。
    """
    backend = (prefer or config.get_storage_backend() or 'auto').lower()

    if backend in ('sqlite',):
        raise ValueError("SQLite 后端已移除：当前仅支持 Feishu 多维表存储")

    if backend not in ('feishu', 'auto'):
        raise ValueError(f"不支持的 storage.backend={backend}：当前仅支持 feishu/auto")

    storage = FeishuStorage()
    _feishu_healthcheck(storage)
    return storage
