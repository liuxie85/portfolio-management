"""
统一配置管理

优先级：环境变量 > config.json > 默认值
"""
import json
import os
from pathlib import Path
from typing import Optional

# 项目根目录（config.json 所在目录）
_PROJECT_ROOT = Path(__file__).parent.parent
_CONFIG_FILE = _PROJECT_ROOT / "config.json"

# 模块级缓存，避免重复读文件
_cached_config: Optional[dict] = None


def _load_config_file() -> dict:
    """从 config.json 加载配置"""
    global _cached_config
    if _cached_config is not None:
        return _cached_config

    if _CONFIG_FILE.exists():
        try:
            with open(_CONFIG_FILE, "r", encoding="utf-8") as f:
                _cached_config = json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            print(f"[配置] 加载 config.json 失败: {e}")
            _cached_config = {}
    else:
        _cached_config = {}

    return _cached_config


def reload_config():
    """强制重新加载配置（测试用）"""
    global _cached_config
    _cached_config = None
    return _load_config_file()


def get(key: str, default=None):
    """获取配置值（支持点号分隔的嵌套 key）

    Args:
        key: 配置键名，支持 'feishu.app_token' 等嵌套路径
        default: 默认值

    Returns:
        配置值
    """
    # 环境变量映射（环境变量优先）
    env_map = {
        "account": "PORTFOLIO_ACCOUNT",
        "storage.backend": "PORTFOLIO_STORAGE_BACKEND",
        "nav.disable_runtime_validation": "PORTFOLIO_NAV_DISABLE_RUNTIME_VALIDATION",
        "feishu.app_token": "FEISHU_APP_TOKEN",
        "feishu.app_id": "FEISHU_APP_ID",
        "feishu.app_secret": "FEISHU_APP_SECRET",
        "feishu.user_token": "FEISHU_USER_TOKEN",
        "feishu.tables.holdings": "FEISHU_TABLE_HOLDINGS",
        "feishu.tables.transactions": "FEISHU_TABLE_TRANSACTIONS",
        "feishu.tables.price_cache": "FEISHU_TABLE_PRICE_CACHE",
        "feishu.tables.nav_history": "FEISHU_TABLE_NAV_HISTORY",
        "feishu.tables.cash_flow": "FEISHU_TABLE_CASH_FLOW",
        "feishu.tables.holdings_snapshot": "FEISHU_TABLE_HOLDINGS_SNAPSHOT",
        "finnhub_api_key": "FINNHUB_API_KEY",
    }

    # 1. 先查环境变量
    env_key = env_map.get(key)
    if env_key:
        env_val = os.environ.get(env_key)
        if env_val:
            return env_val

    # 2. 再查 config.json（按点号拆分嵌套查找）
    cfg = _load_config_file()
    parts = key.split(".")
    node = cfg
    for part in parts:
        if isinstance(node, dict) and part in node:
            node = node[part]
        else:
            return default
    return node if node != "" else default


# ========== 常用配置的便捷访问 ==========

def get_account() -> str:
    """获取默认账户标识"""
    return get("account", "default")


def get_initial_value() -> float:
    """获取初始账户净值（净值=1 时的总资产）"""
    val = get("initial_value")
    return float(val) if val is not None else 0.0


def get_start_year() -> int:
    """获取收益统计起始年份"""
    val = get("start_year")
    return int(val) if val is not None else 2024


def get_project_root() -> Path:
    """获取项目根目录"""
    return _PROJECT_ROOT


def get_data_dir() -> Path:
    """获取数据目录（.data/）"""
    data_dir = _PROJECT_ROOT / ".data"
    data_dir.mkdir(parents=True, exist_ok=True)
    return data_dir


def get_storage_backend() -> str:
    """获取存储后端：auto | feishu（兼容历史配置；sqlite 已移除）"""
    return str(get("storage.backend", "auto")).lower()
