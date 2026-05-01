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

_TRUE_VALUES = {"1", "true", "yes", "y", "on"}
_FALSE_VALUES = {"0", "false", "no", "n", "off"}


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
        "service.host": "PORTFOLIO_SERVICE_HOST",
        "service.port": "PORTFOLIO_SERVICE_PORT",
        "service.url": "PORTFOLIO_SERVICE_URL",
        "nav.disable_runtime_validation": "PORTFOLIO_NAV_DISABLE_RUNTIME_VALIDATION",
        "report.account_label": "PM_REPORT_ACCOUNT_LABEL",
        "report.reports_dir": "PM_REPORTS_DIR",
        "report.publish_root": "PM_PUBLISH_ROOT",
        "report.publish_base_url": "OPENCLAW_PUBLISH_BASE_URL",
        "report.sync_futu_cash_mmf": "PM_SYNC_FUTU_CASH_MMF",
        "report.sync_futu_dry_run": "PM_SYNC_FUTU_DRY_RUN",
        "report.disable_nav_runtime_validation": "PM_DISABLE_NAV_RUNTIME_VALIDATION",
        "futu.opend.host": "FUTU_OPEND_HOST",
        "futu.opend.port": "FUTU_OPEND_PORT",
        "futu.trd_env": "FUTU_TRD_ENV",
        "futu.acc_id": "FUTU_ACC_ID",
        "futu.trd_market": "FUTU_TRD_MARKET",
        "futu.cash_currency": "FUTU_CASH_CURRENCY",
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
        "feishu.tables.compensation_tasks": "FEISHU_TABLE_COMPENSATION_TASKS",
        "feishu.tables.schema_version": "FEISHU_TABLE_SCHEMA_VERSION",
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


def get_bool(key: str, default: bool = False) -> bool:
    """获取布尔配置值，支持 env/config 中常见字符串表示。"""
    value = get(key)
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0

    normalized = str(value).strip().lower()
    if normalized in _TRUE_VALUES:
        return True
    if normalized in _FALSE_VALUES or normalized == "":
        return False
    return default


def get_int(key: str, default: Optional[int] = None) -> Optional[int]:
    """获取整数配置值；缺失或无法解析时返回 default。"""
    value = get(key)
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


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
    return get_int("start_year", 2024) or 2024


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


def get_service_host() -> str:
    """获取本地 HTTP 服务监听地址。"""
    return str(get("service.host", "127.0.0.1"))


def get_service_port() -> int:
    """获取本地 HTTP 服务端口。"""
    return get_int("service.port", 8765) or 8765


def get_service_url() -> str:
    """获取本地 HTTP 服务 URL。"""
    configured = get("service.url")
    if configured:
        return str(configured).rstrip("/")
    return f"http://{get_service_host()}:{get_service_port()}"
