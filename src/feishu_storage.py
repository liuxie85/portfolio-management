"""
飞书多维表存储层
作为唯一存储后端（已移除 SQLite 后端）

职责拆分为 mixin 模块（src/feishu/），本文件作为组合入口。
"""
import json
import re
from dataclasses import asdict, is_dataclass
from datetime import date, datetime, timezone, timedelta
from decimal import Decimal, ROUND_HALF_UP
from typing import List, Optional, Dict, Any

from .models import (
    Holding, Transaction, CashFlow, NAVHistory, PriceCache,
    AssetType, TransactionType, AssetClass, Industry,
    make_tx_dedup_key, make_cf_dedup_key, make_request_id, DATETIME_FORMAT
)
from .snapshot_models import HoldingSnapshot
from .feishu_client import FeishuClient
from .local_cache import (
    LocalPriceCache,
    LocalHoldingsIndexCache,
    LocalNavIndexCache,
    LocalCashFlowAggCache,
)
from .feishu._price_mixin import PriceMixin
from .feishu._transactions_mixin import TransactionsMixin
from .feishu._cash_flow_mixin import CashFlowMixin
from .feishu._holdings_mixin import HoldingsMixin
from .feishu._snapshots_mixin import SnapshotsMixin
from .feishu._nav_mixin import NavMixin


class FeishuStorage(
    HoldingsMixin,
    TransactionsMixin,
    CashFlowMixin,
    SnapshotsMixin,
    NavMixin,
    PriceMixin,
):
    """飞书多维表存储层 (带内存缓存优化)"""

    FEISHU_DATE_TZ = timezone(timedelta(hours=8))
    MONEY_QUANT = Decimal('0.01')
    NAV_QUANT = Decimal('0.000001')
    WEIGHT_QUANT = Decimal('0.000001')

    def __init__(
        self,
        client: FeishuClient = None,
        local_price_cache: Optional[LocalPriceCache] = None,
        local_holdings_index_cache: Optional[LocalHoldingsIndexCache] = None,
        local_nav_index_cache: Optional[LocalNavIndexCache] = None,
        local_cash_flow_agg_cache: Optional[LocalCashFlowAggCache] = None,
    ):
        """
        初始化飞书存储层

        Args:
            client: FeishuClient 实例，如果不传则自动创建
            local_price_cache: 本地价格缓存实例（可注入用于测试）
            local_holdings_index_cache: 本地持仓索引缓存实例（可注入用于测试）
            local_nav_index_cache: 本地净值索引缓存实例（可注入用于测试）
            local_cash_flow_agg_cache: 本地现金流聚合缓存实例（可注入用于测试）
        """
        self.client = client or FeishuClient()

        # 内存缓存：减少 API 调用次数
        # key: "asset_id:account:market" -> value: record_id
        self._holding_id_cache: Dict[str, str] = {}
        # key: "asset_id:account:market" -> value: holding fields snapshot（含 record_id）
        self._holding_fields_cache: Dict[str, Dict[str, Any]] = {}
        # 持仓索引预加载状态
        self._holdings_index_loaded_all: bool = False
        self._holdings_index_loaded_accounts: set[str] = set()

        # 防重缓存：本地 Set 预检，避免重复 API 查询
        # key: request_id/dedup_key -> value: record_id (或 True 表示已存在)
        self._request_id_cache: Dict[str, str] = {}  # transactions 表
        self._dedup_key_cache: Dict[str, str] = {}   # transactions 和 cash_flow 表

        # 本地文件价格缓存（替代飞书多维表）
        self._local_price_cache = local_price_cache or LocalPriceCache()

        # 本地持仓索引缓存（business_key -> fields）
        self._local_holdings_index_cache = local_holdings_index_cache or LocalHoldingsIndexCache()

        # 本地 NAV 索引缓存（account -> nav index + bases）
        self._local_nav_index_cache = local_nav_index_cache or LocalNavIndexCache()
        self._nav_index_loaded_accounts: set[str] = set()
        self._nav_index_mem_cache: Dict[str, Dict[str, Any]] = {}

        # 本地 cash_flow 聚合缓存（account -> monthly/yearly/cumulative）
        self._local_cash_flow_agg_cache = local_cash_flow_agg_cache or LocalCashFlowAggCache()
        self._cash_flow_agg_loaded_accounts: set[str] = set()
        self._cash_flow_agg_mem_cache: Dict[str, Dict[str, Any]] = {}

        self._load_persistent_holdings_index()

    # ========== 字段转换工具（所有 mixin 共用） ==========

    @staticmethod
    def _to_decimal(v: Any) -> Decimal:
        if v is None:
            return Decimal('0')
        if isinstance(v, Decimal):
            return v
        return Decimal(str(v))

    @classmethod
    def _quantize_money(cls, v: Any) -> float:
        return float(cls._to_decimal(v).quantize(cls.MONEY_QUANT, rounding=ROUND_HALF_UP))

    @classmethod
    def _quantize_nav(cls, v: Any) -> float:
        return float(cls._to_decimal(v).quantize(cls.NAV_QUANT, rounding=ROUND_HALF_UP))

    @classmethod
    def _quantize_weight(cls, v: Any) -> float:
        return float(cls._to_decimal(v).quantize(cls.WEIGHT_QUANT, rounding=ROUND_HALF_UP))

    @classmethod
    def _normalize_numeric_field(cls, table: str, key: str, value: Any) -> Any:
        if value is None:
            return None

        money_fields = {
            'holdings': {'avg_cost'},
            'transactions': {'price', 'amount', 'fee', 'tax'},
            'cash_flow': {'amount', 'cny_amount'},
            'nav_history': {
                'total_value', 'cash_value', 'stock_value', 'fund_value',
                'cn_stock_value', 'us_stock_value', 'hk_stock_value',
                'shares', 'cash_flow', 'share_change', 'pnl', 'mtd_pnl', 'ytd_pnl'
            },
            'price_cache': {'price', 'cny_price', 'change', 'change_pct', 'exchange_rate'},
        }
        nav_fields = {
            'nav_history': {'nav', 'mtd_nav_change', 'ytd_nav_change'},
        }
        weight_fields = {
            'nav_history': {'stock_weight', 'cash_weight'},
        }

        if key in money_fields.get(table, set()):
            return cls._quantize_money(value)
        if key in nav_fields.get(table, set()):
            return cls._quantize_nav(value)
        if key in weight_fields.get(table, set()):
            return cls._quantize_weight(value)
        return value

    # ========== 字段转换工具 ==========

    def _to_feishu_fields(self, data: Dict, table: str, preserve_none: bool = False) -> Dict[str, Any]:
        """
        将 Python 字典转换为飞书多维表字段格式

        飞书字段类型：
        - 文本：直接传字符串
        - 数字：直接传数字
        - 日期：传整数时间戳（毫秒）或字符串 "2025-03-12"
        - 复选框：传布尔值

        Args:
            preserve_none: 是否保留 None 值（用于 update 时显式清空字段）
        """
        result = {}

        # 定义各表的数字字段类型映射
        # True = 数字类型, False = 文本类型
        table_number_fields = {
            'holdings': {
                'quantity': True,
                'avg_cost': True,
            },
            'transactions': {
                # NOTE: Feishu transactions table stores numeric fields as Number.
                'quantity': True,
                'price': True,
                'amount': True,
                'fee': True,
                # 'tax' 字段飞书表中可能不存在，作为可选字段处理
            },
            'cash_flow': {
                # NOTE: Feishu cash_flow table stores numeric fields as Number.
                'amount': True,
                'cny_amount': True,
                'exchange_rate': True,
            },
            'holdings_snapshot': {
                'quantity': True,
                'avg_cost': True,
                'price': True,
                'cny_price': True,
                'market_value_cny': True,
            },
            'nav_history': {
                'total_value': True,
                'cash_value': True,
                'stock_value': True,
                'fund_value': True,
                'cn_stock_value': True,
                'us_stock_value': True,
                'hk_stock_value': True,
                'stock_weight': True,
                'cash_weight': True,
                'shares': True,
                'nav': True,
                'cash_flow': True,
                'share_change': True,
                'mtd_nav_change': True,
                'ytd_nav_change': True,
                'pnl': True,
                'mtd_pnl': True,
                'ytd_pnl': True,
            },
            'price_cache': {
                'price': True,
                'cny_price': True,
                'change': True,
                'change_pct': True,
                'exchange_rate': True,
            }
        }

        num_fields_config = table_number_fields.get(table, {})

        for key, value in data.items():
            if value is None:
                if preserve_none:
                    result[key] = None
                continue

            # asset_id 特殊处理：强制转为字符串，确保前导零不丢失
            if key == 'asset_id' and value:
                result[key] = str(value)
                continue

            # 日期转换：飞书日期字段使用 Unix 时间戳（毫秒）或字符串（取决于表字段类型）
            if isinstance(value, datetime):
                result[key] = int(value.timestamp() * 1000)
            elif isinstance(value, date):
                # transactions.tx_date is Text in Feishu; use YYYY-MM-DD to match schema.
                if table == 'transactions' and key == 'tx_date':
                    result[key] = value.strftime('%Y-%m-%d')
                else:
                    # Interpret date as business date in FEISHU_DATE_TZ (Beijing) to avoid cross-day drift.
                    dt = datetime.combine(value, datetime.min.time(), tzinfo=self.FEISHU_DATE_TZ)
                    result[key] = int(dt.timestamp() * 1000)
            # 枚举转换
            elif isinstance(value, (AssetType, TransactionType, AssetClass, Industry)):
                result[key] = value.value
            # JSON 字段
            elif key in ['tag', 'details'] and isinstance(value, (list, dict)):
                result[key] = json.dumps(value, ensure_ascii=False)
            # 数字字段类型处理
            elif key in num_fields_config:
                normalized_value = self._normalize_numeric_field(table, key, value)
                if num_fields_config[key]:
                    # 数字类型：直接传数字
                    result[key] = normalized_value
                else:
                    # 文本类型：转换为字符串
                    result[key] = str(normalized_value)
            # 其他直接传
            else:
                result[key] = value

        return result

    def _from_feishu_fields(self, fields: Dict, table: str) -> Dict[str, Any]:
        """将飞书字段格式转换为 Python 字典"""
        result = {}

        for key, value in fields.items():
            if value is None:
                result[key] = None
                continue

            # asset_id 特殊处理：强制转为字符串，保留前导零
            if key == 'asset_id' and value:
                # 飞书可能返回数字类型，需要转为字符串并保持格式
                asset_id_str = str(value)
                # 如果原值是数字类型被转为字符串且长度小于6位，可能是前导零丢失的A股/基金代码
                # 但这里无法确定原始长度，所以只能保留飞书实际存储的值
                result[key] = asset_id_str
                continue

            # 根据表名和字段名做类型转换
            if table == 'holdings':
                if key == 'quantity' and value is not None and value != '':
                    result[key] = self._parse_float(value) or 0.0
                elif key == 'avg_cost' and value is not None and value != '':
                    parsed = self._parse_float(value)
                    result[key] = self._normalize_numeric_field(table, key, parsed) if parsed is not None else None
                elif key == 'tag' and value:
                    try:
                        result[key] = json.loads(value) if isinstance(value, str) else value
                    except (json.JSONDecodeError, TypeError, ValueError):
                        result[key] = []
                else:
                    result[key] = value

            elif table == 'transactions':
                if key in ['quantity', 'price', 'amount', 'fee', 'tax'] and value is not None and value != '':
                    parsed = self._parse_float(value)
                    result[key] = self._normalize_numeric_field(table, key, parsed) if parsed is not None else None
                elif key == 'tx_date' and value:
                    result[key] = value  # 保持字符串，模型会解析
                else:
                    result[key] = value

            elif table == 'cash_flow':
                if key in ['amount', 'cny_amount', 'exchange_rate'] and value is not None and value != '':
                    parsed = self._parse_float(value)
                    result[key] = self._normalize_numeric_field(table, key, parsed) if parsed is not None else None
                else:
                    result[key] = value

            elif table == 'nav_history':
                # nav_history numeric fields: do NOT manufacture zeros.
                # total_value is required; breakdown fields may be missing in legacy history.
                nav_must_fields = {'total_value'}
                nav_breakdown_fields = {
                    'cash_value', 'stock_value', 'fund_value',
                    'cn_stock_value', 'us_stock_value', 'hk_stock_value'
                }
                nav_optional_numeric_fields = {
                    'stock_weight', 'cash_weight', 'shares', 'nav',
                    'cash_flow', 'share_change',
                    'mtd_nav_change', 'ytd_nav_change',
                    'pnl', 'mtd_pnl', 'ytd_pnl'
                }

                if key in nav_must_fields:
                    parsed = self._parse_float(value)
                    result[key] = self._normalize_numeric_field(table, key, parsed) if parsed is not None else None
                elif key in nav_breakdown_fields:
                    parsed = self._parse_float(value)
                    result[key] = self._normalize_numeric_field(table, key, parsed) if parsed is not None else None
                elif key in nav_optional_numeric_fields:
                    # 关键可选数值字段严禁把空值偷偷补成 0.0；None 和 0 语义不同
                    parsed = self._parse_float(value)
                    result[key] = self._normalize_numeric_field(table, key, parsed) if parsed is not None else None
                elif key == 'details' and value:
                    try:
                        result[key] = json.loads(value) if isinstance(value, str) else value
                    except (json.JSONDecodeError, TypeError, ValueError):
                        result[key] = None
                else:
                    result[key] = value

            elif table == 'price_cache':
                if key in ['price', 'cny_price', 'change', 'change_pct', 'exchange_rate'] and value is not None and value != '':
                    parsed = self._parse_float(value)
                    result[key] = self._normalize_numeric_field(table, key, parsed) if parsed is not None else None
                else:
                    result[key] = value

            else:
                result[key] = value

        return result

    # ========== 安全辅助方法 ==========

    @staticmethod
    def _parse_float(value) -> Optional[float]:
        """解析飞书返回的数字字段，支持逗号分隔符、货币符号、括号负数

        Examples:
            '3,000.00' -> 3000.0
            '¥ 50,000.00' -> 50000.0
            '¥ (209,965.97)' -> -209965.97
            1234.5 -> 1234.5
        """
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return float(value)
        if not isinstance(value, str):
            return None
        s = value.strip()
        if not s:
            return None
        # 检测括号负数格式
        negative = bool(re.search(r'\(.*\)', s))
        # 移除货币符号、空格、括号
        s = re.sub(r'[¥$€£\s()]', '', s)
        # 移除逗号
        s = s.replace(',', '')
        if not s:
            return None
        try:
            result = float(s)
            return -result if negative else result
        except ValueError:
            return None

    @staticmethod
    def _escape_filter_value(value: str) -> str:
        r"""
        转义飞书 filter 字符串中的特殊字符，防止注入攻击

        飞书 filter 使用双引号包裹字符串值，需要转义:
        - 双引号 " -> \"
        - 反斜杠 \ -> \\
        """
        if not isinstance(value, str):
            value = str(value)
        return value.replace('\\', '\\\\').replace('"', '\\"')

    def _date_to_timestamp_ms(self, d: date) -> int:
        """将业务 date 转换为 Unix 时间戳（毫秒），用于飞书日期字段过滤。

        按业务语义，date 解释为北京时间(UTC+8)的 00:00。
        """
        dt = datetime.combine(d, datetime.min.time(), tzinfo=self.FEISHU_DATE_TZ)
        return int(dt.timestamp() * 1000)

    @staticmethod
    def _safe_date_str(d: Optional[date]) -> Optional[str]:
        if not d:
            return None
        return d.strftime('%Y-%m-%d')

    def _extract_updated_at_str(self, fields: Dict[str, Any]) -> Optional[str]:
        raw = fields.get('updated_at')
        if raw is None:
            return None
        if isinstance(raw, (int, float)):
            dt = datetime.fromtimestamp(raw / 1000, tz=self.FEISHU_DATE_TZ)
            return dt.replace(tzinfo=None).strftime(DATETIME_FORMAT)
        if isinstance(raw, str):
            return raw
        return None


    # ========== compensation_tasks ==========

    def add_compensation_task(self, task) -> Any:
        """persist compensation task (optional table).

        If Feishu table is not configured, CompensationService
        falls back to local JSONL queue.
        """
        if is_dataclass(task):
            fields = asdict(task)
        elif hasattr(task, "model_dump"):
            fields = task.model_dump(mode="json")
        else:
            fields = dict(task)

        payload = dict(fields)
        if isinstance(payload.get("payload"), (dict, list)):
            payload["payload"] = json.dumps(payload["payload"], ensure_ascii=False, sort_keys=True)
        result = self.client.create_record("compensation_tasks", payload)
        record_id = result.get("record_id")
        if isinstance(task, dict):
            task["record_id"] = record_id
        else:
            setattr(task, "record_id", record_id)
        return task
