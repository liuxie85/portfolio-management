"""
飞书多维表存储层
作为唯一存储后端（已移除 SQLite 后端）
"""
import json
import re
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


class FeishuStorage:
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

    def _get_holding_cache_key(self, asset_id: str, account: str, market: Optional[str]) -> str:
        """生成持仓缓存 key"""
        return f"{asset_id}:{account}:{market or ''}"

    HOLDING_PROJECTION_FIELDS: List[str] = [
        'asset_id', 'asset_name', 'asset_type', 'account', 'market',
        'quantity', 'avg_cost', 'currency', 'asset_class', 'industry', 'tag',
        'created_at', 'updated_at'
    ]

    NAV_INDEX_PROJECTION_FIELDS: List[str] = [
        'date', 'account', 'total_value', 'shares', 'nav',
        'cash_flow', 'pnl', 'mtd_nav_change', 'ytd_nav_change',
        'mtd_pnl', 'ytd_pnl', 'updated_at',
    ]

    CASH_FLOW_PROJECTION_FIELDS: List[str] = [
        'flow_date', 'account', 'amount', 'currency', 'cny_amount',
        'exchange_rate', 'flow_type', 'updated_at',
    ]

    def _snapshot_for_persistent_cache(self, holding: Holding) -> Dict[str, Any]:
        return {
            'record_id': holding.record_id,
            'quantity': holding.quantity,
            'asset_type': holding.asset_type.value if holding.asset_type else None,
            'asset_name': holding.asset_name,
            'currency': holding.currency,
            'avg_cost': holding.avg_cost,
            'updated_at': holding.updated_at.strftime(DATETIME_FORMAT) if holding.updated_at else None,
        }

    def _load_persistent_holdings_index(self):
        """从本地文件加载持仓索引并预热内存缓存。"""
        items = self._local_holdings_index_cache.load_all()
        for cache_key, item in items.items():
            record_id = item.get('record_id')
            if not record_id:
                continue
            parts = cache_key.split(':', 2)
            if len(parts) != 3:
                continue
            asset_id, account, market = parts
            self._holding_id_cache[cache_key] = record_id
            self._holding_fields_cache[cache_key] = {
                'record_id': record_id,
                'asset_id': asset_id,
                'asset_name': item.get('asset_name') or '',
                'asset_type': item.get('asset_type') or AssetType.OTHER.value,
                'account': account,
                'market': market,
                'quantity': float(item.get('quantity', 0) or 0),
                'avg_cost': item.get('avg_cost'),
                'currency': item.get('currency') or 'CNY',
                'asset_class': None,
                'industry': None,
                'tag': [],
                'created_at': None,
                'updated_at': item.get('updated_at'),
            }

    def _flush_persistent_holdings_index(self):
        self._local_holdings_index_cache.flush()

    def _invalidate_holding_cache_by_record_id(self, record_id: str, *, flush_persistent: bool = False):
        if not record_id:
            return
        for cache_key, fields in list(self._holding_fields_cache.items()):
            if (fields or {}).get('record_id') != record_id:
                continue
            self._holding_fields_cache.pop(cache_key, None)
            self._holding_id_cache.pop(cache_key, None)
            self._local_holdings_index_cache.delete(cache_key, _flush=flush_persistent)

    def _invalidate_holding_cache(self, asset_id: str, account: str, market: Optional[str], *, flush_persistent: bool = False):
        """清除持仓缓存（内存 + 持久化）"""
        cache_key = self._get_holding_cache_key(asset_id, account, market)
        self._holding_id_cache.pop(cache_key, None)
        self._holding_fields_cache.pop(cache_key, None)
        self._local_holdings_index_cache.delete(cache_key, _flush=flush_persistent)

    def _put_holding_cache(self, holding: Holding, *, flush_persistent: bool = False):
        """写入单条持仓到内存缓存（record_id + 字段快照 + 本地持久化索引）"""
        if not holding or not holding.record_id:
            return
        cache_key = self._get_holding_cache_key(holding.asset_id, holding.account, holding.market)
        self._holding_id_cache[cache_key] = holding.record_id
        self._holding_fields_cache[cache_key] = {
            'record_id': holding.record_id,
            'asset_id': holding.asset_id,
            'asset_name': holding.asset_name,
            'asset_type': holding.asset_type.value if holding.asset_type else None,
            'account': holding.account,
            'market': holding.market or '',
            'quantity': holding.quantity,
            'avg_cost': holding.avg_cost,
            'currency': holding.currency,
            'asset_class': holding.asset_class.value if holding.asset_class else None,
            'industry': holding.industry.value if holding.industry else None,
            'tag': holding.tag or [],
            'created_at': holding.created_at.strftime(DATETIME_FORMAT) if holding.created_at else None,
            'updated_at': holding.updated_at.strftime(DATETIME_FORMAT) if holding.updated_at else None,
        }
        self._local_holdings_index_cache.upsert(
            cache_key,
            self._snapshot_for_persistent_cache(holding),
            _flush=flush_persistent,
        )

    def _get_holding_from_cache(self, asset_id: str, account: str, market: Optional[str]) -> Optional[Holding]:
        """从内存缓存读取单条持仓"""
        cache_key = self._get_holding_cache_key(asset_id, account, market)
        cached = self._holding_fields_cache.get(cache_key)
        if not cached:
            return None
        return self._dict_to_holding(dict(cached))

    def _get_holding_from_cache_any_market(self, asset_id: str, account: str) -> Optional[Holding]:
        """market 未指定时，从缓存中匹配任意 market（优先空 market）。"""
        preferred_key = self._get_holding_cache_key(asset_id, account, None)
        preferred = self._holding_fields_cache.get(preferred_key)
        if preferred:
            return self._dict_to_holding(dict(preferred))

        prefix = f"{asset_id}:{account}:"
        for key, cached in self._holding_fields_cache.items():
            if key.startswith(prefix):
                return self._dict_to_holding(dict(cached))
        return None

    def preload_holdings_index(self, account: Optional[str] = None) -> Dict[str, Any]:
        """预加载 holdings 索引与字段快照到内存。

        - 一次 list_records（可选 account 过滤）
        - 构建 business_key=(asset_id,account,market)->record_id
        - 同步构建字段快照，供 get_holding / upsert_holding / update_holding_quantity 复用
        """
        filter_str = None
        if account:
            filter_str = f'CurrentValue.[account] = "{self._escape_filter_value(account)}"'

        records = self.client.list_records(
            'holdings',
            filter_str=filter_str,
            field_names=self.HOLDING_PROJECTION_FIELDS,
        )

        # 先收集本次预加载的完整 key 集，用于清理已删除的旧缓存
        loaded_keys: set[str] = set()
        loaded_count = 0
        for record in records:
            fields = self._from_feishu_fields(record.get('fields') or {}, 'holdings')
            fields['record_id'] = record['record_id']
            holding = self._dict_to_holding(fields)
            cache_key = self._get_holding_cache_key(holding.asset_id, holding.account, holding.market)
            loaded_keys.add(cache_key)
            self._put_holding_cache(holding)
            loaded_count += 1

        # 清理 scope 内已不存在的旧缓存，避免脏 record_id 长期残留
        stale_keys: List[str] = []
        if account:
            for cache_key in list(self._holding_fields_cache.keys()):
                parts = cache_key.split(':', 2)
                if len(parts) != 3:
                    continue
                if parts[1] == account and cache_key not in loaded_keys:
                    stale_keys.append(cache_key)
        else:
            for cache_key in list(self._holding_fields_cache.keys()):
                if cache_key not in loaded_keys:
                    stale_keys.append(cache_key)

        for cache_key in stale_keys:
            parts = cache_key.split(':', 2)
            if len(parts) == 3:
                self._invalidate_holding_cache(parts[0], parts[1], parts[2])

        if loaded_count or stale_keys:
            self._flush_persistent_holdings_index()

        if account:
            self._holdings_index_loaded_accounts.add(account)
        else:
            self._holdings_index_loaded_all = True

        return {
            'account': account,
            'loaded': loaded_count,
            'scope': 'account' if account else 'all',
        }

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
                    except:
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
                    except:
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

    @staticmethod
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

    # ========== holdings 持仓操作 ==========

    def get_holding(self, asset_id: str, account: str, market: Optional[str] = None) -> Optional[Holding]:
        """获取单个持仓（优先使用内存索引与快照）"""
        # 1) 先查内存快照
        cached_holding = self._get_holding_from_cache(asset_id, account, market)
        if not cached_holding and market is None:
            cached_holding = self._get_holding_from_cache_any_market(asset_id, account)
        if cached_holding:
            return cached_holding

        # 2) 未命中时，优先预加载 account 级索引（单账户写入场景最常见）
        if account and (not self._holdings_index_loaded_all) and (account not in self._holdings_index_loaded_accounts):
            self.preload_holdings_index(account=account)
            cached_holding = self._get_holding_from_cache(asset_id, account, market)
            if not cached_holding and market is None:
                cached_holding = self._get_holding_from_cache_any_market(asset_id, account)
            if cached_holding:
                return cached_holding

        # 3) 若当前 account 已完成预加载仍未命中，可直接判定不存在，避免重复 list_records
        if self._holdings_index_loaded_all or (account in self._holdings_index_loaded_accounts):
            return None

        # 4) 回退到 API 精确查询（仅必要字段）
        if market:
            filter_str = (
                f'CurrentValue.[asset_id] = "{self._escape_filter_value(asset_id)}" '
                f'AND CurrentValue.[account] = "{self._escape_filter_value(account)}" '
                f'AND CurrentValue.[market] = "{self._escape_filter_value(market)}"'
            )
        else:
            filter_str = (
                f'CurrentValue.[asset_id] = "{self._escape_filter_value(asset_id)}" '
                f'AND CurrentValue.[account] = "{self._escape_filter_value(account)}"'
            )

        records = self.client.list_records(
            'holdings',
            filter_str=filter_str,
            field_names=self.HOLDING_PROJECTION_FIELDS,
        )
        if not records:
            return None

        # 不指定 market 时，优先 market 为空的记录
        selected = records[0]
        if not market:
            for record in records:
                if not (record.get('fields') or {}).get('market'):
                    selected = record
                    break

        fields = self._from_feishu_fields(selected.get('fields') or {}, 'holdings')
        fields['record_id'] = selected['record_id']
        holding = self._dict_to_holding(fields)
        self._put_holding_cache(holding)

        # 兼容旧逻辑：未指定 market 时，额外给 market='' 的查询 key 做一份映射
        if market is None and holding.market:
            default_key = self._get_holding_cache_key(asset_id, account, None)
            self._holding_id_cache[default_key] = holding.record_id
            self._holding_fields_cache[default_key] = dict(self._holding_fields_cache[self._get_holding_cache_key(asset_id, account, holding.market)])

        return holding

    def get_holdings(self, account: Optional[str] = None, asset_type: Optional[str] = None, include_empty: bool = False) -> List[Holding]:
        """获取持仓列表

        Args:
            account: 账户过滤
            asset_type: 资产类型过滤
            include_empty: 是否包含空记录（默认过滤掉）
        """
        # 飞书 API 不支持数字字段的 > 比较，只查询文本字段条件
        conditions = []

        if account:
            conditions.append(f'CurrentValue.[account] = "{account}"')
        if asset_type:
            conditions.append(f'CurrentValue.[asset_type] = "{asset_type}"')

        filter_str = ' AND '.join(conditions) if conditions else None
        records = self.client.list_records(
            'holdings',
            filter_str=filter_str,
            field_names=self.HOLDING_PROJECTION_FIELDS,
        )

        holdings = []
        for record in records:
            fields = self._from_feishu_fields(record.get('fields') or {}, 'holdings')
            fields['record_id'] = record['record_id']
            holding = self._dict_to_holding(fields)
            self._put_holding_cache(holding)

            # 在代码中过滤 quantity <= 0 的记录（除非 include_empty=True）
            if not include_empty and holding.quantity <= 0:
                continue

            holdings.append(holding)

        # 按 asset_type 和 asset_id 排序
        holdings.sort(key=lambda h: (h.asset_type.value if h.asset_type else '', h.asset_id))
        return holdings

    def upsert_holding(self, holding: Holding) -> Holding:
        """插入或更新持仓（优先使用预加载索引与内存快照）"""
        from .time_utils import bj_now_naive

        now = bj_now_naive()

        # 先读缓存快照；未命中时 get_holding 会按需触发 account 级 preload
        existing = self.get_holding(holding.asset_id, holding.account, holding.market)

        if existing and existing.record_id:
            is_cash_like = (existing.asset_type and existing.asset_type.value in ('cash', 'mmf'))
            new_quantity = (
                self._quantize_money(existing.quantity + holding.quantity)
                if is_cash_like else (existing.quantity + holding.quantity)
            )
            update_fields = {
                'quantity': new_quantity,
                'updated_at': now.strftime(DATETIME_FORMAT)
            }

            # 更新名称（如果新名称更完整）
            new_name = holding.asset_name or existing.asset_name
            if new_name and len(new_name) > len(existing.asset_name or ''):
                update_fields['asset_name'] = new_name
                print(f"[持仓名称更新] {existing.asset_name} -> {new_name}")

            try:
                self.client.update_record('holdings', existing.record_id, update_fields)
            except Exception:
                # 缓存可能脏（记录被删除/改写），立即失效
                self._invalidate_holding_cache(holding.asset_id, holding.account, holding.market, flush_persistent=True)
                raise

            # 更新返回值与缓存
            existing.quantity = new_quantity
            existing.updated_at = now
            if 'asset_name' in update_fields:
                existing.asset_name = update_fields['asset_name']

            holding.record_id = existing.record_id
            holding.updated_at = now
            self._put_holding_cache(existing)
            return holding

        # 不存在则创建
        holding.created_at = now
        holding.updated_at = now

        fields = self._holding_to_dict(holding)
        feishu_fields = self._to_feishu_fields(fields, 'holdings')

        result = self.client.create_record('holdings', feishu_fields)
        holding.record_id = result['record_id']
        self._put_holding_cache(holding)
        return holding

    def upsert_holdings_bulk(self, holdings: List[Holding], mode: str = 'additive') -> Dict[str, Any]:
        """批量 upsert 持仓，减少 HTTP 调用。

        Args:
            holdings: 待写入持仓列表
            mode:
                - additive: quantity += holding.quantity
                - replace: quantity = holding.quantity
        """
        from .time_utils import bj_now_naive

        if mode not in ('additive', 'replace'):
            raise ValueError(f"unsupported mode={mode}, expected 'additive' or 'replace'")

        if not holdings:
            return {'mode': mode, 'updated': 0, 'created': 0, 'preloaded_accounts': []}

        # additive 模式下，先为缺失缓存且未预加载的 account 做一次预加载
        preloaded_accounts: List[str] = []
        if mode == 'additive':
            accounts_to_preload = set()
            for h in holdings:
                cache_key = self._get_holding_cache_key(h.asset_id, h.account, h.market)
                has_cache = cache_key in self._holding_fields_cache
                if (not has_cache) and h.account and (not self._holdings_index_loaded_all) and (h.account not in self._holdings_index_loaded_accounts):
                    accounts_to_preload.add(h.account)
            for account in sorted(accounts_to_preload):
                self.preload_holdings_index(account=account)
                preloaded_accounts.append(account)

        now = bj_now_naive()
        now_str = now.strftime(DATETIME_FORMAT)

        update_payloads: List[Dict[str, Any]] = []
        update_targets: List[Holding] = []
        create_payloads: List[Dict[str, Any]] = []
        create_targets: List[Holding] = []

        # 同一批次内允许同 business_key 多次出现；用工作快照累加，避免每次都从旧缓存读
        working_existing: Dict[str, Holding] = {}

        for incoming in holdings:
            cache_key = self._get_holding_cache_key(incoming.asset_id, incoming.account, incoming.market)
            existing = working_existing.get(cache_key)
            if existing is None:
                existing = self.get_holding(incoming.asset_id, incoming.account, incoming.market)
                if existing:
                    working_existing[cache_key] = Holding(**existing.model_dump())
                    existing = working_existing[cache_key]

            if existing and existing.record_id:
                if mode == 'replace':
                    new_quantity = incoming.quantity
                else:
                    is_cash_like = (existing.asset_type and existing.asset_type.value in ('cash', 'mmf'))
                    new_quantity = (
                        self._quantize_money(existing.quantity + incoming.quantity)
                        if is_cash_like else (existing.quantity + incoming.quantity)
                    )

                update_fields = {
                    'quantity': new_quantity,
                    'updated_at': now_str,
                }
                new_name = incoming.asset_name or existing.asset_name
                if new_name and len(new_name) > len(existing.asset_name or ''):
                    update_fields['asset_name'] = new_name

                update_payloads.append({'record_id': existing.record_id, 'fields': update_fields})

                # 更新批次内工作快照，后续同 key 继续在此基础上累加
                existing.quantity = new_quantity
                existing.updated_at = now
                if 'asset_name' in update_fields:
                    existing.asset_name = update_fields['asset_name']
                update_targets.append(Holding(**existing.model_dump()))
            else:
                new_holding = Holding(**incoming.model_dump())
                new_holding.created_at = now
                new_holding.updated_at = now
                fields = self._holding_to_dict(new_holding)
                feishu_fields = self._to_feishu_fields(fields, 'holdings')
                create_payloads.append({'fields': feishu_fields})
                create_targets.append(new_holding)

        # 批量更新
        if update_payloads:
            try:
                self.client.batch_update_records('holdings', update_payloads)
            except Exception:
                # 批量更新失败，相关缓存可能已脏，全部失效
                for h in update_targets:
                    self._invalidate_holding_cache(h.asset_id, h.account, h.market)
                self._flush_persistent_holdings_index()
                raise
            for h in update_targets:
                self._put_holding_cache(h)

        # 批量创建
        if create_payloads:
            created_records = self.client.batch_create_records('holdings', create_payloads)
            for idx, h in enumerate(create_targets):
                rec = created_records[idx] if idx < len(created_records) else {}
                h.record_id = rec.get('record_id') or (rec.get('record') or {}).get('record_id')
                if h.record_id:
                    self._put_holding_cache(h)

        # 批量路径完成后做一次集中刷盘
        if update_payloads or create_payloads:
            self._flush_persistent_holdings_index()

        return {
            'mode': mode,
            'updated': len(update_payloads),
            'created': len(create_payloads),
            'preloaded_accounts': preloaded_accounts,
        }

    def update_holding_quantity(self, asset_id: str, account: str, quantity_change: float, market: Optional[str] = None):
        """更新持仓数量（优先使用预加载索引与内存快照）"""
        from .time_utils import bj_now_naive

        holding = self.get_holding(asset_id, account, market)
        if not holding or not holding.record_id:
            return

        is_cash_like = (holding.asset_type and holding.asset_type.value in ('cash', 'mmf'))
        new_quantity = self._quantize_money(holding.quantity + quantity_change) if is_cash_like else (holding.quantity + quantity_change)
        now_str = bj_now_naive().strftime('%Y-%m-%d %H:%M:%S')
        update_fields = {
            'quantity': new_quantity,
            'updated_at': now_str
        }
        try:
            self.client.update_record('holdings', holding.record_id, update_fields)
        except Exception:
            self._invalidate_holding_cache(asset_id, account, market, flush_persistent=True)
            raise

        # 同步内存缓存
        holding.quantity = new_quantity
        holding.updated_at = datetime.strptime(now_str, DATETIME_FORMAT)
        self._put_holding_cache(holding)

    def delete_holding_if_zero(self, asset_id: str, account: str, market: Optional[str] = None):
        """如果持仓为0则删除（容忍极小浮点残值）"""
        holding = self.get_holding(asset_id, account, market)
        if holding and holding.record_id and abs(holding.quantity) <= 1e-8:
            self.client.delete_record('holdings', holding.record_id)
            self._invalidate_holding_cache(asset_id, account, market, flush_persistent=True)

    def delete_holding_by_record_id(self, record_id: str) -> bool:
        """通过记录ID删除持仓"""
        ok = self.client.delete_record('holdings', record_id)
        if ok:
            self._invalidate_holding_cache_by_record_id(record_id, flush_persistent=True)
        return ok

    def delete_transaction_by_record_id(self, record_id: str) -> bool:
        """通过记录ID删除交易"""
        return self.client.delete_record('transactions', record_id)

    def delete_cash_flow_by_record_id(self, record_id: str) -> bool:
        """通过记录ID删除出入金"""
        ok = self.client.delete_record('cash_flow', record_id)
        if ok:
            # 删除/回填都可能破坏聚合正确性，直接失效对应内存加载状态
            self._cash_flow_agg_loaded_accounts.clear()
            self._cash_flow_agg_mem_cache.clear()
        return ok

    def delete_nav_by_record_id(self, record_id: str) -> bool:
        """通过记录ID删除净值记录"""
        ok = self.client.delete_record('nav_history', record_id)
        if ok:
            self._nav_index_loaded_accounts.clear()
            self._nav_index_mem_cache.clear()
        return ok

    def _holding_to_dict(self, holding: Holding) -> Dict:
        """Holding 转字典"""
        from .time_utils import bj_now_naive

        result = {
            'asset_id': holding.asset_id,
            'asset_name': holding.asset_name,
            'asset_type': holding.asset_type,
            'market': holding.market or '',
            'account': holding.account,
            'quantity': holding.quantity,
            'avg_cost': holding.avg_cost,
            'currency': holding.currency,
            'asset_class': holding.asset_class,
            'industry': holding.industry,
            'tag': holding.tag,
        }

        # 处理时间戳
        now = bj_now_naive()
        if holding.created_at:
            result['created_at'] = holding.created_at.strftime(DATETIME_FORMAT)
        if holding.updated_at:
            result['updated_at'] = holding.updated_at.strftime(DATETIME_FORMAT)

        return result

    def _dict_to_holding(self, data: Dict) -> Holding:
        """字典转 Holding"""
        from datetime import datetime

        # 解析时间戳
        created_at = None
        updated_at = None

        if data.get('created_at'):
            try:
                created_at = datetime.strptime(data['created_at'], DATETIME_FORMAT)
            except (ValueError, TypeError):
                pass

        if data.get('updated_at'):
            try:
                updated_at = datetime.strptime(data['updated_at'], DATETIME_FORMAT)
            except (ValueError, TypeError):
                pass

        return Holding(
            record_id=data.get('record_id'),
            asset_id=data.get('asset_id', ''),
            asset_name=data.get('asset_name', ''),
            asset_type=AssetType(data.get('asset_type')) if data.get('asset_type') else AssetType.OTHER,
            market=data.get('market') or None,
            account=data.get('account', ''),
            quantity=float(data.get('quantity', 0)),
            avg_cost=float(data.get('avg_cost')) if data.get('avg_cost') is not None else None,
            currency=data.get('currency', 'CNY'),
            asset_class=AssetClass(data.get('asset_class')) if data.get('asset_class') else None,
            industry=Industry(data.get('industry')) if data.get('industry') else None,
            tag=data.get('tag', []),
            created_at=created_at,
            updated_at=updated_at
        )

    # ========== transactions 交易记录操作 ==========

    @staticmethod
    def _is_missing_field_error(error: Exception) -> bool:
        msg = str(error)
        lowered = msg.lower()
        return (
            'fieldnamenotfound' in lowered or
            ('field' in lowered and 'not found' in lowered) or
            '字段不存在' in msg or
            '不存在' in msg
        )

    def add_transaction(self, tx: Transaction) -> Transaction:
        """添加交易记录（自动防止重复提交）

        防重机制（按优先级）：
        1. request_id: 调用方传入的幂等键
        2. dedup_key: 内容指纹，自动生成
        """
        # 自动生成 request_id / dedup_key
        # 目标：允许同一天同一资产多笔交易，同时保持幂等性可控。
        if not tx.request_id:
            tx.request_id = make_request_id(prefix="tx")
        if not tx.dedup_key:
            tx.dedup_key = make_tx_dedup_key(tx)

        # 1. request_id 幂等性检查
        if tx.request_id:
            existing = self._find_by_request_id(tx.request_id)
            if existing:
                print(f"[幂等性保护] 发现重复请求(request_id={tx.request_id})，跳过创建")
                tx.record_id = existing.record_id
                return tx

        # 2. dedup_key 内容指纹检查
        if tx.dedup_key:
            existing = self._find_by_dedup_key('transactions', tx.dedup_key)
            if existing:
                print(f"[防重保护] 发现相同内容交易(dedup_key={tx.dedup_key})，跳过创建")
                tx.record_id = existing
                return tx

        fields = self._transaction_to_dict(tx)
        feishu_fields = self._to_feishu_fields(fields, 'transactions')

        try:
            result = self.client.create_record('transactions', feishu_fields)
        except Exception as e:
            if self._is_missing_field_error(e):
                raise ValueError("Feishu transactions 表缺少 request_id/dedup_key 等幂等字段，已拒绝降级写入；请先补齐表字段") from e
            raise

        tx.record_id = result['record_id']

        # 写入防重缓存，避免后续重复查询
        if tx.request_id:
            self._request_id_cache[tx.request_id] = tx.record_id
        if tx.dedup_key:
            self._dedup_key_cache[f"transactions:{tx.dedup_key}"] = tx.record_id

        return tx

    def _find_by_request_id(self, request_id: str) -> Optional[Transaction]:
        """通过 request_id 查找交易记录（用于幂等性检查，带本地缓存）"""
        if not request_id:
            return None

        # 1. 本地缓存预检（避免重复 API 调用）
        cached_record_id = self._request_id_cache.get(request_id)
        if cached_record_id:
            # 缓存命中，直接查询记录详情
            try:
                record = self.client.get_record_strict('transactions', cached_record_id)
                fields = self._from_feishu_fields(record['fields'], 'transactions')
                fields['record_id'] = record['record_id']
                return self._dict_to_transaction(fields)
            except Exception:
                # 缓存记录可能已删除，清除缓存后回退到查询模式
                self._request_id_cache.pop(request_id, None)

        # 2. 缓存未命中，发起 API 查询
        filter_str = f'CurrentValue.[request_id] = "{self._escape_filter_value(request_id)}"'
        try:
            records = self.client.list_records('transactions', filter_str=filter_str)
            if records:
                record_id = records[0]['record_id']
                # 写入本地缓存
                self._request_id_cache[request_id] = record_id
                fields = self._from_feishu_fields(records[0]['fields'], 'transactions')
                fields['record_id'] = record_id
                return self._dict_to_transaction(fields)
        except Exception as e:
            if self._is_missing_field_error(e):
                raise ValueError("Feishu transactions 表缺少 request_id 字段，无法保证幂等性；请先补齐表字段") from e
            print(f"[警告] 幂等性检查失败: {e}")

        return None

    def _find_by_dedup_key(self, table: str, dedup_key: str) -> Optional[str]:
        """通过 dedup_key 查找记录（用于内容指纹防重，带本地缓存）

        Returns:
            record_id if found, else None
        """
        if not dedup_key:
            return None

        # 1. 本地缓存预检（避免重复 API 调用）
        cache_key = f"{table}:{dedup_key}"
        cached_record_id = self._dedup_key_cache.get(cache_key)
        if cached_record_id:
            # 缓存命中，验证记录是否仍存在
            try:
                record = self.client.get_record_strict(table, cached_record_id)
                if record:
                    return cached_record_id
            except Exception:
                # 缓存记录可能已删除，清除缓存后继续查询
                self._dedup_key_cache.pop(cache_key, None)

        # 2. 缓存未命中，发起 API 查询
        filter_str = f'CurrentValue.[dedup_key] = "{self._escape_filter_value(dedup_key)}"'
        try:
            records = self.client.list_records(table, filter_str=filter_str)
            if records:
                record_id = records[0]['record_id']
                # 写入本地缓存
                self._dedup_key_cache[cache_key] = record_id
                return record_id
        except Exception as e:
            if self._is_missing_field_error(e):
                raise ValueError(f"Feishu {table} 表缺少 dedup_key 字段，无法保证防重；请先补齐表字段") from e
            raise

        return None

    def get_transaction(self, record_id: str) -> Optional[Transaction]:
        """获取单条交易记录（通过 record_id）"""
        try:
            record = self.client.get_record_strict('transactions', record_id)
        except Exception:
            return None

        fields = self._from_feishu_fields(record['fields'], 'transactions')
        fields['record_id'] = record['record_id']
        return self._dict_to_transaction(fields)

    def get_transactions(self, account: Optional[str] = None,
                        start_date: Optional[date] = None,
                        end_date: Optional[date] = None,
                        tx_type: Optional[str] = None) -> List[Transaction]:
        """获取交易记录列表"""
        conditions = []

        if account:
            conditions.append(f'CurrentValue.[account] = "{account}"')
        if tx_type:
            conditions.append(f'CurrentValue.[tx_type] = "{tx_type}"')

        filter_str = ' AND '.join(conditions) if conditions else None
        records = self.client.list_records('transactions', filter_str=filter_str)

        transactions = []
        for record in records:
            fields = self._from_feishu_fields(record['fields'], 'transactions')
            fields['record_id'] = record['record_id']
            tx = self._dict_to_transaction(fields)
            # 飞书日期字段不支持比较操作符，客户端过滤
            if start_date and tx.tx_date and tx.tx_date < start_date:
                continue
            if end_date and tx.tx_date and tx.tx_date > end_date:
                continue
            transactions.append(tx)

        # 按日期倒序
        transactions.sort(key=lambda t: t.tx_date, reverse=True)
        return transactions

    def _transaction_to_dict(self, tx: Transaction) -> Dict:
        """Transaction 转字典"""
        # NOTE: keep this dict aligned with Feishu schema (see schema audit).
        result = {
            'tx_date': tx.tx_date,
            'tx_type': tx.tx_type,
            'asset_id': tx.asset_id,
            'asset_name': tx.asset_name,
            'asset_type': tx.asset_type,
            'market': tx.market,
            'account': tx.account,
            'quantity': tx.quantity,
            'price': tx.price,
            'amount': tx.amount,
            'currency': tx.currency,
            'fee': tx.fee,
            'remark': tx.remark,
            # Feishu schema fields (optional)
            'request_id': tx.request_id,
            'dedup_key': tx.dedup_key,
        }
        # Filter out empty values to reduce FieldNameNotFound risk and keep Feishu clean.
        return {k: v for k, v in result.items() if v is not None and v != ''}

    def _dict_to_transaction(self, data: Dict) -> Transaction:
        """字典转 Transaction"""
        tx_date = data.get('tx_date')
        if isinstance(tx_date, (int, float)):
            tx_date = datetime.fromtimestamp(tx_date / 1000, tz=self.FEISHU_DATE_TZ).date()
        elif isinstance(tx_date, str):
            tx_date = datetime.strptime(tx_date, '%Y-%m-%d').date()

        return Transaction(
            record_id=data.get('record_id'),
            request_id=data.get('request_id'),
            tx_date=tx_date,
            tx_type=TransactionType(data.get('tx_type')) if data.get('tx_type') else TransactionType.BUY,
            asset_id=data.get('asset_id', ''),
            asset_name=data.get('asset_name'),
            asset_type=AssetType(data.get('asset_type')) if data.get('asset_type') else None,
            market=data.get('market'),
            account=data.get('account', ''),
            quantity=float(data.get('quantity', 0)),
            price=float(data.get('price', 0)),
            amount=float(data.get('amount')) if data.get('amount') is not None else None,
            currency=data.get('currency', 'CNY'),
            fee=float(data.get('fee', 0)),
            tax=float(data.get('tax', 0)),
            related_account=data.get('related_account'),
            remark=data.get('remark'),
            source=data.get('source', 'manual')
        )

    # ========== cash_flow 出入金操作 ==========

    def add_cash_flow(self, cf: CashFlow) -> CashFlow:
        """添加出入金记录（自动防重）"""
        # 自动生成 dedup_key
        if not cf.dedup_key:
            cf.dedup_key = make_cf_dedup_key(cf)

        # dedup_key 内容指纹检查
        if cf.dedup_key:
            existing = self._find_by_dedup_key('cash_flow', cf.dedup_key)
            if existing:
                print(f"[防重保护] 发现相同内容出入金(dedup_key={cf.dedup_key})，跳过创建")
                cf.record_id = existing
                return cf

        fields = self._cash_flow_to_dict(cf)
        feishu_fields = self._to_feishu_fields(fields, 'cash_flow')

        try:
            result = self.client.create_record('cash_flow', feishu_fields)
        except Exception as e:
            if self._is_missing_field_error(e):
                raise ValueError("Feishu cash_flow 表缺少 dedup_key 等防重字段，已拒绝降级写入；请先补齐表字段") from e
            raise
        cf.record_id = result['record_id']

        # 写入防重缓存，避免后续重复查询
        if cf.dedup_key:
            self._dedup_key_cache[f"cash_flow:{cf.dedup_key}"] = cf.record_id

        # 增量更新本地 cash_flow 聚合缓存（若已加载该账户）
        if cf.account in self._cash_flow_agg_loaded_accounts and cf.flow_date:
            from .time_utils import bj_now_naive
            cny_amount = cf.cny_amount if cf.cny_amount is not None else cf.amount
            self._local_cash_flow_agg_cache.append_flow(
                cf.account,
                cf.flow_date,
                float(cny_amount or 0.0),
                cf.record_id,
                bj_now_naive().strftime(DATETIME_FORMAT),
            )
            # 重新读取，确保内存与持久化一致
            self._cash_flow_agg_mem_cache[cf.account] = self._local_cash_flow_agg_cache.get_account(cf.account)

        return cf

    def get_cash_flow(self, record_id: str) -> Optional[CashFlow]:
        """获取单条出入金记录"""
        try:
            record = self.client.get_record_strict('cash_flow', record_id)
        except Exception:
            return None

        fields = self._from_feishu_fields(record['fields'], 'cash_flow')
        fields['record_id'] = record['record_id']
        return self._dict_to_cash_flow(fields)

    def preload_cash_flow_aggs(self, account: str, force_refresh: bool = False) -> Dict[str, Any]:
        """预加载并缓存 cash_flow 月度/年度聚合。"""
        if (not force_refresh) and (account in self._cash_flow_agg_loaded_accounts):
            cached = self._cash_flow_agg_mem_cache.get(account) or {}
            return {
                'account': account,
                'loaded': int(cached.get('flow_count', 0) or 0),
                'source': 'memory',
                'invalidated': False,
            }

        cached_local = self._local_cash_flow_agg_cache.get_account(account)

        filter_str = f'CurrentValue.[account] = "{self._escape_filter_value(account)}"'
        try:
            records = self.client.list_records(
                'cash_flow',
                filter_str=filter_str,
                field_names=self.CASH_FLOW_PROJECTION_FIELDS,
            )
        except Exception as e:
            if 'FieldNameNotFound' in str(e):
                fallback_fields = [f for f in self.CASH_FLOW_PROJECTION_FIELDS if f != 'updated_at']
                records = self.client.list_records(
                    'cash_flow',
                    filter_str=filter_str,
                    field_names=fallback_fields,
                )
            else:
                raise

        flows: List[Dict[str, Any]] = []
        daily: Dict[str, float] = {}
        monthly: Dict[str, float] = {}
        yearly: Dict[str, float] = {}
        cumulative = Decimal('0')

        for record in records:
            fields = self._from_feishu_fields(record.get('fields') or {}, 'cash_flow')
            cf = self._dict_to_cash_flow({**fields, 'record_id': record['record_id']})
            if not cf.flow_date:
                continue
            amount = cf.cny_amount if cf.cny_amount is not None else cf.amount
            amount_dec = self._to_decimal(amount or 0)
            amount_float = float(amount_dec)

            ds = cf.flow_date.strftime('%Y-%m-%d')
            ym = cf.flow_date.strftime('%Y-%m')
            yy = cf.flow_date.strftime('%Y')
            daily[ds] = float(self._to_decimal(daily.get(ds, 0.0)) + amount_dec)
            monthly[ym] = float(self._to_decimal(monthly.get(ym, 0.0)) + amount_dec)
            yearly[yy] = float(self._to_decimal(yearly.get(yy, 0.0)) + amount_dec)
            cumulative += amount_dec

            flows.append({
                'date': self._safe_date_str(cf.flow_date),
                'record_id': record['record_id'],
                'cny_amount': amount_float,
                'updated_at': self._extract_updated_at_str(record.get('fields') or {}),
            })

        flows.sort(key=lambda x: x.get('date') or '')
        last_record = dict(flows[-1]) if flows else None

        invalidated = False
        if cached_local:
            old_fp = {r.get('date'): (r.get('record_id'), r.get('updated_at')) for r in (cached_local.get('flows') or [])}
            new_fp = {r.get('date'): (r.get('record_id'), r.get('updated_at')) for r in flows}
            if old_fp != new_fp:
                invalidated = True

        payload = {
            'account': account,
            'daily': daily,
            'monthly': monthly,
            'yearly': yearly,
            'cumulative': float(cumulative),
            'flow_count': len(flows),
            'flows': flows,
            'last_record': last_record,
            'latest_updated_at': (last_record or {}).get('updated_at') if last_record else None,
        }

        self._cash_flow_agg_mem_cache[account] = payload
        self._cash_flow_agg_loaded_accounts.add(account)
        self._local_cash_flow_agg_cache.set_account(account, payload)

        return {'account': account, 'loaded': len(flows), 'source': 'feishu', 'invalidated': invalidated}

    def _ensure_cash_flow_aggs_loaded(self, account: str):
        if account in self._cash_flow_agg_loaded_accounts:
            return
        cached = self._local_cash_flow_agg_cache.get_account(account)
        if cached:
            self._cash_flow_agg_mem_cache[account] = cached
            self._cash_flow_agg_loaded_accounts.add(account)
            return
        self.preload_cash_flow_aggs(account)

    def get_cash_flow_aggs(self, account: str) -> Dict[str, Any]:
        self._ensure_cash_flow_aggs_loaded(account)
        return self._cash_flow_agg_mem_cache.get(account) or {}

    def get_cash_flows(self, account: Optional[str] = None,
                      start_date: Optional[date] = None,
                      end_date: Optional[date] = None) -> List[CashFlow]:
        """获取出入金记录列表（投影字段，降低 payload）。"""
        conditions = []

        if account:
            conditions.append(f'CurrentValue.[account] = "{self._escape_filter_value(account)}"')
        filter_str = ' AND '.join(conditions) if conditions else None
        try:
            records = self.client.list_records(
                'cash_flow',
                filter_str=filter_str,
                field_names=self.CASH_FLOW_PROJECTION_FIELDS,
            )
        except Exception as e:
            if 'FieldNameNotFound' in str(e):
                fallback_fields = [f for f in self.CASH_FLOW_PROJECTION_FIELDS if f != 'updated_at']
                records = self.client.list_records(
                    'cash_flow',
                    filter_str=filter_str,
                    field_names=fallback_fields,
                )
            else:
                raise

        cash_flows = []
        for record in records:
            fields = self._from_feishu_fields(record['fields'], 'cash_flow')
            fields['record_id'] = record['record_id']
            cf = self._dict_to_cash_flow(fields)
            # 飞书日期字段不支持比较操作符，客户端过滤
            if start_date and cf.flow_date and cf.flow_date < start_date:
                continue
            if end_date and cf.flow_date and cf.flow_date > end_date:
                continue
            cash_flows.append(cf)

        cash_flows.sort(key=lambda c: c.flow_date, reverse=True)
        return cash_flows

    def get_total_cash_flow_cny(self, account: str) -> float:
        """获取账户累计出入金总额(人民币)"""
        records = self.client.list_records(
            'cash_flow',
            filter_str=f'CurrentValue.[account] = "{self._escape_filter_value(account)}"'
        )

        total = Decimal('0')
        for record in records:
            fields = record['fields']
            cny_amount = fields.get('cny_amount', fields.get('amount', 0))
            if cny_amount is not None and cny_amount != '':
                total += self._to_decimal(cny_amount)

        return float(total)

    def _cash_flow_to_dict(self, cf: CashFlow) -> Dict:
        """CashFlow 转字典"""
        # Normalize flow_type to match Feishu SingleSelect options (DEPOSIT/WITHDRAW)
        flow_type = str(cf.flow_type).upper() if cf.flow_type is not None else None
        result = {
            'flow_date': cf.flow_date,
            'account': cf.account,
            'amount': cf.amount,
            'currency': cf.currency,
            'cny_amount': cf.cny_amount,
            'exchange_rate': cf.exchange_rate,
            'flow_type': flow_type,
            'source': cf.source,
            'remark': cf.remark,
        }
        if cf.dedup_key:
            result['dedup_key'] = cf.dedup_key
        return result

    def _dict_to_cash_flow(self, data: Dict) -> CashFlow:
        """字典转 CashFlow"""
        flow_date = data.get('flow_date')
        if isinstance(flow_date, (int, float)):
            flow_date = datetime.fromtimestamp(flow_date / 1000, tz=self.FEISHU_DATE_TZ).date()
        elif isinstance(flow_date, str):
            flow_date = datetime.strptime(flow_date, '%Y-%m-%d').date()

        return CashFlow(
            record_id=data.get('record_id'),
            flow_date=flow_date,
            account=data.get('account', ''),
            amount=float(data.get('amount', 0)),
            currency=data.get('currency', 'CNY'),
            cny_amount=float(data.get('cny_amount')) if data.get('cny_amount') is not None else None,
            exchange_rate=float(data.get('exchange_rate')) if data.get('exchange_rate') is not None else None,
            # Normalize flow_type to match Feishu SingleSelect options (DEPOSIT/WITHDRAW)
            flow_type=str(data.get('flow_type', 'DEPOSIT')).upper(),
            source=data.get('source'),
            remark=data.get('remark'),
        )

    # ========== holdings_snapshot 快照操作 ==========

    def batch_upsert_holding_snapshots(self, snapshots: List[HoldingSnapshot], dry_run: bool = False) -> Dict[str, Any]:
        """Write holdings_snapshot rows in a best-effort idempotent way.

        Strategy (simple + safe):
        - Use dedup_key as the stable business identifier.
        - Query existing records by dedup_key (client-side filter; Feishu doesn't support OR well).
        - Update existing; create missing.

        Notes:
        - This is a write-path, so schema mismatch should raise (no silent fallback).
        """
        if not snapshots:
            return {"created": 0, "updated": 0, "dry_run": dry_run}

        # 1) Build a map dedup_key -> snapshot
        by_key: Dict[str, HoldingSnapshot] = {}
        for s in snapshots:
            by_key[s.dedup_key] = s

        # 2) Fetch existing records for this as_of/account (narrow query)
        # We intentionally do NOT try to OR-by-dedup_key in filter.
        any_s = snapshots[0]
        # Narrow by as_of + account to reduce payload; as_of is Text.
        # Feishu filter uses '&&' for AND (not the literal 'AND').
        filter_str = (
            f'CurrentValue.[as_of] = "{self._escape_filter_value(any_s.as_of)}" && '
            f'CurrentValue.[account] = "{self._escape_filter_value(any_s.account)}"'
        )
        existing_records = self.client.list_records('holdings_snapshot', filter_str=filter_str)

        existing_by_key: Dict[str, str] = {}
        for r in existing_records:
            k = (r.get('fields') or {}).get('dedup_key')
            if k:
                existing_by_key[str(k)] = r['record_id']

        creates = []
        updates = []

        for k, s in by_key.items():
            fields = {
                'as_of': s.as_of,
                'account': s.account,
                'asset_id': s.asset_id,
                'market': s.market,
                'quantity': s.quantity,
                'currency': s.currency,
                'price': s.price,
                'cny_price': s.cny_price,
                'market_value_cny': s.market_value_cny,
                'dedup_key': s.dedup_key,
                'asset_name': s.asset_name,
                'avg_cost': s.avg_cost,
                'source': s.source,
                'remark': s.remark,
            }
            feishu_fields = self._to_feishu_fields(fields, 'holdings_snapshot')

            record_id = existing_by_key.get(k)
            if record_id:
                updates.append({'record_id': record_id, 'fields': feishu_fields})
            else:
                creates.append({'fields': feishu_fields})

        if dry_run:
            return {
                'dry_run': True,
                'filter': filter_str,
                'existing_count': len(existing_records),
                'to_create': len(creates),
                'to_update': len(updates),
                'create_sample': creates[:3],
                'update_sample': updates[:3],
            }

        created = 0
        updated = 0
        if creates:
            # batch_create expects [{'fields': {...}}, ...]
            self.client.batch_create_records('holdings_snapshot', creates)
            created = len(creates)
        if updates:
            self.client.batch_update_records('holdings_snapshot', updates)
            updated = len(updates)

        return {'dry_run': False, 'created': created, 'updated': updated, 'existing_count': len(existing_records)}

    # ========== nav_history 净值历史操作 ==========

    def _build_nav_index_payload(self, account: str, records: List[Dict[str, Any]]) -> Dict[str, Any]:
        navs: List[NAVHistory] = []
        nav_records: List[Dict[str, Any]] = []

        for record in records:
            raw_fields = record.get('fields') or {}
            fields = self._from_feishu_fields(raw_fields, 'nav_history')
            fields['record_id'] = record['record_id']
            nav = self._dict_to_nav(fields)
            if not nav.date:
                continue
            navs.append(nav)
            nav_records.append({
                'date': self._safe_date_str(nav.date),
                'record_id': nav.record_id,
                'total_value': nav.total_value,
                'shares': nav.shares,
                'nav': nav.nav,
                'cash_flow': nav.cash_flow,
                'pnl': nav.pnl,
                'mtd_nav_change': nav.mtd_nav_change,
                'ytd_nav_change': nav.ytd_nav_change,
                'mtd_pnl': nav.mtd_pnl,
                'ytd_pnl': nav.ytd_pnl,
                'updated_at': self._extract_updated_at_str(raw_fields),
            })

        nav_records.sort(key=lambda x: x.get('date') or '')
        navs.sort(key=lambda x: x.date)

        month_end_base: Dict[str, Dict[str, Any]] = {}
        year_end_base: Dict[str, Dict[str, Any]] = {}
        for row in nav_records:
            ds = row.get('date')
            if not ds:
                continue
            d = datetime.strptime(ds, '%Y-%m-%d').date()
            month_end_base[d.strftime('%Y-%m')] = dict(row)
            year_end_base[str(d.year)] = dict(row)

        inception_base = dict(nav_records[0]) if nav_records else None
        last_record = dict(nav_records[-1]) if nav_records else None

        return {
            'account': account,
            'record_count': len(nav_records),
            'nav_history': nav_records,
            'month_end_base': month_end_base,
            'year_end_base': year_end_base,
            'inception_base': inception_base,
            'last_record': last_record,
            'latest_updated_at': (last_record or {}).get('updated_at') if last_record else None,
            '_nav_objects': navs,
        }

    @staticmethod
    def _nav_index_fingerprint(payload: Dict[str, Any]) -> Dict[str, tuple]:
        fp: Dict[str, tuple] = {}
        for row in payload.get('nav_history') or []:
            ds = row.get('date')
            if not ds:
                continue
            fp[ds] = (row.get('record_id'), row.get('updated_at'))
        return fp

    def preload_nav_index(self, account: str, force_refresh: bool = False) -> Dict[str, Any]:
        """预加载并缓存 nav_history 索引（含 month/year/inception bases）。"""
        if (not force_refresh) and (account in self._nav_index_loaded_accounts):
            cached = self._nav_index_mem_cache.get(account) or {}
            return {
                'account': account,
                'loaded': int(cached.get('record_count', 0) or 0),
                'source': 'memory',
                'invalidated': False,
            }

        cached_local = self._local_nav_index_cache.get_account(account)

        filter_str = f'CurrentValue.[account] = "{self._escape_filter_value(account)}"'
        try:
            records = self.client.list_records(
                'nav_history',
                filter_str=filter_str,
                field_names=self.NAV_INDEX_PROJECTION_FIELDS,
            )
        except Exception as e:
            if 'FieldNameNotFound' in str(e):
                fallback_fields = [f for f in self.NAV_INDEX_PROJECTION_FIELDS if f != 'updated_at']
                records = self.client.list_records(
                    'nav_history',
                    filter_str=filter_str,
                    field_names=fallback_fields,
                )
            else:
                raise

        payload = self._build_nav_index_payload(account, records)
        invalidated = False

        if cached_local:
            missing_base = not cached_local.get('inception_base') or not cached_local.get('month_end_base') or not cached_local.get('year_end_base')
            if missing_base:
                invalidated = True
            else:
                old_fp = self._nav_index_fingerprint(cached_local)
                new_fp = self._nav_index_fingerprint(payload)
                if old_fp != new_fp:
                    invalidated = True

        self._nav_index_mem_cache[account] = payload
        self._nav_index_loaded_accounts.add(account)

        persist_payload = dict(payload)
        persist_payload.pop('_nav_objects', None)
        self._local_nav_index_cache.set_account(account, persist_payload)

        return {
            'account': account,
            'loaded': len(payload.get('nav_history') or []),
            'source': 'feishu',
            'invalidated': invalidated,
        }

    def _ensure_nav_index_loaded(self, account: str):
        if account in self._nav_index_loaded_accounts:
            return

        cached_local = self._local_nav_index_cache.get_account(account)
        if cached_local:
            navs: List[NAVHistory] = []
            for row in cached_local.get('nav_history') or []:
                ds = row.get('date')
                if not ds:
                    continue
                try:
                    d = datetime.strptime(ds[:10], '%Y-%m-%d').date()
                except Exception:
                    continue
                navs.append(NAVHistory(
                    record_id=row.get('record_id'),
                    date=d,
                    account=account,
                    total_value=float(row.get('total_value') or 0.0),
                    shares=float(row['shares']) if row.get('shares') is not None else None,
                    nav=float(row['nav']) if row.get('nav') is not None else None,
                    cash_flow=float(row['cash_flow']) if row.get('cash_flow') is not None else None,
                    pnl=float(row['pnl']) if row.get('pnl') is not None else None,
                    mtd_nav_change=float(row['mtd_nav_change']) if row.get('mtd_nav_change') is not None else None,
                    ytd_nav_change=float(row['ytd_nav_change']) if row.get('ytd_nav_change') is not None else None,
                    mtd_pnl=float(row['mtd_pnl']) if row.get('mtd_pnl') is not None else None,
                    ytd_pnl=float(row['ytd_pnl']) if row.get('ytd_pnl') is not None else None,
                ))

            payload = dict(cached_local)
            payload['_nav_objects'] = sorted(navs, key=lambda x: x.date)
            self._nav_index_mem_cache[account] = payload
            self._nav_index_loaded_accounts.add(account)
            return

        self.preload_nav_index(account)

    def get_nav_index(self, account: str) -> Dict[str, Any]:
        self._ensure_nav_index_loaded(account)
        return self._nav_index_mem_cache.get(account) or {}

    def _invalidate_nav_index(self, account: str):
        self._nav_index_loaded_accounts.discard(account)
        self._nav_index_mem_cache.pop(account, None)

    def _normalize_nav_date(self, nav_date: Any) -> date:
        if isinstance(nav_date, datetime):
            return nav_date.date()
        if isinstance(nav_date, str):
            return datetime.strptime(nav_date[:10], '%Y-%m-%d').date()
        return nav_date

    def _nav_to_index_row(self, nav: NAVHistory, updated_at: Optional[str] = None) -> Dict[str, Any]:
        return {
            'date': self._safe_date_str(nav.date),
            'record_id': nav.record_id,
            'total_value': nav.total_value,
            'shares': nav.shares,
            'nav': nav.nav,
            'cash_flow': nav.cash_flow,
            'pnl': nav.pnl,
            'mtd_nav_change': nav.mtd_nav_change,
            'ytd_nav_change': nav.ytd_nav_change,
            'mtd_pnl': nav.mtd_pnl,
            'ytd_pnl': nav.ytd_pnl,
            'updated_at': updated_at,
        }

    def _apply_nav_rows_to_local_cache(self, account: str, rows: List[Dict[str, Any]]):
        """增量更新本地 NAV 索引缓存，并失效内存镜像（下次从本地恢复，无需 API）。"""
        if not rows:
            return
        self._local_nav_index_cache.upsert_nav_records(account, rows, _flush=True)
        self._invalidate_nav_index(account)

    def save_nav(self, nav: NAVHistory, overwrite_existing: bool = True, dry_run: bool = False):
        """保存净值记录

        兼容不同 nav_history 表结构：若目标表缺少部分扩展字段（如 details），
        在首次写入失败时自动剔除未知字段并重试。
        写入前强制将日期标准化为纯 date，避免时分秒/时区导致的重复问题。

        Args:
            overwrite_existing: 是否允许覆盖同日已有记录
            dry_run: 仅演练，不实际写入
        """
        nav.date = self._normalize_nav_date(nav.date)

        existing = self.get_nav_on_date(nav.account, nav.date)
        if existing and existing.record_id and not overwrite_existing:
            raise ValueError(f"nav_history 已存在同日记录，拒绝覆盖: account={nav.account}, date={nav.date}")

        fields = self._nav_to_dict(nav)
        # 更新时需要保留 None，显式清空旧值；创建时继续过滤 None
        feishu_fields = self._to_feishu_fields(fields, 'nav_history', preserve_none=bool(existing and existing.record_id))

        if dry_run:
            return {"existing": bool(existing and existing.record_id), "fields": feishu_fields}

        used_fields = feishu_fields
        try:
            if existing and existing.record_id:
                self.client.update_record('nav_history', existing.record_id, feishu_fields)
                nav.record_id = existing.record_id
            else:
                result = self.client.create_record('nav_history', feishu_fields)
                nav.record_id = result['record_id']
        except Exception as e:
            msg = str(e)
            if 'FieldNameNotFound' not in msg:
                raise

            # 降级重试：剔除新表中不存在的扩展字段
            fallback_fields = dict(feishu_fields)
            for k in ['details']:
                fallback_fields.pop(k, None)
            used_fields = fallback_fields

            if existing and existing.record_id:
                self.client.update_record('nav_history', existing.record_id, fallback_fields)
                nav.record_id = existing.record_id
            else:
                result = self.client.create_record('nav_history', fallback_fields)
                nav.record_id = result['record_id']

        nav_row = self._nav_to_index_row(nav, updated_at=used_fields.get('updated_at'))
        self._apply_nav_rows_to_local_cache(nav.account, [nav_row])
        return

    def upsert_nav_bulk(
        self,
        nav_list: List[NAVHistory],
        mode: str = 'replace',
        allow_partial: bool = False,
    ) -> Dict[str, Any]:
        """批量 upsert nav_history（按 account+date 业务键）。

        Args:
            nav_list: NAVHistory 列表
            mode:
                - replace: 更新时 preserve_none=True（允许显式清空旧值）
                - upsert:  更新时 preserve_none=False（不主动清空旧值）
            allow_partial: True 时单账户失败不影响其他账户
        """
        if mode not in ('replace', 'upsert'):
            raise ValueError("mode must be 'replace' or 'upsert'")

        if not nav_list:
            return {
                'mode': mode,
                'total': 0,
                'updated': 0,
                'created': 0,
                'preloaded_accounts': [],
                'accounts': {},
                'errors': [],
            }

        grouped: Dict[str, List[NAVHistory]] = {}
        for nav in nav_list:
            if not nav or not nav.account:
                continue
            nav.date = self._normalize_nav_date(nav.date)
            grouped.setdefault(nav.account, []).append(nav)

        total_updated = 0
        total_created = 0
        preloaded_accounts: List[str] = []
        errors: List[Dict[str, Any]] = []
        account_results: Dict[str, Dict[str, Any]] = {}

        for account in sorted(grouped.keys()):
            navs_raw = grouped.get(account) or []
            # 同一 account + date 多次输入时，按最后一条为准，避免重复 update/create payload
            by_date_nav: Dict[str, NAVHistory] = {}
            for n in navs_raw:
                by_date_nav[self._safe_date_str(n.date)] = n
            navs = [by_date_nav[d] for d in sorted(by_date_nav.keys())]
            try:
                # 1) 一次预加载索引（仅投影字段）构建 date->record_id
                self.preload_nav_index(account)
                preloaded_accounts.append(account)
                idx = self.get_nav_index(account)
                existing_by_date: Dict[str, str] = {}
                existing_row_by_date: Dict[str, Dict[str, Any]] = {}
                for row in idx.get('nav_history') or []:
                    ds = str((row or {}).get('date') or '')
                    rid = (row or {}).get('record_id')
                    if ds:
                        existing_row_by_date[ds] = dict(row or {})
                    if ds and rid:
                        existing_by_date[ds] = rid

                update_payloads: List[Dict[str, Any]] = []
                update_rows_for_cache: List[Dict[str, Any]] = []
                create_payloads: List[Dict[str, Any]] = []
                create_rows_for_cache: List[Dict[str, Any]] = []

                preserve_none_for_update = (mode == 'replace')

                # 2) 构建 batch update/create payload
                for nav in sorted(navs, key=lambda x: x.date):
                    ds = self._safe_date_str(nav.date)
                    fields = self._nav_to_dict(nav)
                    rid = existing_by_date.get(ds)
                    if rid:
                        feishu_fields = self._to_feishu_fields(fields, 'nav_history', preserve_none=preserve_none_for_update)
                        update_payloads.append({'record_id': rid, 'fields': feishu_fields})
                        nav.record_id = rid

                        existing_row = dict(existing_row_by_date.get(ds) or {})
                        merged_row = dict(existing_row)
                        merged_row.update(self._nav_to_index_row(nav, updated_at=feishu_fields.get('updated_at')))
                        # upsert mode 不应把未提供字段写成 None：保留旧缓存值，避免本地索引与远端语义不一致
                        if not preserve_none_for_update:
                            for k, v in list(merged_row.items()):
                                if v is None and k in existing_row:
                                    merged_row[k] = existing_row.get(k)
                        update_rows_for_cache.append(merged_row)
                    else:
                        feishu_fields = self._to_feishu_fields(fields, 'nav_history', preserve_none=False)
                        create_payloads.append({'fields': feishu_fields})
                        create_rows_for_cache.append(self._nav_to_index_row(nav, updated_at=feishu_fields.get('updated_at')))

                # 3) 批量写入（最多 1 次 preload + 1 次 batch_update + 1 次 batch_create for N<=500）
                if update_payloads:
                    try:
                        self.client.batch_update_records('nav_history', update_payloads)
                    except Exception as e:
                        msg = str(e)
                        if 'FieldNameNotFound' not in msg:
                            raise
                        fallback_updates = []
                        fallback_rows = []
                        for p, row in zip(update_payloads, update_rows_for_cache):
                            f = dict(p.get('fields') or {})
                            f.pop('details', None)
                            fallback_updates.append({'record_id': p['record_id'], 'fields': f})
                            r = dict(row)
                            r['updated_at'] = f.get('updated_at')
                            fallback_rows.append(r)
                        self.client.batch_update_records('nav_history', fallback_updates)
                        update_rows_for_cache = fallback_rows

                if create_payloads:
                    try:
                        created = self.client.batch_create_records('nav_history', create_payloads)
                    except Exception as e:
                        msg = str(e)
                        if 'FieldNameNotFound' not in msg:
                            raise
                        fallback_creates = []
                        for p in create_payloads:
                            f = dict((p.get('fields') or {}))
                            f.pop('details', None)
                            fallback_creates.append({'fields': f})
                        created = self.client.batch_create_records('nav_history', fallback_creates)
                        # 同步 updated_at（若 details 被剔除，不影响）
                        for i, p in enumerate(fallback_creates):
                            if i < len(create_rows_for_cache):
                                create_rows_for_cache[i]['updated_at'] = (p.get('fields') or {}).get('updated_at')

                    for i, nav in enumerate([n for n in navs if self._safe_date_str(n.date) not in existing_by_date]):
                        rec = created[i] if i < len(created) else {}
                        rid = rec.get('record_id') or ((rec.get('record') or {}).get('record_id') if isinstance(rec, dict) else None)
                        nav.record_id = rid
                        if i < len(create_rows_for_cache):
                            create_rows_for_cache[i]['record_id'] = rid

                # 4) 增量刷新本地索引缓存
                all_rows = []
                all_rows.extend(update_rows_for_cache)
                all_rows.extend(create_rows_for_cache)
                if all_rows:
                    self._apply_nav_rows_to_local_cache(account, all_rows)

                updated_n = len(update_payloads)
                created_n = len(create_payloads)
                total_updated += updated_n
                total_created += created_n
                account_results[account] = {
                    'updated': updated_n,
                    'created': created_n,
                    'total': len(navs),
                }
            except Exception as e:
                err = {'account': account, 'error': str(e), 'count': len(navs)}
                errors.append(err)
                if not allow_partial:
                    raise
                account_results[account] = {
                    'updated': 0,
                    'created': 0,
                    'total': len(navs),
                    'error': str(e),
                }

        return {
            'mode': mode,
            'total': len(nav_list),
            'updated': total_updated,
            'created': total_created,
            'preloaded_accounts': preloaded_accounts,
            'accounts': account_results,
            'errors': errors,
        }

    def get_nav_history(self, account: str, days: int = 365) -> List[NAVHistory]:
        """获取净值历史（优先本地预加载索引）。"""
        from datetime import timedelta
        from .time_utils import bj_today
        start_date = bj_today() - timedelta(days=days)

        self.preload_nav_index(account)
        idx = self.get_nav_index(account)
        navs: List[NAVHistory] = list(idx.get('_nav_objects') or [])
        if not navs:
            # 兜底：如果缓存不可用，直接走一次 preload
            self.preload_nav_index(account, force_refresh=True)
            idx = self.get_nav_index(account)
            navs = list(idx.get('_nav_objects') or [])

        filtered = [n for n in navs if n.date and n.date >= start_date]
        filtered.sort(key=lambda n: n.date)
        return filtered

    def get_latest_nav(self, account: str) -> Optional[NAVHistory]:
        """获取最新净值记录（优先索引）。"""
        idx = self.get_nav_index(account)
        navs = idx.get('_nav_objects') or []
        return navs[-1] if navs else None

    def get_nav_on_date(self, account: str, nav_date: date) -> Optional[NAVHistory]:
        """获取指定日期的净值记录（按纯日期匹配，优先索引）。"""
        if isinstance(nav_date, datetime):
            nav_date = nav_date.date()
        elif isinstance(nav_date, str):
            nav_date = datetime.strptime(nav_date[:10], '%Y-%m-%d').date()

        idx = self.get_nav_index(account)
        navs = idx.get('_nav_objects') or []
        matches = [n for n in navs if n.date == nav_date]

        if len(matches) > 1:
            print(f"[警告] nav_history 存在重复日期记录: account={account}, date={nav_date}, count={len(matches)}")

        return matches[0] if matches else None

    def update_nav_fields(
        self,
        record_id: str,
        fields: Dict[str, Any],
        dry_run: bool = False,
        allowed_fields: Optional[set] = None,
    ):
        """更新 nav_history 指定字段（patch 语义）。

        Safety:
        - If allowed_fields is provided, reject any key not in the whitelist.
        - Always preserve_none=True so callers can intentionally clear fields.
        """
        if allowed_fields is not None:
            illegal = [k for k in fields.keys() if k not in allowed_fields]
            if illegal:
                raise ValueError(f"update_nav_fields: illegal field(s): {illegal}. allowed={sorted(list(allowed_fields))}")

        normalized = {}
        for k, v in fields.items():
            if k in ('mtd_nav_change', 'ytd_nav_change') and v is not None:
                normalized[k] = self._quantize_nav(v)
            elif k in ('mtd_pnl', 'ytd_pnl', 'pnl', 'cash_flow', 'share_change') and v is not None:
                normalized[k] = self._quantize_money(v)
            else:
                normalized[k] = v

        feishu_fields = self._to_feishu_fields(normalized, 'nav_history', preserve_none=True)
        if dry_run:
            return {"record_id": record_id, "fields": feishu_fields}
        self.client.update_record('nav_history', record_id, feishu_fields)
        self._nav_index_loaded_accounts.clear()
        self._nav_index_mem_cache.clear()
        return {"record_id": record_id, "fields": feishu_fields}

    def get_latest_nav_before(self, account: str, before_date: date) -> Optional[NAVHistory]:
        """获取指定日期之前的最新净值记录（优先索引）。"""
        navs = self.get_nav_index(account).get('_nav_objects') or []
        candidates = [n for n in navs if n.date and n.date < before_date]
        candidates.sort(key=lambda n: n.date, reverse=True)
        return candidates[0] if candidates else None

    def get_total_shares(self, account: str) -> float:
        """获取账户总份额"""
        latest = self.get_latest_nav(account)
        return latest.shares if latest else 0.0

    def _nav_to_dict(self, nav: NAVHistory) -> Dict:
        """NAVHistory 转字典"""
        return {
            'date': nav.date,
            'account': nav.account,
            'total_value': nav.total_value,
            'cash_value': nav.cash_value,
            'stock_value': nav.stock_value,
            'fund_value': nav.fund_value,
            'cn_stock_value': nav.cn_stock_value,
            'us_stock_value': nav.us_stock_value,
            'hk_stock_value': nav.hk_stock_value,
            'stock_weight': nav.stock_weight,
            'cash_weight': nav.cash_weight,
            'shares': nav.shares,
            'nav': nav.nav,
            'cash_flow': nav.cash_flow,
            'share_change': nav.share_change,
            'mtd_nav_change': nav.mtd_nav_change,
            'ytd_nav_change': nav.ytd_nav_change,
            'pnl': nav.pnl,
            'mtd_pnl': nav.mtd_pnl,
            'ytd_pnl': nav.ytd_pnl,
            'details': nav.details,
        }

    def _dict_to_nav(self, data: Dict) -> NAVHistory:
        """字典转 NAVHistory"""
        nav_date = data.get('date')
        if isinstance(nav_date, (int, float)):
            # 飞书日期字段返回 Unix 时间戳（毫秒），按业务时区解析，避免东八区日期被截成前一天
            nav_date = datetime.fromtimestamp(nav_date / 1000, tz=self.FEISHU_DATE_TZ).date()
        elif isinstance(nav_date, str):
            nav_date = datetime.strptime(nav_date[:10], '%Y-%m-%d').date()

        def _opt_float(key):
            v = data.get(key)
            if v is None:
                return None
            return FeishuStorage._parse_float(v)

        return NAVHistory(
            date=nav_date,
            record_id=data.get('record_id'),
            account=data.get('account', ''),
            total_value=FeishuStorage._parse_float(data.get('total_value')) or 0.0,
            cash_value=_opt_float('cash_value'),
            stock_value=_opt_float('stock_value'),
            fund_value=_opt_float('fund_value'),
            cn_stock_value=_opt_float('cn_stock_value'),
            us_stock_value=_opt_float('us_stock_value'),
            hk_stock_value=_opt_float('hk_stock_value'),
            stock_weight=_opt_float('stock_weight'),
            cash_weight=_opt_float('cash_weight'),
            shares=_opt_float('shares'),
            nav=_opt_float('nav'),
            cash_flow=_opt_float('cash_flow'),
            share_change=_opt_float('share_change'),
            mtd_nav_change=_opt_float('mtd_nav_change'),
            ytd_nav_change=_opt_float('ytd_nav_change'),
            pnl=_opt_float('pnl'),
            mtd_pnl=_opt_float('mtd_pnl'),
            ytd_pnl=_opt_float('ytd_pnl'),
            details=data.get('details')
        )

    # ========== price_cache 价格缓存操作 ==========

    def get_price(self, asset_id: str, *, allow_expired: bool = False, max_stale_after_expiry_sec: int = 0) -> Optional[PriceCache]:
        """获取缓存价格 - 使用本地文件

        Args:
            allow_expired: True 时允许返回过期缓存（用于非交易时段稳定优先）
            max_stale_after_expiry_sec: 允许过期后最多多少秒仍可返回
        """
        return self._local_price_cache.get(asset_id, allow_expired=allow_expired, max_stale_after_expiry_sec=max_stale_after_expiry_sec)

    def save_price(self, price: PriceCache):
        """保存价格缓存 - 使用本地文件（零 API 调用）"""
        self._local_price_cache.save(price)

    def get_all_prices(self) -> List[PriceCache]:
        """获取所有有效价格缓存 - 使用本地文件（零 API 调用）"""
        return self._local_price_cache.get_all()

    def _price_cache_to_dict(self, price: PriceCache) -> Dict:
        """PriceCache 转字典"""
        return {
            'asset_id': price.asset_id,
            'asset_name': price.asset_name,
            'asset_type': price.asset_type,
            'price': price.price,
            'currency': price.currency,
            'cny_price': price.cny_price,
            'change': price.change,
            'change_pct': price.change_pct,
            'exchange_rate': price.exchange_rate,
            'data_source': price.data_source,
            'expires_at': price.expires_at,
        }

    def _dict_to_price_cache(self, data: Dict) -> PriceCache:
        """字典转 PriceCache"""
        return PriceCache(
            asset_id=data.get('asset_id', ''),
            asset_name=data.get('asset_name'),
            asset_type=AssetType(data.get('asset_type')) if data.get('asset_type') else AssetType.OTHER,
            price=float(data.get('price', 0)),
            currency=data.get('currency', 'CNY'),
            cny_price=float(data.get('cny_price')) if data.get('cny_price') is not None else None,
            change=float(data.get('change')) if data.get('change') is not None else None,
            change_pct=float(data.get('change_pct')) if data.get('change_pct') is not None else None,
            exchange_rate=float(data.get('exchange_rate')) if data.get('exchange_rate') is not None else None,
            data_source=data.get('data_source'),
            expires_at=data.get('expires_at')
        )
