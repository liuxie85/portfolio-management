"""
飞书多维表存储层
作为唯一存储后端（已移除 SQLite 后端）
"""
import json
import re
from datetime import date, datetime, timezone, timedelta
from decimal import Decimal, ROUND_HALF_UP
from typing import List, Optional, Dict, Any, Tuple

from .models import (
    Holding, Transaction, CashFlow, NAVHistory, PriceCache,
    AssetType, TransactionType, AssetClass, Industry,
    make_tx_dedup_key, make_cf_dedup_key, make_request_id, DATETIME_FORMAT
)
from .snapshot_models import HoldingSnapshot
from .feishu_client import FeishuClient
from .local_cache import LocalPriceCache


class FeishuStorage:
    """飞书多维表存储层 (带内存缓存优化)"""

    FEISHU_DATE_TZ = timezone(timedelta(hours=8))
    MONEY_QUANT = Decimal('0.01')
    NAV_QUANT = Decimal('0.000001')
    WEIGHT_QUANT = Decimal('0.000001')

    def __init__(self, client: FeishuClient = None):
        """
        初始化飞书存储层

        Args:
            client: FeishuClient 实例，如果不传则自动创建
        """
        self.client = client or FeishuClient()

        # 内存缓存：减少 API 调用次数
        # key: "asset_id:account:market" -> value: record_id
        self._holding_id_cache: Dict[str, str] = {}

        # 防重缓存：本地 Set 预检，避免重复 API 查询
        # key: request_id/dedup_key -> value: record_id (或 True 表示已存在)
        self._request_id_cache: Dict[str, str] = {}  # transactions 表
        self._dedup_key_cache: Dict[str, str] = {}   # transactions 和 cash_flow 表

        # 本地文件价格缓存（替代飞书多维表）
        self._local_price_cache = LocalPriceCache()

    def _get_holding_cache_key(self, asset_id: str, account: str, market: Optional[str]) -> str:
        """生成持仓缓存 key"""
        return f"{asset_id}:{account}:{market or ''}"

    def _invalidate_holding_cache(self, asset_id: str, account: str, market: Optional[str]):
        """清除持仓缓存"""
        cache_key = self._get_holding_cache_key(asset_id, account, market)
        self._holding_id_cache.pop(cache_key, None)

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

    # ========== holdings 持仓操作 ==========

    def get_holding(self, asset_id: str, account: str, market: Optional[str] = None) -> Optional[Holding]:
        """获取单个持仓 (带缓存预热)"""
        # 飞书 API 不支持 OR，需要分两次查询
        # 先查指定 market 的
        if market:
            filter_str = f'CurrentValue.[asset_id] = "{self._escape_filter_value(asset_id)}" AND CurrentValue.[account] = "{self._escape_filter_value(account)}" AND CurrentValue.[market] = "{self._escape_filter_value(market)}"'
            records = self.client.list_records('holdings', filter_str=filter_str)
            if records:
                record = records[0]
                fields = self._from_feishu_fields(record['fields'], 'holdings')
                fields['record_id'] = record['record_id']

                # 缓存 record_id
                cache_key = self._get_holding_cache_key(asset_id, account, market)
                self._holding_id_cache[cache_key] = record['record_id']

                return self._dict_to_holding(fields)
        else:
            # 没有指定 market，先查有 market 的，再查空的
            filter_str = f'CurrentValue.[asset_id] = "{self._escape_filter_value(asset_id)}" AND CurrentValue.[account] = "{self._escape_filter_value(account)}"'
            records = self.client.list_records('holdings', filter_str=filter_str)

            if records:
                # 优先返回 market 为空的记录，其次是第一个
                for record in records:
                    if not record['fields'].get('market'):
                        fields = self._from_feishu_fields(record['fields'], 'holdings')
                        fields['record_id'] = record['record_id']

                        # 缓存 record_id
                        cache_key = self._get_holding_cache_key(asset_id, account, market)
                        self._holding_id_cache[cache_key] = record['record_id']

                        return self._dict_to_holding(fields)
                # 返回第一条
                record = records[0]
                fields = self._from_feishu_fields(record['fields'], 'holdings')
                fields['record_id'] = record['record_id']

                # 缓存 record_id（使用返回记录的 market）
                record_market = record['fields'].get('market', '')
                cache_key = self._get_holding_cache_key(asset_id, account, record_market or None)
                self._holding_id_cache[cache_key] = record['record_id']

                return self._dict_to_holding(fields)

        return None

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
        records = self.client.list_records('holdings', filter_str=filter_str)

        holdings = []
        for record in records:
            fields = self._from_feishu_fields(record['fields'], 'holdings')
            fields['record_id'] = record['record_id']
            holding = self._dict_to_holding(fields)

            # 在代码中过滤 quantity <= 0 的记录（除非 include_empty=True）
            if not include_empty and holding.quantity <= 0:
                continue

            holdings.append(holding)

        # 按 asset_type 和 asset_id 排序
        holdings.sort(key=lambda h: (h.asset_type.value if h.asset_type else '', h.asset_id))
        return holdings

    def upsert_holding(self, holding: Holding) -> Holding:
        """插入或更新持仓 (带内存缓存优化)"""
        from .time_utils import bj_now_naive

        now = bj_now_naive()
        cache_key = self._get_holding_cache_key(
            holding.asset_id, holding.account, holding.market
        )

        # 1. 尝试从缓存获取 record_id
        cached_record_id = self._holding_id_cache.get(cache_key)

        if cached_record_id:
            # 2a. 缓存命中：直接尝试更新（乐观更新）
            try:
                # 需要先获取当前数量（累加逻辑）
                existing = self.get_holding(
                    holding.asset_id, holding.account, holding.market
                )
                if existing and existing.record_id:
                    is_cash_like = (existing.asset_type and existing.asset_type.value in ('cash', 'mmf'))
                    new_quantity = self._quantize_money(existing.quantity + holding.quantity) if is_cash_like else (existing.quantity + holding.quantity)
                    update_fields = {
                        'quantity': new_quantity,
                        'updated_at': now.strftime(DATETIME_FORMAT)
                    }

                    # 更新名称（如果新名称更完整）
                    new_name = holding.asset_name or existing.asset_name
                    if new_name and len(new_name) > len(existing.asset_name or ''):
                        update_fields['asset_name'] = new_name
                        print(f"[持仓名称更新] {existing.asset_name} -> {new_name}")

                    self.client.update_record(
                        'holdings', cached_record_id, update_fields
                    )
                    holding.record_id = cached_record_id
                    holding.updated_at = now
                    return holding
                else:
                    # 缓存过期或记录被删除，清除缓存
                    self._invalidate_holding_cache(
                        holding.asset_id, holding.account, holding.market
                    )
            except Exception as e:
                # 更新失败（记录可能被删除），清除缓存重试
                print(f"[缓存更新失败] {cache_key}: {e}")
                self._invalidate_holding_cache(
                    holding.asset_id, holding.account, holding.market
                )

        # 2b. 缓存未命中或更新失败：查询后决定创建或更新
        existing = self.get_holding(
            holding.asset_id, holding.account, holding.market
        )

        if existing and existing.record_id:
            # 更新现有记录
            is_cash_like = (existing.asset_type and existing.asset_type.value in ('cash', 'mmf'))
            new_quantity = self._quantize_money(existing.quantity + holding.quantity) if is_cash_like else (existing.quantity + holding.quantity)
            update_fields = {
                'quantity': new_quantity,
                'updated_at': now.strftime(DATETIME_FORMAT)
            }

            # 更新名称（如果新名称更完整）
            new_name = holding.asset_name or existing.asset_name
            if new_name and len(new_name) > len(existing.asset_name or ''):
                update_fields['asset_name'] = new_name
                print(f"[持仓名称更新] {existing.asset_name} -> {new_name}")

            self.client.update_record(
                'holdings', existing.record_id, update_fields
            )
            holding.record_id = existing.record_id
            holding.updated_at = now

            # 缓存 record_id
            self._holding_id_cache[cache_key] = existing.record_id
        else:
            # 创建新记录
            holding.created_at = now
            holding.updated_at = now

            fields = self._holding_to_dict(holding)
            feishu_fields = self._to_feishu_fields(fields, 'holdings')

            result = self.client.create_record('holdings', feishu_fields)
            holding.record_id = result['record_id']

            # 缓存新记录的 record_id
            self._holding_id_cache[cache_key] = result['record_id']

        return holding

    def update_holding_quantity(self, asset_id: str, account: str, quantity_change: float, market: Optional[str] = None):
        """更新持仓数量"""
        from .time_utils import bj_now_naive

        holding = self.get_holding(asset_id, account, market)
        if not holding or not holding.record_id:
            return

        is_cash_like = (holding.asset_type and holding.asset_type.value in ('cash', 'mmf'))
        new_quantity = self._quantize_money(holding.quantity + quantity_change) if is_cash_like else (holding.quantity + quantity_change)
        update_fields = {
            'quantity': new_quantity,
            'updated_at': bj_now_naive().strftime('%Y-%m-%d %H:%M:%S')
        }
        self.client.update_record('holdings', holding.record_id, update_fields)

    def delete_holding_if_zero(self, asset_id: str, account: str, market: Optional[str] = None):
        """如果持仓为0则删除（容忍极小浮点残值）"""
        holding = self.get_holding(asset_id, account, market)
        if holding and holding.record_id and abs(holding.quantity) <= 1e-8:
            self.client.delete_record('holdings', holding.record_id)

    def delete_holding_by_record_id(self, record_id: str) -> bool:
        """通过记录ID删除持仓"""
        return self.client.delete_record('holdings', record_id)

    def delete_transaction_by_record_id(self, record_id: str) -> bool:
        """通过记录ID删除交易"""
        return self.client.delete_record('transactions', record_id)

    def delete_cash_flow_by_record_id(self, record_id: str) -> bool:
        """通过记录ID删除出入金"""
        return self.client.delete_record('cash_flow', record_id)

    def delete_nav_by_record_id(self, record_id: str) -> bool:
        """通过记录ID删除净值记录"""
        return self.client.delete_record('nav_history', record_id)

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

    def get_cash_flows(self, account: Optional[str] = None,
                      start_date: Optional[date] = None,
                      end_date: Optional[date] = None) -> List[CashFlow]:
        """获取出入金记录列表"""
        conditions = []

        if account:
            conditions.append(f'CurrentValue.[account] = "{account}"')
        filter_str = ' AND '.join(conditions) if conditions else None
        records = self.client.list_records('cash_flow', filter_str=filter_str)

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

    def save_nav(self, nav: NAVHistory, overwrite_existing: bool = True, dry_run: bool = False):
        """保存净值记录

        兼容不同 nav_history 表结构：若目标表缺少部分扩展字段（如 details），
        在首次写入失败时自动剔除未知字段并重试。
        写入前强制将日期标准化为纯 date，避免时分秒/时区导致的重复问题。

        Args:
            overwrite_existing: 是否允许覆盖同日已有记录
            dry_run: 仅演练，不实际写入
        """
        if isinstance(nav.date, datetime):
            nav.date = nav.date.date()
        elif isinstance(nav.date, str):
            nav.date = datetime.strptime(nav.date[:10], '%Y-%m-%d').date()

        existing = self.get_nav_on_date(nav.account, nav.date)
        if existing and existing.record_id and not overwrite_existing:
            raise ValueError(f"nav_history 已存在同日记录，拒绝覆盖: account={nav.account}, date={nav.date}")

        fields = self._nav_to_dict(nav)
        # 更新时需要保留 None，显式清空旧值；创建时继续过滤 None
        feishu_fields = self._to_feishu_fields(fields, 'nav_history', preserve_none=bool(existing and existing.record_id))

        if dry_run:
            return {"existing": bool(existing and existing.record_id), "fields": feishu_fields}

        try:
            if existing and existing.record_id:
                self.client.update_record('nav_history', existing.record_id, feishu_fields)
                nav.record_id = existing.record_id
            else:
                result = self.client.create_record('nav_history', feishu_fields)
                nav.record_id = result['record_id']
            return
        except Exception as e:
            msg = str(e)
            if 'FieldNameNotFound' not in msg:
                raise

        # 降级重试：剔除新表中不存在的扩展字段
        fallback_fields = dict(feishu_fields)
        for k in ['details']:
            fallback_fields.pop(k, None)

        if existing and existing.record_id:
            self.client.update_record('nav_history', existing.record_id, fallback_fields)
            nav.record_id = existing.record_id
        else:
            result = self.client.create_record('nav_history', fallback_fields)
            nav.record_id = result['record_id']

    def get_nav_history(self, account: str, days: int = 365) -> List[NAVHistory]:
        """获取净值历史"""
        from datetime import timedelta
        from .time_utils import bj_today
        start_date = bj_today() - timedelta(days=days)

        # 飞书日期字段不支持 >=/<= 比较操作符，只用 account 过滤，日期在客户端筛选
        filter_str = f'CurrentValue.[account] = "{self._escape_filter_value(account)}"'
        records = self.client.list_records('nav_history', filter_str=filter_str)

        navs = []
        for record in records:
            fields = self._from_feishu_fields(record['fields'], 'nav_history')
            fields['record_id'] = record['record_id']
            nav = self._dict_to_nav(fields)
            if nav.date and nav.date >= start_date:
                navs.append(nav)

        navs.sort(key=lambda n: n.date)
        return navs

    def get_latest_nav(self, account: str) -> Optional[NAVHistory]:
        """获取最新净值记录"""
        filter_str = f'CurrentValue.[account] = "{self._escape_filter_value(account)}"'
        records = self.client.list_records('nav_history', filter_str=filter_str)

        if not records:
            return None

        # 按日期排序取最新
        navs = []
        for record in records:
            fields = self._from_feishu_fields(record['fields'], 'nav_history')
            fields['record_id'] = record['record_id']
            navs.append(self._dict_to_nav(fields))

        navs.sort(key=lambda n: n.date, reverse=True)
        return navs[0] if navs else None

    def get_nav_on_date(self, account: str, nav_date: date) -> Optional[NAVHistory]:
        """获取指定日期的净值记录（按纯日期匹配）"""
        if isinstance(nav_date, datetime):
            nav_date = nav_date.date()
        elif isinstance(nav_date, str):
            nav_date = datetime.strptime(nav_date[:10], '%Y-%m-%d').date()

        filter_str = f'CurrentValue.[account] = "{self._escape_filter_value(account)}"'
        records = self.client.list_records('nav_history', filter_str=filter_str)

        matches = []
        for record in records:
            fields = self._from_feishu_fields(record['fields'], 'nav_history')
            fields['record_id'] = record['record_id']
            nav = self._dict_to_nav(fields)
            if nav.date == nav_date:
                matches.append(nav)

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
        return {"record_id": record_id, "fields": feishu_fields}

    def get_latest_nav_before(self, account: str, before_date: date) -> Optional[NAVHistory]:
        """获取指定日期之前的最新净值记录"""
        # 飞书日期字段不支持比较操作符，获取全部后客户端筛选
        filter_str = f'CurrentValue.[account] = "{self._escape_filter_value(account)}"'
        records = self.client.list_records('nav_history', filter_str=filter_str)

        if not records:
            return None

        navs = []
        for record in records:
            fields = self._from_feishu_fields(record['fields'], 'nav_history')
            fields['record_id'] = record['record_id']
            nav = self._dict_to_nav(fields)
            if nav.date and nav.date < before_date:
                navs.append(nav)

        navs.sort(key=lambda n: n.date, reverse=True)
        return navs[0] if navs else None

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

    def get_price(self, asset_id: str) -> Optional[PriceCache]:
        """获取缓存价格（检查有效期）- 使用本地文件"""
        return self._local_price_cache.get(asset_id)

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
