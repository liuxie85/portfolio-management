"""
数据模型 (Pydantic v2)

变更记录:
- 删除遗留字段 (id)
- 删除未使用的计算字段 (pnl, current_price, weight)
- 新增 dedup_key 防重字段 (Transaction, CashFlow)
- Holding.market 默认值从 None 改为 ""
- 精简 TransactionType 枚举
- 新增 dedup_key 生成工具函数
"""
import hashlib
import uuid
from datetime import date, datetime
from decimal import Decimal, ROUND_HALF_UP
from enum import Enum
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from typing import Optional, Dict, List, Any


# ========== 常量 ==========

# 日期时间格式常量
DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'

MONEY_QUANT = Decimal('0.01')
NAV_QUANT = Decimal('0.000001')
WEIGHT_QUANT = Decimal('0.000001')


def _quantize_decimal(value: Any, quant: Decimal) -> Optional[float]:
    if value is None:
        return None
    return float(Decimal(str(value)).quantize(quant, rounding=ROUND_HALF_UP))


# 现金资产ID常量
CASH_ASSET_ID = "CNY-CASH"      # 人民币现金
MMF_ASSET_ID = "CNY-MMF"        # 货币基金
USD_CASH_ASSET_ID = "USD-CASH"  # 美元现金
HKD_CASH_ASSET_ID = "HKD-CASH"  # 港币现金

# 现金资产ID集合（用于快速判断）
CASH_ASSET_IDS = {CASH_ASSET_ID, MMF_ASSET_ID, USD_CASH_ASSET_ID, HKD_CASH_ASSET_ID}


# ========== 枚举类型 ==========

class AssetType(str, Enum):
    """资产类型"""
    A_STOCK = "a_stock"
    HK_STOCK = "hk_stock"
    US_STOCK = "us_stock"
    FUND = "fund"
    CASH = "cash"
    MMF = "mmf"
    CRYPTO = "crypto"
    BOND = "bond"
    OTHER = "other"


class Currency(str, Enum):
    """币种枚举"""
    CNY = "CNY"  # 人民币
    USD = "USD"  # 美元
    HKD = "HKD"  # 港币


class MarketType(str, Enum):
    """市场类型枚举（用于缓存 TTL 计算）"""
    CN = "cn"      # A股
    HK = "hk"      # 港股
    US = "us"      # 美股
    FUND = "fund"  # 基金


class AssetClass(str, Enum):
    """资产类别（按市场/地域）"""
    CN_ASSET = "中国资产"
    US_ASSET = "美国资产"
    HK_ASSET = "港股资产"
    CASH = "现金"
    ALTERNATIVE = "另类资产"


class TransactionType(str, Enum):
    """交易类型"""
    BUY = "BUY"
    SELL = "SELL"
    DEPOSIT = "DEPOSIT"
    WITHDRAW = "WITHDRAW"


class Industry(str, Enum):
    """行业分类"""
    ZHONGGAI = "中概"
    CONSUMPTION = "消费"
    ENERGY = "能源"
    SEMICONDUCTOR = "半导体"
    FINANCE = "金融"
    INTERNET = "互联网"
    TECH = "科技"
    HEALTHCARE = "医疗"
    REAL_ESTATE = "房地产"
    ENTERTAINMENT = "文体娱乐"
    INDEX = "非行业指数"
    CASH = "现金"
    BLOCKCHAIN = "区块链"
    OTHER = "其他"


# ========== 数据模型 ==========

class Holding(BaseModel):
    """持仓记录

    业务主键: (asset_id, account, market)
    """
    record_id: Optional[str] = None

    # 核心字段
    asset_id: str = Field(..., description="资产代码")
    asset_name: str = Field(..., description="资产名称")
    asset_type: AssetType = Field(..., description="资产类型")
    account: str = Field(..., description="账户标识")
    market: str = Field("", description="券商/平台")
    quantity: float = Field(0.0, description="持仓数量")
    avg_cost: Optional[float] = Field(None, description="平均成本价")
    currency: str = Field(..., description="币种")

    # 分类
    asset_class: Optional[AssetClass] = None
    industry: Optional[Industry] = None

    # 可选元数据（保留飞书兼容）
    tag: Optional[List[str]] = Field(default_factory=list)
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    # 运行时计算字段（calculate_valuation 写入，不持久化）
    current_price: Optional[float] = None
    cny_price: Optional[float] = None
    market_value_cny: Optional[float] = None
    weight: Optional[float] = None

    @field_validator('market', mode='before')
    @classmethod
    def coerce_market(cls, v):
        return v if v is not None else ""

    @field_validator('avg_cost', mode='before')
    @classmethod
    def quantize_avg_cost(cls, v):
        return _quantize_decimal(v, MONEY_QUANT)

    model_config = ConfigDict(from_attributes=True)


class Transaction(BaseModel):
    """交易记录

    防重机制: request_id（调用方控制）+ dedup_key（内容指纹自动生成）
    """
    record_id: Optional[str] = None
    dedup_key: Optional[str] = Field(None, description="内容指纹，自动生成")
    request_id: Optional[str] = Field(None, description="调用方幂等键")

    # 核心字段
    tx_date: date = Field(..., description="交易日期")
    tx_type: TransactionType = Field(..., description="交易类型")
    asset_id: str = Field(..., description="资产代码")
    asset_name: Optional[str] = None
    asset_type: Optional[AssetType] = None
    account: str = Field(..., description="账户标识")
    market: str = Field("", description="券商/平台")
    quantity: float = Field(..., description="数量(买入为正,卖出为负)")
    price: float = Field(..., description="成交价格")
    amount: Optional[float] = Field(None, description="成交金额(自动计算)")
    currency: str = Field(..., description="币种")
    fee: float = Field(0.0, description="手续费")
    remark: Optional[str] = None

    # 可选元数据（当前业务未使用，保留扩展性）
    tax: float = Field(0.0, description="税费(预留)")
    related_account: Optional[str] = Field(None, description="转账对方(预留)")
    source: str = Field("manual", description="来源")

    @field_validator('market', mode='before')
    @classmethod
    def coerce_market(cls, v):
        return v if v is not None else ""

    @field_validator('price', 'fee', 'tax', mode='before')
    @classmethod
    def quantize_money_fields(cls, v):
        return _quantize_decimal(v, MONEY_QUANT)

    @field_validator('amount', mode='before')
    @classmethod
    def calculate_amount(cls, v, info):
        """优先量化显式传入的 amount；自动计算在 model_validator 中兜底。"""
        if v is not None:
            return _quantize_decimal(v, MONEY_QUANT)
        return None

    @model_validator(mode='after')
    def fill_amount(self):
        if self.amount is None and self.quantity is not None and self.price is not None:
            self.amount = _quantize_decimal(Decimal(str(self.quantity)) * Decimal(str(self.price)), MONEY_QUANT)
        return self

    model_config = ConfigDict(from_attributes=True)


class CashFlow(BaseModel):
    """出入金记录

    防重机制: dedup_key（内容指纹自动生成）
    """
    record_id: Optional[str] = None
    dedup_key: Optional[str] = Field(None, description="内容指纹，自动生成")

    # 核心字段
    flow_date: date = Field(..., description="日期")
    account: str = Field(..., description="账户")
    amount: float = Field(..., description="金额(正数入金,负数出金)")
    currency: str = Field(..., description="币种")
    cny_amount: Optional[float] = None
    exchange_rate: Optional[float] = None
    flow_type: str = Field(..., description="DEPOSIT/WITHDRAW")
    source: Optional[str] = None
    remark: Optional[str] = None

    @field_validator('amount', 'cny_amount', mode='before')
    @classmethod
    def quantize_money_fields(cls, v):
        return _quantize_decimal(v, MONEY_QUANT)


class PriceCache(BaseModel):
    """价格缓存

    业务主键: asset_id
    """
    asset_id: str
    asset_name: Optional[str] = None
    asset_type: AssetType = AssetType.OTHER

    price: float
    currency: str
    cny_price: float

    change: Optional[float] = None
    change_pct: Optional[float] = None

    exchange_rate: Optional[float] = None
    data_source: Optional[str] = None
    expires_at: Optional[datetime] = None

    @field_validator('price', 'cny_price', mode='before')
    @classmethod
    def quantize_price_fields(cls, v):
        return _quantize_decimal(v, MONEY_QUANT)


class NAVHistory(BaseModel):
    """净值历史

    业务主键: (account, date)
    字段对照 CSV: 月份→date, 股票市值→stock_value, 现金结余→cash_value,
    账户净值→total_value, 股票仓位占比→stock_weight, 现金占比→cash_weight,
    总份额→shares, 净值→nav, 资金变动→fund_flow, 份额变动→share_change,
    当月净值涨幅→mtd_nav_change, 当年净值涨幅→ytd_nav_change,
    当期资产升值→pnl, 当月资产升值→mtd_pnl, 当年资产升值→ytd_pnl
    """
    record_id: Optional[str] = None
    date: date
    account: str

    # 市值分解
    total_value: float
    cash_value: Optional[float] = None
    stock_value: Optional[float] = None
    fund_value: Optional[float] = None

    # 区域分布
    cn_stock_value: Optional[float] = None
    us_stock_value: Optional[float] = None
    hk_stock_value: Optional[float] = None

    # 仓位占比
    stock_weight: Optional[float] = None
    cash_weight: Optional[float] = None

    # 份额与净值
    shares: Optional[float] = None
    nav: Optional[float] = None

    # 资金流动
    cash_flow: Optional[float] = None
    share_change: Optional[float] = None

    # 收益
    mtd_nav_change: Optional[float] = None
    ytd_nav_change: Optional[float] = None
    pnl: Optional[float] = None
    mtd_pnl: Optional[float] = None
    ytd_pnl: Optional[float] = None

    # 扩展计算数据（各年份明细等）
    details: Optional[Dict[str, Any]] = None

    @field_validator(
        'total_value', 'cash_value', 'stock_value', 'fund_value',
        'cn_stock_value', 'us_stock_value', 'hk_stock_value',
        'shares', 'cash_flow', 'share_change', 'pnl', 'mtd_pnl', 'ytd_pnl',
        mode='before'
    )
    @classmethod
    def quantize_money_fields(cls, v):
        # keep None as None; do not manufacture 0
        if v is None:
            return None
        return _quantize_decimal(v, MONEY_QUANT)

    @field_validator('nav', 'mtd_nav_change', 'ytd_nav_change', mode='before')
    @classmethod
    def quantize_nav_fields(cls, v):
        return _quantize_decimal(v, NAV_QUANT)

    @field_validator('stock_weight', 'cash_weight', mode='before')
    @classmethod
    def quantize_weight_fields(cls, v):
        return _quantize_decimal(v, WEIGHT_QUANT)


class PortfolioValuation(BaseModel):
    """组合估值结果（运行时计算，不持久化）"""
    account: str

    # 总市值
    total_value_cny: float = 0.0

    # 分类市值
    cash_value_cny: float = 0.0
    stock_value_cny: float = 0.0
    fund_value_cny: float = 0.0

    # 市场分布
    cn_asset_value: float = 0.0
    us_asset_value: float = 0.0
    hk_asset_value: float = 0.0

    # 份额与净值
    shares: Optional[float] = None
    nav: Optional[float] = None

    # 持仓明细
    holdings: List[Holding] = Field(default_factory=list)

    # 估值告警（如分类兜底、价格缺失、缓存回退等）
    warnings: List[str] = Field(default_factory=list)

    @field_validator(
        'total_value_cny', 'cash_value_cny', 'stock_value_cny', 'fund_value_cny',
        'cn_asset_value', 'us_asset_value', 'hk_asset_value', 'shares',
        mode='before'
    )
    @classmethod
    def quantize_money_fields(cls, v):
        return _quantize_decimal(v, MONEY_QUANT)

    @field_validator('nav', mode='before')
    @classmethod
    def quantize_nav_field(cls, v):
        return _quantize_decimal(v, NAV_QUANT)

    @property
    def cash_ratio(self) -> float:
        return self.cash_value_cny / self.total_value_cny if self.total_value_cny > 0 else 0

    @property
    def stock_ratio(self) -> float:
        return self.stock_value_cny / self.total_value_cny if self.total_value_cny > 0 else 0

    @property
    def fund_ratio(self) -> float:
        return self.fund_value_cny / self.total_value_cny if self.total_value_cny > 0 else 0


# ========== 防重工具函数 ==========

def make_tx_dedup_key(tx: Transaction) -> str:
    """生成交易记录的防重指纹

    设计目标：
    - 允许“同一天同一笔资产多笔交易”。
    - 默认幂等仍成立：相同 request_id 视为同一笔请求；无 request_id 时，通过 dedup_key 抑制“完全重复提交”。

    规则：
    - 若调用方提供 request_id：dedup_key 基于 request_id（稳定、可重放、且天然区分多笔）。
    - 否则：dedup_key 基于 (account, tx_date, tx_type, asset_id, quantity, price, fee)。

    说明：
    - 这样同一天同一资产但不同数量/价格/手续费的多笔交易会自然区分。
    - 若需要区分“完全相同的两笔”（例如拆单但参数完全一致），应显式传入不同的 request_id。
    """
    if tx.request_id:
        raw = f"RID|{tx.account}|{tx.request_id}"
    else:
        raw = f"{tx.account}|{tx.tx_date}|{tx.tx_type.value}|{tx.asset_id}|{tx.quantity}|{tx.price}|{tx.fee}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def make_request_id(prefix: str = "tx") -> str:
    """生成一个可用于交易幂等性的 request_id。

    用途：当用户/上层调用未提供 request_id 时，Skill 可自动生成，
    使“同一天同一资产多笔交易”可写入，同时仍可避免无意的重复提交。

    说明：这里使用时间戳作为轻量唯一性来源（避免引入额外依赖）。
    """
    return f"{prefix}_{uuid.uuid4().hex}"


def make_cf_dedup_key(cf: CashFlow) -> str:
    """生成出入金记录的防重指纹

    基于 (account, flow_date, flow_type, amount, currency) 生成 SHA256 前16位。
    同一天、同金额、同币种的出入金会生成相同的 key。
    """
    raw = f"{cf.account}|{cf.flow_date}|{cf.flow_type}|{cf.amount}|{cf.currency}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]
