"""
资产代码工具模块

统一管理资产代码的标准化、校验、类型检测。
合并了原 skill_api.py、portfolio.py、price_fetcher.py 中的重复逻辑。
"""
import re
from datetime import datetime

from .time_utils import bj_today
from typing import Optional, Tuple

from .models import AssetType, AssetClass


class InvalidAssetCodeError(ValueError):
    """资产代码格式错误"""
    pass


def _split_market_suffix(code: str) -> Tuple[str, Optional[str]]:
    """Split known market suffix like .US/.HK/.SH/.SZ.

    Returns:
        (base, suffix) where suffix is one of {'US','HK','SH','SZ'} or None.

    Notes:
    - We only treat a final ".XX" as suffix; internal dots in US tickers (e.g. BRK.B) are preserved.
    """
    c = (code or '').strip().upper()
    m = re.match(r"^(.*)\.(US|HK|SH|SZ)$", c)
    if m:
        return m.group(1), m.group(2)
    return c, None


def normalize_code(code: str) -> str:
    """标准化资产代码格式（尽量归一为 portfolio-management 内部约定）

    - 支持带后缀的输入：FUTU.US / 0700.HK / 600519.SH
    - 港股: 补齐为5位 (700 -> 00700)
    - A股/基金: 保持6位
    - 美股/现金: 保持原样（保留 ticker 内部点号，如 BRK.B）
    """
    code = (code or '').strip().upper()

    if _is_cash_code(code):
        return code

    base, suffix = _split_market_suffix(code)

    # If user passed HK prefix like HK700 / HK00700
    if base.startswith('HK') and base[2:].isdigit():
        base = base[2:]
        suffix = suffix or 'HK'

    # Normalize numeric part
    if base.isdigit():
        if suffix == 'HK' or len(base) <= 4:
            return base.zfill(5)
        return base

    # Non-numeric (US ticker etc.)
    return base


def validate_code(code: str) -> str:
    """校验资产代码格式，返回标准化后的内部代码（大写，去掉常见市场后缀）

    Examples:
    - FUTU.US -> FUTU
    - 0700.HK / 700.HK / HK700 -> 00700
    - 600519.SH -> 600519

    Raises:
        InvalidAssetCodeError: 代码格式不正确
    """
    if not code:
        raise InvalidAssetCodeError("资产代码不能为空")

    code = (code or '').strip().upper()

    if _is_cash_code(code):
        return code

    base, suffix = _split_market_suffix(code)

    # Allow HK prefix form: HK700 / HK00700
    if base.startswith('HK') and base[2:].isdigit():
        base = base[2:]
        suffix = suffix or 'HK'

    # Numeric codes
    if base.isdigit():
        if len(base) == 6:
            return base
        if len(base) == 5:
            return base
        if 1 <= len(base) <= 4:
            # 港股可能输入 700、1810 等，自动补零为5位
            return base.zfill(5)
        raise InvalidAssetCodeError(f"代码 {code} 格式不正确。数字代码超过6位")

    # Pure alpha US ticker
    if base.isalpha():
        return base

    # Alnum ticker with dot/dash (e.g. BRK.B)
    if re.match(r'^[A-Z0-9.\-]+$', base):
        return base

    raise InvalidAssetCodeError(
        f"代码 {code} 格式不正确。支持的格式：\n"
        f"  - A股/基金: 6位数字 (如 000001 或 600519.SH)\n"
        f"  - 港股: 5位数字 (如 00700 或 0700.HK)\n"
        f"  - 美股: 字母/带点代码 (如 AAPL, BRK.B, FUTU.US)\n"
        f"  - 现金: CNY-CASH, USD-CASH 等"
    )


def detect_asset_type(code: str) -> Tuple[AssetType, str, AssetClass]:
    """检测资产类型、币种和资产类别

    Important:
    - Respect explicit suffix if provided (e.g. FUTU.US / 0700.HK / 600519.SH).
    - Otherwise, fall back to heuristic detection.

    Returns:
        (AssetType, currency, AssetClass)
    """
    code = (code or '').strip().upper()

    # 现金/货币基金
    if code.endswith('-CASH'):
        currency = code.split('-')[0]
        if currency == 'HKD':
            return AssetType.CASH, 'HKD', AssetClass.HK_ASSET
        return AssetType.CASH, currency, AssetClass.CASH

    if code.endswith('-MMF'):
        currency = code.split('-')[0]
        return AssetType.MMF, currency, AssetClass.CASH

    base, suffix = _split_market_suffix(code)

    # If suffix is explicit, we can decide market/currency deterministically.
    if suffix == 'US':
        return AssetType.US_STOCK, 'USD', AssetClass.US_ASSET
    if suffix == 'HK':
        return AssetType.HK_STOCK, 'HKD', AssetClass.HK_ASSET
    if suffix in ('SH', 'SZ'):
        # treat as CN asset; distinguish stock/fund by base prefix
        b = base
        if b.startswith(('6', '5')):
            # 5xxxxx are ETFs (fund) but still CN asset class; keep A_STOCK vs FUND as before
            if b.startswith('5'):
                return AssetType.FUND, 'CNY', AssetClass.CN_ASSET
            return AssetType.A_STOCK, 'CNY', AssetClass.CN_ASSET
        # SZ
        if b.startswith('15'):
            return AssetType.FUND, 'CNY', AssetClass.CN_ASSET
        return AssetType.A_STOCK, 'CNY', AssetClass.CN_ASSET

    # 标准化后判断
    normalized = normalize_code(code)

    # 5位纯数字 -> 港股
    if normalized.isdigit() and len(normalized) == 5:
        return AssetType.HK_STOCK, 'HKD', AssetClass.HK_ASSET

    # 6位纯数字
    if normalized.isdigit() and len(normalized) == 6:
        # A股：沪市(60x/688/689科创板) + 深市主板(000/001) + 中小板(002/003) + 创业板(300/301)
        if normalized.startswith(('600', '601', '603', '605', '688', '689', '000', '001', '002', '003', '300', '301')):
            return AssetType.A_STOCK, 'CNY', AssetClass.CN_ASSET
        # 场内ETF：沪市(51x) + 深市(15x)
        if normalized.startswith('5') or normalized.startswith('15'):
            return AssetType.FUND, 'CNY', AssetClass.CN_ASSET
        # 其余6位数字 -> 场外基金
        return AssetType.FUND, 'CNY', AssetClass.CN_ASSET

    # 非数字 -> 美股
    return AssetType.US_STOCK, 'USD', AssetClass.US_ASSET


def detect_market_type(code: str) -> Optional[str]:
    """检测市场类型，用于缓存策略

    Precedence:
    1) Explicit suffix .US/.HK/.SH/.SZ
    2) Prefix SH/SZ/HK
    3) Heuristics on normalized numeric/alpha codes

    Returns:
        'cn', 'hk', 'us', 'fund', or None
    """
    code = (code or '').strip().upper()

    if _is_cash_code(code):
        return None

    base, suffix = _split_market_suffix(code)
    if suffix == 'US':
        return 'us'
    if suffix == 'HK':
        return 'hk'
    if suffix in ('SH', 'SZ'):
        return 'cn'

    # 带前缀的代码
    if code.startswith(('SH', 'SZ')):
        return 'cn'
    if code.startswith('HK'):
        return 'hk'

    normalized = normalize_code(code)

    # 纯数字
    if normalized.isdigit():
        if len(normalized) == 5:
            return 'hk'
        if len(normalized) == 6:
            # A股：沪市(60x/688/689) + 深市主板(000/001) + 中小板(002/003) + 创业板(300/301)
            if normalized.startswith(('600', '601', '603', '605', '688', '689', '000', '001', '002', '003', '300', '301')):
                return 'cn'
            # 场内ETF
            if normalized.startswith('5') or normalized.startswith('15'):
                return 'cn'
            # 场外基金
            return 'fund'

    # 非数字 -> 美股
    return 'us'


def parse_date(date_str: Optional[str]):
    """解析日期字符串，空值返回今天（北京时间）"""
    if not date_str:
        return bj_today()
    try:
        return datetime.strptime(date_str, '%Y-%m-%d').date()
    except ValueError:
        print(f"日期格式错误: {date_str}, 使用今天(北京时间)")
        return bj_today()


def _is_cash_code(code: str) -> bool:
    """判断是否是现金/货币基金代码"""
    return code.endswith('-CASH') or code.endswith('-MMF')
