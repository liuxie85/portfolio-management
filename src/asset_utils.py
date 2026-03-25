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


def normalize_code(code: str) -> str:
    """标准化资产代码格式

    - 港股: 补齐为5位 (700 -> 00700)
    - A股/基金: 保持6位
    - 美股/现金: 保持原样
    """
    code = code.strip().upper()

    if _is_cash_code(code):
        return code

    if code.isdigit():
        if len(code) <= 4:
            return code.zfill(5)  # 港股补齐
        return code

    return code


def validate_code(code: str) -> str:
    """校验资产代码格式，返回标准化后的大写代码

    Raises:
        InvalidAssetCodeError: 代码格式不正确
    """
    if not code:
        raise InvalidAssetCodeError("资产代码不能为空")

    code = code.strip().upper()

    if _is_cash_code(code):
        return code

    if code.isdigit():
        if len(code) == 6:
            return code
        elif len(code) == 5:
            return code
        elif 1 <= len(code) <= 4:
            # 港股可能输入 700、1810 等，自动补零为5位
            return code.zfill(5)
        else:
            raise InvalidAssetCodeError(f"代码 {code} 格式不正确。数字代码超过6位")

    if code.isalpha():
        return code

    if re.match(r'^[A-Z0-9.\-]+$', code):
        return code

    raise InvalidAssetCodeError(
        f"代码 {code} 格式不正确。支持的格式：\n"
        f"  - A股/基金: 6位数字 (如 000001)\n"
        f"  - 港股: 5位数字 (如 00001)\n"
        f"  - 美股: 字母代码 (如 AAPL)\n"
        f"  - 现金: CNY-CASH, USD-CASH 等"
    )


def detect_asset_type(code: str) -> Tuple[AssetType, str, AssetClass]:
    """检测资产类型、币种和资产类别

    Returns:
        (AssetType, currency, AssetClass)
    """
    code = code.strip().upper()

    # 现金/货币基金
    if code.endswith('-CASH'):
        currency = code.split('-')[0]
        if currency == 'HKD':
            return AssetType.CASH, 'HKD', AssetClass.HK_ASSET
        return AssetType.CASH, currency, AssetClass.CASH

    if code.endswith('-MMF'):
        currency = code.split('-')[0]
        return AssetType.MMF, currency, AssetClass.CASH

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

    Returns:
        'cn', 'hk', 'us', 'fund', 或 None
    """
    code = code.strip().upper()

    if _is_cash_code(code):
        return None

    # 带前缀的代码
    if code.startswith(('SH', 'SZ')):
        return 'cn'
    if code.startswith('HK'):
        return 'hk'

    # 纯数字
    if code.isdigit():
        if len(code) == 5 or (len(code) == 4):
            return 'hk'
        if len(code) == 6:
            # A股：沪市(60x/688/689) + 深市主板(000/001) + 中小板(002/003) + 创业板(300/301)
            if code.startswith(('600', '601', '603', '605', '688', '689', '000', '001', '002', '003', '300', '301')):
                return 'cn'
            # 场内ETF
            if code.startswith('5') or code.startswith('15'):
                return 'cn'
            # 场外基金
            return 'fund'

    # 非数字 -> 美股
    if not code.isdigit():
        return 'us'

    return None


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
