"""报表/估值口径统一工具函数。"""
from __future__ import annotations

from typing import Literal, Optional

from .models import AssetType, Holding

NormalizedType = Literal["cash", "fund", "stock", "other"]


def is_cash_like(asset_type: Optional[AssetType | str], asset_id: str = "") -> bool:
    t = asset_type.value if hasattr(asset_type, 'value') else asset_type
    asset_id = (asset_id or "").upper()
    if t in (AssetType.CASH.value, AssetType.MMF.value, "cash", "mmf"):
        return True
    if asset_id.endswith("-CASH") or asset_id.endswith("-MMF"):
        return True
    return False


def normalize_asset_type(asset_type: Optional[AssetType | str], asset_id: str = "") -> NormalizedType:
    t = asset_type.value if hasattr(asset_type, 'value') else asset_type
    if is_cash_like(t, asset_id):
        return "cash"
    if t in (
        AssetType.FUND.value,
        AssetType.EXCHANGE_FUND.value,
        AssetType.OTC_FUND.value,
        AssetType.CN_FUND.value,
        AssetType.US_FUND.value,
        AssetType.HK_FUND.value,
        "fund",
        "exchange_fund",
        "otc_fund",
        "cn_fund",
        "us_fund",
        "hk_fund",
    ):
        return "fund"
    if t in (AssetType.A_STOCK.value, AssetType.HK_STOCK.value, AssetType.US_STOCK.value,
             AssetType.BOND.value, AssetType.CRYPTO.value,
             "a_stock", "hk_stock", "us_stock", "bond", "crypto"):
        return "stock"
    return "other"


def normalize_holding_type(holding: Holding) -> NormalizedType:
    return normalize_asset_type(holding.asset_type, holding.asset_id)


def normalization_warning(asset_type: Optional[AssetType | str], asset_id: str = "") -> Optional[str]:
    """当代码对资产类型做了兜底修正时，返回 warning 文案。"""
    t = asset_type.value if hasattr(asset_type, 'value') else asset_type
    normalized = normalize_asset_type(t, asset_id)
    if normalized == 'cash' and t not in (AssetType.CASH.value, AssetType.MMF.value, 'cash', 'mmf'):
        aid = (asset_id or '').upper()
        if aid.endswith('-CASH') or aid.endswith('-MMF'):
            return f"{asset_id}: 原始 asset_type={t or '空'}，按代码后缀归一为 cash"
    return None
