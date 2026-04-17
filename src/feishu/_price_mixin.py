"""Price cache operations mixin for FeishuStorage."""
from typing import Dict, List, Optional

from ..models import PriceCache, AssetType


class PriceMixin:
    """Price cache CRUD — delegates entirely to LocalPriceCache."""

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
