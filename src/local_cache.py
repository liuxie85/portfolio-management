"""
本地价格缓存模块

从 feishu_storage.py 提取。使用本地 JSON 文件存储价格缓存，
替代飞书多维表存储，节省 API 配额。
"""
import json
import threading
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from .models import AssetType, PriceCache, DATETIME_FORMAT

# 默认缓存文件路径
PRICE_CACHE_FILE = Path(__file__).parent.parent / '.data' / 'price_cache.json'


class LocalPriceCache:
    """本地文件价格缓存（替代飞书多维表存储）

    特性：
    1. 零 API 调用 - 本地文件读写
    2. 低延迟 - 无需网络请求
    3. 自动过期清理
    4. 线程安全 - 使用锁保护并发访问
    """

    def __init__(self, cache_file: Path = PRICE_CACHE_FILE):
        self.cache_file = cache_file
        self._cache: Dict[str, Dict] = {}
        self._lock = threading.Lock()
        self._load()

    def _load_unlocked(self):
        """从文件加载缓存（无锁版本，需在锁内调用）

        使用 EAFP 模式避免 TOCTOU 竞争条件
        """
        try:
            with open(self.cache_file, 'r', encoding='utf-8') as f:
                self._cache = json.load(f)
        except FileNotFoundError:
            self._cache = {}
        except (json.JSONDecodeError, IOError):
            self._cache = {}

    def _load(self):
        """从文件加载缓存 - 线程安全"""
        with self._lock:
            self._load_unlocked()

    def _save_unlocked(self):
        """保存缓存到文件（无锁版本，需在锁内调用）"""
        try:
            self.cache_file.parent.mkdir(parents=True, exist_ok=True)
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump(self._cache, f, ensure_ascii=False, indent=2)
        except IOError as e:
            print(f"[警告] 保存本地价格缓存失败: {e}")

    def get(self, asset_id: str) -> Optional[PriceCache]:
        """获取价格缓存（检查有效期）- 线程安全"""
        with self._lock:
            data = self._cache.get(asset_id)
            if not data:
                return None

            # 检查过期时间
            expires_at = data.get('expires_at', '')
            now = datetime.now().strftime(DATETIME_FORMAT)

            if expires_at and expires_at < now:
                self._delete_unlocked(asset_id)
                return None

            return PriceCache(
                asset_id=data.get('asset_id', ''),
                asset_name=data.get('asset_name'),
                asset_type=AssetType(data.get('asset_type')) if data.get('asset_type') else AssetType.OTHER,
                price=float(data.get('price', 0)),
                currency=data.get('currency', 'CNY'),
                cny_price=float(data.get('cny_price')) if data.get('cny_price') else None,
                change=float(data.get('change')) if data.get('change') else None,
                change_pct=float(data.get('change_pct')) if data.get('change_pct') else None,
                exchange_rate=float(data.get('exchange_rate')) if data.get('exchange_rate') else None,
                data_source=data.get('data_source'),
                expires_at=expires_at if expires_at else None
            )

    def save(self, price: PriceCache):
        """保存价格缓存 - 线程安全"""
        expires_at_str = None
        if price.expires_at:
            if isinstance(price.expires_at, datetime):
                expires_at_str = price.expires_at.strftime(DATETIME_FORMAT)
            else:
                expires_at_str = price.expires_at

        with self._lock:
            self._cache[price.asset_id] = {
                'asset_id': price.asset_id,
                'asset_name': price.asset_name,
                'asset_type': price.asset_type.value if price.asset_type else None,
                'price': price.price,
                'currency': price.currency,
                'cny_price': price.cny_price,
                'change': price.change,
                'change_pct': price.change_pct,
                'exchange_rate': price.exchange_rate,
                'data_source': price.data_source,
                'expires_at': expires_at_str,
                'updated_at': datetime.now().strftime(DATETIME_FORMAT)
            }
            self._save_unlocked()

    def _delete_unlocked(self, asset_id: str):
        """删除价格缓存（无锁版本，需在锁内调用）"""
        if asset_id in self._cache:
            del self._cache[asset_id]
            self._save_unlocked()

    def delete(self, asset_id: str):
        """删除价格缓存 - 线程安全"""
        with self._lock:
            self._delete_unlocked(asset_id)

    def get_all(self) -> List[PriceCache]:
        """获取所有未过期的价格缓存 - 线程安全"""
        with self._lock:
            results = []
            now = datetime.now().strftime(DATETIME_FORMAT)
            expired_ids = []

            cache_items = list(self._cache.items())

            for asset_id, data in cache_items:
                expires_at = data.get('expires_at', '')
                if expires_at and expires_at < now:
                    expired_ids.append(asset_id)
                    continue

                try:
                    results.append(PriceCache(
                        asset_id=data.get('asset_id', ''),
                        asset_name=data.get('asset_name'),
                        asset_type=AssetType(data.get('asset_type')) if data.get('asset_type') else AssetType.OTHER,
                        price=float(data.get('price', 0)),
                        currency=data.get('currency', 'CNY'),
                        cny_price=float(data.get('cny_price')) if data.get('cny_price') else None,
                        change=float(data.get('change')) if data.get('change') else None,
                        change_pct=float(data.get('change_pct')) if data.get('change_pct') else None,
                        exchange_rate=float(data.get('exchange_rate')) if data.get('exchange_rate') else None,
                        data_source=data.get('data_source'),
                        expires_at=expires_at if expires_at else None
                    ))
                except (ValueError, TypeError):
                    continue

            # 清理过期数据
            for asset_id in expired_ids:
                self._cache.pop(asset_id, None)
            if expired_ids:
                self._save_unlocked()

            return results

    def clear_expired(self):
        """清理所有过期缓存 - 线程安全"""
        with self._lock:
            now = datetime.now().strftime(DATETIME_FORMAT)
            expired_ids = [
                asset_id for asset_id, data in self._cache.items()
                if data.get('expires_at') and data['expires_at'] < now
            ]
            for asset_id in expired_ids:
                self._cache.pop(asset_id, None)
            if expired_ids:
                self._save_unlocked()
                print(f"[本地缓存] 清理 {len(expired_ids)} 条过期价格缓存")
