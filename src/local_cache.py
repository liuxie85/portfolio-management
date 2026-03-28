"""
本地价格缓存模块

从 feishu_storage.py 提取。使用本地 JSON 文件存储价格缓存，
替代飞书多维表存储，节省 API 配额。
"""
import json
import threading
import time
from datetime import datetime

from .time_utils import bj_now_naive
from pathlib import Path
from typing import Dict, List, Optional

from .models import AssetType, PriceCache, DATETIME_FORMAT

# 默认缓存文件路径
PRICE_CACHE_FILE = Path(__file__).parent.parent / '.data' / 'price_cache.json'

# 延迟写入配置
FLUSH_INTERVAL_SECONDS = 5  # 5秒自动刷盘
FLUSH_MAX_DIRTY_COUNT = 10   # 累积10条变更立即刷盘


class LocalPriceCache:
    """本地文件价格缓存（替代飞书多维表存储）

    特性：
    1. 零 API 调用 - 本地文件读写
    2. 低延迟 - 无需网络请求
    3. 自动过期清理
    4. 线程安全 - 使用锁保护并发访问
    5. 延迟写入 - 批量写入减少 I/O
    """

    def __init__(self, cache_file: Path = PRICE_CACHE_FILE):
        self.cache_file = cache_file
        self._cache: Dict[str, Dict] = {}
        self._lock = threading.Lock()
        self._dirty_count = 0  # 未保存的变更计数
        self._dirty_flag = False  # 是否有未保存的变更
        self._flush_timer: Optional[threading.Timer] = None
        self._shutdown = False
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
            self._dirty_count = 0
            self._dirty_flag = False
        except IOError as e:
            print(f"[警告] 保存本地价格缓存失败: {e}")

    def _schedule_flush(self):
        """调度延迟写入（无锁版本，需在锁内调用）"""
        if self._shutdown:
            return
        if self._flush_timer is None or not self._flush_timer.is_alive():
            self._flush_timer = threading.Timer(FLUSH_INTERVAL_SECONDS, self._flush_delayed)
            self._flush_timer.daemon = True
            self._flush_timer.start()

    def _flush_delayed(self):
        """延迟写入回调"""
        with self._lock:
            if self._dirty_flag:
                self._save_unlocked()
            self._flush_timer = None

    def flush(self):
        """强制刷盘 - 线程安全"""
        with self._lock:
            if self._dirty_flag:
                self._save_unlocked()
            # 取消定时器
            if self._flush_timer and self._flush_timer.is_alive():
                self._flush_timer.cancel()
                self._flush_timer = None

    def close(self):
        """关闭缓存，刷盘并清理资源"""
        self._shutdown = True
        self.flush()

    def __del__(self):
        """析构时确保数据写入"""
        self.close()

    def get(self, asset_id: str, *, allow_expired: bool = False, max_stale_after_expiry_sec: int = 0) -> Optional[PriceCache]:
        """获取价格缓存 - 线程安全

        默认行为：过期即删除并返回 None。

        Args:
            allow_expired: True 时允许返回过期缓存（用于非交易时段“稳定优先”的估值/报表）
            max_stale_after_expiry_sec: 允许过期后最多多少秒仍可返回（0=不允许）
        """
        with self._lock:
            data = self._cache.get(asset_id)
            if not data:
                return None

            expires_at = data.get('expires_at', '')
            now_dt = bj_now_naive()
            now = now_dt.strftime(DATETIME_FORMAT)

            if expires_at and expires_at < now:
                if not allow_expired:
                    self._delete_unlocked(asset_id, _flush=True)
                    return None
                # allow_expired: only allow within window
                try:
                    exp_dt = datetime.strptime(expires_at, DATETIME_FORMAT)
                    age = (now_dt - exp_dt).total_seconds()
                    if age < 0:
                        age = 0
                    if max_stale_after_expiry_sec <= 0:
                        return None
                    if age > max_stale_after_expiry_sec:
                        return None
                except Exception:
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

    def save(self, price: PriceCache, *, _flush: bool = False):
        """保存价格缓存 - 线程安全（延迟写入）

        Args:
            price: 价格缓存对象
            _flush: 内部标志，True 时立即写入（用于批量操作后的最后一次）
        """
        # 再过一次模型校验，避免旁路构造的价格对象把脏精度写入本地缓存
        price = PriceCache(**price.model_dump())
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
                'updated_at': bj_now_naive().strftime(DATETIME_FORMAT)
            }
            self._dirty_count += 1
            self._dirty_flag = True

            # 满足任一条件时立即写入：
            # 1. 调用方要求立即写入 (_flush=True)
            # 2. 累积变更达到阈值
            if _flush or self._dirty_count >= FLUSH_MAX_DIRTY_COUNT:
                self._save_unlocked()
            else:
                # 否则调度延迟写入
                self._schedule_flush()

    def _delete_unlocked(self, asset_id: str, *, _flush: bool = False):
        """删除价格缓存（无锁版本，需在锁内调用）"""
        if asset_id in self._cache:
            del self._cache[asset_id]
            self._dirty_count += 1
            self._dirty_flag = True

            # 根据条件决定是否立即写入
            if _flush or self._dirty_count >= FLUSH_MAX_DIRTY_COUNT:
                self._save_unlocked()
            else:
                self._schedule_flush()

    def delete(self, asset_id: str):
        """删除价格缓存 - 线程安全"""
        with self._lock:
            self._delete_unlocked(asset_id)

    def get_all(self) -> List[PriceCache]:
        """获取所有未过期的价格缓存 - 线程安全"""
        with self._lock:
            results = []
            now = bj_now_naive().strftime(DATETIME_FORMAT)
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

            # 清理过期数据（批量删除，最后统一刷盘）
            for asset_id in expired_ids:
                self._cache.pop(asset_id, None)
            if expired_ids:
                self._dirty_count += len(expired_ids)
                self._dirty_flag = True
                # 批量清理后立即刷盘，避免数据不一致
                self._save_unlocked()

            return results

    def clear_expired(self):
        """清理所有过期缓存 - 线程安全"""
        with self._lock:
            now = bj_now_naive().strftime(DATETIME_FORMAT)
            expired_ids = [
                asset_id for asset_id, data in self._cache.items()
                if data.get('expires_at') and data['expires_at'] < now
            ]
            for asset_id in expired_ids:
                self._cache.pop(asset_id, None)
            if expired_ids:
                self._dirty_count += len(expired_ids)
                self._dirty_flag = True
                self._save_unlocked()
                print(f"[本地缓存] 清理 {len(expired_ids)} 条过期价格缓存")
