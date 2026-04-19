"""
本地价格缓存模块

从 feishu_storage.py 提取。使用本地 JSON 文件存储价格缓存，
替代飞书多维表存储，节省 API 配额。
"""
import json
import threading
import time
from datetime import date, datetime

from .time_utils import bj_now_naive
from pathlib import Path
from typing import Any, Dict, List, Optional

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
        try:
            self.close()
        except Exception:
            pass

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
                cny_price=float(data['cny_price']) if data.get('cny_price') is not None else None,
                change=float(data['change']) if data.get('change') is not None else None,
                change_pct=float(data['change_pct']) if data.get('change_pct') is not None else None,
                exchange_rate=float(data['exchange_rate']) if data.get('exchange_rate') is not None else None,
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
                        cny_price=float(data['cny_price']) if data.get('cny_price') is not None else None,
                        change=float(data['change']) if data.get('change') is not None else None,
                        change_pct=float(data['change_pct']) if data.get('change_pct') is not None else None,
                        exchange_rate=float(data['exchange_rate']) if data.get('exchange_rate') is not None else None,
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


# 默认持仓索引缓存文件路径
HOLDINGS_INDEX_FILE = Path(__file__).parent.parent / '.data' / 'holdings_index.json'


class LocalHoldingsIndexCache:
    """本地持仓索引缓存（business_key -> fields snapshot）

    存储结构：
    {
      "version": 1,
      "items": {
        "asset:account:market": {
          "record_id": "rec_xxx",
          "quantity": 100,
          "asset_type": "a_stock",
          "asset_name": "平安银行",
          "currency": "CNY",
          "avg_cost": 12.3,
          "updated_at": "2026-03-29 12:00:00"
        }
      }
    }
    """

    VERSION = 1

    def __init__(self, cache_file: Path = HOLDINGS_INDEX_FILE):
        self.cache_file = cache_file
        self._cache: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.Lock()
        self._dirty_count = 0
        self._dirty_flag = False
        self._flush_timer: Optional[threading.Timer] = None
        self._shutdown = False
        self._load()

    def _load_unlocked(self):
        try:
            with open(self.cache_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            if isinstance(data, dict) and isinstance(data.get('items'), dict):
                self._cache = data['items']
            elif isinstance(data, dict):
                # 兼容旧格式：直接是 key->item
                self._cache = data
            else:
                self._cache = {}
        except FileNotFoundError:
            self._cache = {}
        except (json.JSONDecodeError, IOError):
            self._cache = {}

    def _load(self):
        with self._lock:
            self._load_unlocked()

    def _save_unlocked(self):
        try:
            self.cache_file.parent.mkdir(parents=True, exist_ok=True)
            payload = {
                'version': self.VERSION,
                'saved_at': bj_now_naive().strftime(DATETIME_FORMAT),
                'items': self._cache,
            }
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
            self._dirty_count = 0
            self._dirty_flag = False
        except IOError as e:
            print(f"[警告] 保存本地持仓索引缓存失败: {e}")

    def _schedule_flush(self):
        if self._shutdown:
            return
        if self._flush_timer is None or not self._flush_timer.is_alive():
            self._flush_timer = threading.Timer(FLUSH_INTERVAL_SECONDS, self._flush_delayed)
            self._flush_timer.daemon = True
            self._flush_timer.start()

    def _flush_delayed(self):
        with self._lock:
            if self._dirty_flag:
                self._save_unlocked()
            self._flush_timer = None

    def flush(self):
        with self._lock:
            if self._dirty_flag:
                self._save_unlocked()
            if self._flush_timer and self._flush_timer.is_alive():
                self._flush_timer.cancel()
                self._flush_timer = None

    def close(self):
        self._shutdown = True
        self.flush()

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass

    def load_all(self) -> Dict[str, Dict[str, Any]]:
        with self._lock:
            return {k: dict(v) for k, v in self._cache.items()}

    def upsert(self, cache_key: str, payload: Dict[str, Any], *, _flush: bool = False):
        with self._lock:
            self._cache[cache_key] = dict(payload)
            self._dirty_count += 1
            self._dirty_flag = True
            if _flush or self._dirty_count >= FLUSH_MAX_DIRTY_COUNT:
                self._save_unlocked()
            else:
                self._schedule_flush()

    def delete(self, cache_key: str, *, _flush: bool = False):
        with self._lock:
            if cache_key not in self._cache:
                return
            self._cache.pop(cache_key, None)
            self._dirty_count += 1
            self._dirty_flag = True
            if _flush or self._dirty_count >= FLUSH_MAX_DIRTY_COUNT:
                self._save_unlocked()
            else:
                self._schedule_flush()


# 默认净值索引缓存文件路径
NAV_INDEX_FILE = Path(__file__).parent.parent / '.data' / 'nav_index_cache.json'


class LocalNavIndexCache:
    """本地 NAV 索引缓存。

    按 account 存储：
    - nav_history 轻量投影（用于快速回放）
    - month_end_base / year_end_base / inception_base
    - last_record

    说明：
    - append_nav_record 仅适合“新增日期”场景；
    - upsert_nav_record(s) 支持按 date 替换或新增（用于批量回填/修复）。
    """

    VERSION = 1

    def __init__(self, cache_file: Path = NAV_INDEX_FILE):
        self.cache_file = cache_file
        self._cache: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.Lock()
        self._dirty_count = 0
        self._dirty_flag = False
        self._flush_timer: Optional[threading.Timer] = None
        self._shutdown = False
        self._load()

    def _load_unlocked(self):
        try:
            with open(self.cache_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            if isinstance(data, dict) and isinstance(data.get('accounts'), dict):
                self._cache = data['accounts']
            elif isinstance(data, dict):
                self._cache = data
            else:
                self._cache = {}
        except FileNotFoundError:
            self._cache = {}
        except (json.JSONDecodeError, IOError):
            self._cache = {}

    def _load(self):
        with self._lock:
            self._load_unlocked()

    def _save_unlocked(self):
        try:
            self.cache_file.parent.mkdir(parents=True, exist_ok=True)
            payload = {
                'version': self.VERSION,
                'saved_at': bj_now_naive().strftime(DATETIME_FORMAT),
                'accounts': self._cache,
            }
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
            self._dirty_count = 0
            self._dirty_flag = False
        except IOError as e:
            print(f"[警告] 保存本地 NAV 索引缓存失败: {e}")

    def _schedule_flush(self):
        if self._shutdown:
            return
        if self._flush_timer is None or not self._flush_timer.is_alive():
            self._flush_timer = threading.Timer(FLUSH_INTERVAL_SECONDS, self._flush_delayed)
            self._flush_timer.daemon = True
            self._flush_timer.start()

    def _flush_delayed(self):
        with self._lock:
            if self._dirty_flag:
                self._save_unlocked()
            self._flush_timer = None

    def flush(self):
        with self._lock:
            if self._dirty_flag:
                self._save_unlocked()
            if self._flush_timer and self._flush_timer.is_alive():
                self._flush_timer.cancel()
                self._flush_timer = None

    def close(self):
        self._shutdown = True
        self.flush()

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass

    @staticmethod
    def _parse_date(s: Any) -> Optional[date]:
        if not s or not isinstance(s, str):
            return None
        try:
            return datetime.strptime(s[:10], '%Y-%m-%d').date()
        except Exception:
            return None

    def get_account(self, account: str) -> Dict[str, Any]:
        with self._lock:
            data = self._cache.get(account)
            if not isinstance(data, dict):
                return {}
            return json.loads(json.dumps(data))

    def set_account(self, account: str, payload: Dict[str, Any], *, _flush: bool = False):
        with self._lock:
            self._cache[account] = json.loads(json.dumps(payload))
            self._dirty_count += 1
            self._dirty_flag = True
            if _flush or self._dirty_count >= FLUSH_MAX_DIRTY_COUNT:
                self._save_unlocked()
            else:
                self._schedule_flush()

    def update_account(self, account: str, patch: Dict[str, Any], *, _flush: bool = False):
        with self._lock:
            base = self._cache.get(account) if isinstance(self._cache.get(account), dict) else {}
            base = dict(base)
            base.update(json.loads(json.dumps(patch)))
            self._cache[account] = base
            self._dirty_count += 1
            self._dirty_flag = True
            if _flush or self._dirty_count >= FLUSH_MAX_DIRTY_COUNT:
                self._save_unlocked()
            else:
                self._schedule_flush()

    @classmethod
    def _rebuild_nav_account_payload(cls, base: Dict[str, Any], navs: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Rebuild derived account payload from nav_history rows."""
        navs_sorted = sorted([dict(r) for r in navs], key=lambda r: r.get('date') or '')

        month_end_base: Dict[str, Dict[str, Any]] = {}
        year_end_base: Dict[str, Dict[str, Any]] = {}
        for r in navs_sorted:
            d = cls._parse_date(r.get('date'))
            if not d:
                continue
            month_end_base[d.strftime('%Y-%m')] = dict(r)
            year_end_base[str(d.year)] = dict(r)

        inception_base = dict(navs_sorted[0]) if navs_sorted else None
        last_record = dict(navs_sorted[-1]) if navs_sorted else None

        out = dict(base or {})
        out['nav_history'] = navs_sorted
        out['month_end_base'] = month_end_base
        out['year_end_base'] = year_end_base
        out['inception_base'] = inception_base
        out['last_record'] = last_record
        out['record_count'] = len(navs_sorted)
        if last_record:
            out['latest_updated_at'] = last_record.get('updated_at')
        else:
            out['latest_updated_at'] = None
        return out

    def _save_account_navs_unlocked(self, account: str, navs: List[Dict[str, Any]], *, _flush: bool = False):
        base = self._cache.get(account) if isinstance(self._cache.get(account), dict) else {}
        rebuilt = self._rebuild_nav_account_payload(base, navs)
        self._cache[account] = rebuilt
        self._dirty_count += 1
        self._dirty_flag = True
        if _flush or self._dirty_count >= FLUSH_MAX_DIRTY_COUNT:
            self._save_unlocked()
        else:
            self._schedule_flush()

    def append_nav_record(self, account: str, record: Dict[str, Any], *, _flush: bool = False):
        with self._lock:
            base = self._cache.get(account) if isinstance(self._cache.get(account), dict) else {}
            navs = list(base.get('nav_history') or [])
            navs.append(dict(record))
            self._save_account_navs_unlocked(account, navs, _flush=_flush)

    def upsert_nav_record(self, account: str, record: Dict[str, Any], *, _flush: bool = False):
        """按 date upsert 单条 nav 记录（存在则替换，不存在则新增）。"""
        with self._lock:
            base = self._cache.get(account) if isinstance(self._cache.get(account), dict) else {}
            navs = list(base.get('nav_history') or [])
            ds = str((record or {}).get('date') or '')
            if not ds:
                return

            replaced = False
            for i, r in enumerate(navs):
                if str((r or {}).get('date') or '') == ds:
                    navs[i] = dict(record)
                    replaced = True
                    break
            if not replaced:
                navs.append(dict(record))
            self._save_account_navs_unlocked(account, navs, _flush=_flush)

    def upsert_nav_records(self, account: str, records: List[Dict[str, Any]], *, _flush: bool = False):
        """按 date 批量 upsert nav 记录（用于批量回填后增量刷新索引）。"""
        if not records:
            return
        with self._lock:
            base = self._cache.get(account) if isinstance(self._cache.get(account), dict) else {}
            navs = list(base.get('nav_history') or [])
            by_date: Dict[str, Dict[str, Any]] = {}
            for r in navs:
                ds = str((r or {}).get('date') or '')
                if ds:
                    by_date[ds] = dict(r)
            for r in records:
                ds = str((r or {}).get('date') or '')
                if not ds:
                    continue
                by_date[ds] = dict(r)
            merged = list(by_date.values())
            self._save_account_navs_unlocked(account, merged, _flush=_flush)


# 默认现金流聚合缓存文件路径
CASH_FLOW_AGG_FILE = Path(__file__).parent.parent / '.data' / 'cash_flow_agg_cache.json'


class LocalCashFlowAggCache:
    """本地现金流聚合缓存。

    按 account 存储：
    - monthly: YYYY-MM -> sum(cny_amount)
    - yearly: YYYY -> sum(cny_amount)
    - cumulative / last_record / flow_count
    """

    VERSION = 1

    def __init__(self, cache_file: Path = CASH_FLOW_AGG_FILE):
        self.cache_file = cache_file
        self._cache: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.Lock()
        self._dirty_count = 0
        self._dirty_flag = False
        self._flush_timer: Optional[threading.Timer] = None
        self._shutdown = False
        self._load()

    def _load_unlocked(self):
        try:
            with open(self.cache_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            if isinstance(data, dict) and isinstance(data.get('accounts'), dict):
                self._cache = data['accounts']
            elif isinstance(data, dict):
                self._cache = data
            else:
                self._cache = {}
        except FileNotFoundError:
            self._cache = {}
        except (json.JSONDecodeError, IOError):
            self._cache = {}

    def _load(self):
        with self._lock:
            self._load_unlocked()

    def _save_unlocked(self):
        try:
            self.cache_file.parent.mkdir(parents=True, exist_ok=True)
            payload = {
                'version': self.VERSION,
                'saved_at': bj_now_naive().strftime(DATETIME_FORMAT),
                'accounts': self._cache,
            }
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
            self._dirty_count = 0
            self._dirty_flag = False
        except IOError as e:
            print(f"[警告] 保存本地现金流聚合缓存失败: {e}")

    def _schedule_flush(self):
        if self._shutdown:
            return
        if self._flush_timer is None or not self._flush_timer.is_alive():
            self._flush_timer = threading.Timer(FLUSH_INTERVAL_SECONDS, self._flush_delayed)
            self._flush_timer.daemon = True
            self._flush_timer.start()

    def _flush_delayed(self):
        with self._lock:
            if self._dirty_flag:
                self._save_unlocked()
            self._flush_timer = None

    def flush(self):
        with self._lock:
            if self._dirty_flag:
                self._save_unlocked()
            if self._flush_timer and self._flush_timer.is_alive():
                self._flush_timer.cancel()
                self._flush_timer = None

    def close(self):
        self._shutdown = True
        self.flush()

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass

    def get_account(self, account: str) -> Dict[str, Any]:
        with self._lock:
            data = self._cache.get(account)
            if not isinstance(data, dict):
                return {}
            return json.loads(json.dumps(data))

    def set_account(self, account: str, payload: Dict[str, Any], *, _flush: bool = False):
        with self._lock:
            self._cache[account] = json.loads(json.dumps(payload))
            self._dirty_count += 1
            self._dirty_flag = True
            if _flush or self._dirty_count >= FLUSH_MAX_DIRTY_COUNT:
                self._save_unlocked()
            else:
                self._schedule_flush()

    def append_flow(self, account: str, flow_date: date, cny_amount: float, record_id: Optional[str], updated_at: Optional[str], *, _flush: bool = False):
        with self._lock:
            base = self._cache.get(account) if isinstance(self._cache.get(account), dict) else {}
            base = dict(base)
            daily = dict(base.get('daily') or {})
            monthly = dict(base.get('monthly') or {})
            yearly = dict(base.get('yearly') or {})

            ds = flow_date.strftime('%Y-%m-%d')
            ym = flow_date.strftime('%Y-%m')
            yy = flow_date.strftime('%Y')
            daily[ds] = float(daily.get(ds, 0.0) + (cny_amount or 0.0))
            monthly[ym] = float(monthly.get(ym, 0.0) + (cny_amount or 0.0))
            yearly[yy] = float(yearly.get(yy, 0.0) + (cny_amount or 0.0))

            base['daily'] = daily
            base['monthly'] = monthly
            base['yearly'] = yearly
            base['cumulative'] = float(base.get('cumulative', 0.0) + (cny_amount or 0.0))
            base['flow_count'] = int(base.get('flow_count', 0)) + 1
            base['last_record'] = {
                'date': flow_date.strftime('%Y-%m-%d'),
                'record_id': record_id,
                'updated_at': updated_at,
                'cny_amount': cny_amount,
            }

            self._cache[account] = base
            self._dirty_count += 1
            self._dirty_flag = True
            if _flush or self._dirty_count >= FLUSH_MAX_DIRTY_COUNT:
                self._save_unlocked()
            else:
                self._schedule_flush()
