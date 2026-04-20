#!/usr/bin/env python3
"""
价格获取模块 (带缓存和交易时间优化)
整合A股(腾讯)、港股(腾讯)、美股(yfinance)、基金(akshare)价格查询

优化特性:
1. 自动缓存价格，减少API调用
2. 根据交易时间智能调整缓存有效期
3. 非交易时间延长缓存，交易时间缩短缓存
4. 美股多数据源备选，防止限流
"""
import requests
import os
import time
import random
from datetime import datetime, timedelta
from typing import Dict, Optional, List, Tuple, Any

from .time_utils import bj_now_naive
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import threading
from pathlib import Path

from .market_time import MarketTimeUtil
from .asset_utils import detect_market_type as _detect_market_type_func
from . import config as _config
from .pricing import PriceRequest, PriceService
from .pricing.classifier import (
    get_exchange_prefix,
    get_type_hints_from_name,
    is_etf,
    is_otc_fund,
    normalize_code_with_name,
)
from .pricing.payload import (
    MONEY_QUANT,
    PCT_QUANT,
    RATE_QUANT,
    normalize_price_payload,
    quantize_money,
    quantize_pct,
    quantize_rate,
    to_decimal,
)


# 汇率缓存文件路径（使用项目相对路径）
RATE_CACHE_FILE = Path(__file__).parent.parent / '.data' / 'rate_cache.json'


def _market_type_from_asset_type(code: str, asset_type: Any, default_market_type: Optional[str]) -> Optional[str]:
    """Map persisted asset_type to pricing market_type, keeping legacy fund compatible."""
    from .models import AssetType

    if asset_type is None:
        return default_market_type

    atv = asset_type.value if hasattr(asset_type, 'value') else str(asset_type)
    if atv in (AssetType.A_STOCK.value, AssetType.CN_FUND.value):
        return 'cn'
    if atv == AssetType.EXCHANGE_FUND.value:
        return default_market_type if default_market_type in ('hk', 'us') else 'cn'
    if atv in (AssetType.HK_STOCK.value, AssetType.HK_FUND.value):
        return 'hk'
    if atv in (AssetType.US_STOCK.value, AssetType.US_FUND.value):
        return 'us'
    if atv in (AssetType.OTC_FUND.value,):
        return 'fund'
    if atv in (AssetType.FUND.value,):
        # Legacy/unknown fund rows: route exchange-traded fund codes as CN quotes.
        return 'cn' if is_etf(code) else 'fund'
    return default_market_type


class PriceFetcher:
    """统一价格获取器 (带缓存优化，支持飞书多维表)

    Harness-friendly conventions:
    - Prefer batch APIs when available (Tencent quotes) to reduce latency and failure surface.
    - Keep per-asset payloads self-describing (source/is_from_cache/is_stale/expires_at).
    - Provide scripts/diagnose_pricing.py for quick observability.
    """

    MONEY_QUANT = MONEY_QUANT
    RATE_QUANT = RATE_QUANT
    PCT_QUANT = PCT_QUANT

    # 关键词列表（用于名称辅助判断资产类型）
    STOCK_KEYWORDS = [
        '股票', '股份', '集团', '银行', '科技', '医药', '能源',
        '保险', '证券', '茅台', '格力', '美的', '五粮液', '平安',
        '招行', '兴业', '浦发', '华夏', '民生', '中信', '光大',
        '海油', '石油', '石化', '神华', '中免', '恒瑞', '药明',
        '宁德', '比亚迪', '隆基', '通威', '海康',
        '腾讯', '美团', '阿里', '京东', '百度', '小米',
    ]
    FUND_KEYWORDS = [
        'etf', '联接', '基金', '混合', '债券', '指数', 'qdii', 'fof',
        '货币', '理财', '分级', 'lof', '保本', '定增',
        '天弘', '易方达', '广发', '华夏', '汇添富', '南方', '嘉实',
        '博时', '工银', '华宝', '华安', '国泰', '招商', '鹏华',
    ]
    CASH_KEYWORDS = ['现金', '货币', 'mmf', 'cash', '余额宝']

    @staticmethod
    def _to_decimal(value):
        return to_decimal(value)

    @classmethod
    def _quantize_money(cls, value) -> float:
        return quantize_money(value)

    @classmethod
    def _quantize_rate(cls, value) -> float:
        return quantize_rate(value)

    @classmethod
    def _quantize_pct(cls, value) -> float:
        return quantize_pct(value)

    @classmethod
    def _normalize_price_payload(cls, payload: Dict) -> Dict:
        """统一价格输出口径。

        约定：
        - 所有金额字段按 MONEY_QUANT 量化
        - change_pct / exchange_rate 量化
        - 自动补充 fetched_at（北京时间 naive ISO 字符串），便于诊断“是否刷新/是否走缓存”
        """
        return normalize_price_payload(payload)

    def __init__(self, storage=None, use_cache: bool = True):
        """
        Args:
            storage: FeishuStorage 实例（可选，用于价格缓存）
            use_cache: 是否启用缓存
        """
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        })
        self.storage = storage
        self.use_cache = use_cache and storage is not None
        self._rate_cache = {}  # 汇率缓存
        self._rate_cache_time = None
        # last-batch meta for observability
        self._last_tencent_batch_meta = None
        self.price_service = PriceService.for_legacy_fetcher(self)
        self._last_price_service_diagnostics = []

    def fetch(
        self,
        code: str,
        asset_name: str = None,
        force_refresh: bool = False,
        *,
        asset_type_map: Dict[str, Any] = None,
        market_closed_ttl_multiplier: float = 1.0,
        accept_stale_when_closed: bool = False,
        max_stale_after_expiry_sec: int = 0,
        use_cache_only: bool = False,
    ) -> Optional[Dict]:
        """获取单个资产价格（带缓存）。

        约定：
        - 未过期缓存：直接返回（不触发 realtime 请求）
        - 过期缓存：默认不返回；若 accept_stale_when_closed=True，则在 max_stale_after_expiry_sec 窗口内可作为 fallback
        - use_cache_only=True：仅使用缓存（包括允许窗口内的过期缓存），不触发 realtime

        Args:
            code: 资产代码
            asset_name: 资产名称（用于辅助判断）
            force_refresh: True 时跳过缓存，强制 realtime
            asset_type_map: 可选 {code -> AssetType}，用于更准确的 market_type/TTL 计算
            market_closed_ttl_multiplier: TTL 乘数（如非交易时段延长）
            accept_stale_when_closed: 是否允许在缓存过期后仍读取（通常用于市场关闭时的“稳定优先”）
            max_stale_after_expiry_sec: 允许过期后最多多少秒仍可返回
            use_cache_only: 仅使用缓存，不请求实时价格（用于超时 fallback）
        """
        code = code.upper().strip()
        asset_name = (asset_name or '').strip()

        # 现金和货币基金直接返回，不缓存
        if code == 'CASH' or code.endswith('-CASH'):
            return self._get_cash_price(code)

        if code.endswith('-MMF'):
            return self._get_mmf_price(code)

        expired_cache_payload: Optional[Dict] = None

        # 1) 缓存命中（优先返回未过期缓存；过期缓存作为 fallback）
        if self.use_cache and not force_refresh:
            cached = self.storage.get_price(
                code,
                allow_expired=accept_stale_when_closed,
                max_stale_after_expiry_sec=max_stale_after_expiry_sec,
            )
            if cached:
                is_expired = False
                if cached.expires_at:
                    try:
                        expire_dt = (
                            datetime.fromisoformat(cached.expires_at.replace('Z', '+00:00'))
                            if isinstance(cached.expires_at, str)
                            else cached.expires_at
                        )
                        is_expired = expire_dt <= bj_now_naive()
                    except Exception:
                        # 保守：解析失败视为过期
                        is_expired = True

                payload = self._normalize_price_payload({
                    'code': cached.asset_id,
                    'name': cached.asset_name,
                    'price': cached.price,
                    'currency': cached.currency,
                    'cny_price': cached.cny_price,
                    'change': cached.change,
                    'change_pct': cached.change_pct,
                    'exchange_rate': cached.exchange_rate,
                    'source': cached.data_source or 'cache',
                    'expires_at': cached.expires_at,
                    'is_from_cache': True,
                    'is_stale': bool(is_expired),
                })

                if not is_expired:
                    return payload

                # expired but allowed: keep as fallback
                if accept_stale_when_closed:
                    payload['source'] = 'cache_fallback'
                    expired_cache_payload = payload

        # 仅用缓存：命中则返回（未命中则 None）
        if use_cache_only:
            return expired_cache_payload

        # 2) realtime 获取
        request_asset_type = asset_type_map.get(code) if asset_type_map is not None and code in asset_type_map else None
        if request_asset_type is not None:
            result = self._fetch_realtime(code, asset_name, request_asset_type)
        else:
            result = self._fetch_realtime(code, asset_name)
        if result:
            result = self._normalize_price_payload(result)

        # realtime 失败：允许使用过期缓存 fallback
        if not result and expired_cache_payload is not None:
            return expired_cache_payload

        # 3) 写入缓存
        if result and self.use_cache:
            from .models import PriceCache, AssetType

            market_type = _detect_market_type_func(code)
            # Prefer holdings-provided asset_type when available to avoid market mis-detection.
            if asset_type_map is not None and code in asset_type_map:
                market_type = _market_type_from_asset_type(code, asset_type_map.get(code), market_type)

            ttl = int(MarketTimeUtil.get_cache_ttl(market_type) * market_closed_ttl_multiplier)
            expires_at = bj_now_naive() + timedelta(seconds=ttl)

            # 检测资产类型（用于 price_cache 记录；不影响 holdings 的 asset_type）
            asset_type = AssetType.OTHER
            detected_asset_type = None
            try:
                from .asset_utils import detect_asset_type
                detected_asset_type = detect_asset_type(code)[0]
            except Exception:
                detected_asset_type = None

            if detected_asset_type in (AssetType.EXCHANGE_FUND, AssetType.OTC_FUND):
                asset_type = detected_asset_type
            elif market_type == 'cn':
                asset_type = AssetType.A_STOCK
            elif market_type == 'hk':
                asset_type = AssetType.HK_STOCK
            elif market_type == 'us':
                asset_type = AssetType.US_STOCK
            elif market_type == 'fund':
                asset_type = AssetType.OTC_FUND

            price_cache = PriceCache(
                asset_id=code,
                asset_name=result.get('name'),
                asset_type=asset_type,
                price=result.get('price', 0),
                currency=result.get('currency', 'CNY'),
                cny_price=result.get('cny_price', result.get('price', 0)),
                change=result.get('change'),
                change_pct=result.get('change_pct'),
                exchange_rate=result.get('exchange_rate'),
                data_source=result.get('source'),
                expires_at=expires_at,
            )
            self.storage.save_price(price_cache)
            result['market_type'] = market_type

        return result

    def fetch_batch(self, codes: List[str], name_map: Dict[str, str] = None,
                    asset_type_map: Dict[str, Any] = None,
                    market_closed_ttl_multiplier: float = 1.0,
                    accept_stale_when_closed: bool = False,
                    max_stale_after_expiry_sec: int = 0,
                    force_refresh: bool = False, use_concurrent: bool = True,
                    skip_us: bool = False, use_cache_only: bool = False) -> Dict[str, Dict]:
        """批量获取价格 (智能缓存 + 并发查询)

        Args:
            codes: 资产代码列表
            name_map: 代码到名称的映射
            force_refresh: 强制刷新缓存
            use_concurrent: 是否使用并发查询
            skip_us: 是否跳过美股查询（用于快速获取）
            use_cache_only: 仅使用缓存，不请求实时价格（超时时使用）

        Returns:
            代码到价格数据的映射
        """
        name_map = name_map or {}
        results: Dict[str, Dict] = {}

        # === 稳定去重（保序）===
        # 同一标的在多券商/多来源持仓中可能重复出现；重复 code 会导致冗余外部请求，
        # 且 tencent_batch meta 出现 returned < requested 的“假警告”。
        # 这里对 code 做 strip + upper 的稳定去重，保留首个出现的原始字符串作为 primary key。
        original_codes = list(codes or [])
        norm_to_codes: Dict[str, List[str]] = {}
        unique_codes: List[str] = []
        norm_to_primary: Dict[str, str] = {}
        for code in original_codes:
            norm = (code or '').strip().upper()
            if not norm:
                continue
            if norm not in norm_to_primary:
                norm_to_primary[norm] = code
                unique_codes.append(code)
            norm_to_codes.setdefault(norm, []).append(code)

        codes = unique_codes

        # 第一步：智能检查缓存，分离需要查询和已有缓存的
        to_fetch: List[str] = []
        expired_cache: Dict[str, Dict] = {}  # 记录过期缓存，用于 fallback

        # 批次级：提前获取一次汇率，供本批次所有资产复用（避免每个资产重复拉汇率）
        try:
            batch_rates = self._fetch_exchange_rates()
        except Exception:
            batch_rates = None

        for code in codes:
            normalized_code = (code or '').upper().strip()

            # 现金/货基优先直接生成价格，避免在缓存回退路径中漏掉外币现金汇率
            if normalized_code == 'CASH' or normalized_code.endswith('-CASH'):
                try:
                    if batch_rates:
                        results[code] = self._get_cash_price_with_rates(normalized_code, batch_rates)
                    else:
                        results[code] = self._get_cash_price(normalized_code)
                    continue
                except Exception:
                    # 如果实时汇率失败，再走后续缓存/回退逻辑
                    pass

            if normalized_code.endswith('-MMF'):
                results[code] = self._get_mmf_price(normalized_code)
                continue

            if self.use_cache:
                from .models import PriceCache
                cached = self.storage.get_price(code, allow_expired=accept_stale_when_closed, max_stale_after_expiry_sec=max_stale_after_expiry_sec)
                if cached:
                    cached_dict = self._price_cache_to_dict(cached)
                    # 检查是否过期
                    is_expired = True
                    if cached.expires_at:
                        try:
                            expire_dt = datetime.fromisoformat(cached.expires_at.replace('Z', '+00:00')) if isinstance(cached.expires_at, str) else cached.expires_at
                            is_expired = expire_dt <= bj_now_naive()
                        except (ValueError, TypeError, AttributeError):
                            pass

                    if not is_expired:
                        # 缓存有效，直接使用
                        results[code] = cached_dict
                        continue
                    else:
                        # 缓存过期但保留，用于失败时 fallback
                        expired_cache[code] = cached_dict

            # 需要获取新价格
            if not use_cache_only:
                to_fetch.append(code)
            elif code in expired_cache:
                # 仅使用缓存模式：即使过期也使用
                results[code] = expired_cache[code]

        if not to_fetch:
            # 把 primary 的结果复制回重复出现的 code（如果有重复）
            if norm_to_codes:
                out = dict(results)
                for norm, codes_list in norm_to_codes.items():
                    if len(codes_list) <= 1:
                        continue
                    primary = norm_to_primary.get(norm)
                    if primary and primary in results:
                        for dup in codes_list:
                            out.setdefault(dup, results[primary])
                return out
            return results

        # 第二步：区分美股和非美股
        us_codes = []
        other_codes = []
        for code in to_fetch:
            market_type = _detect_market_type_func(code)
            # Prefer holdings-provided asset_type when available to avoid market mis-detection.
            if asset_type_map is not None and code in asset_type_map:
                market_type = _market_type_from_asset_type(code, asset_type_map.get(code), market_type)
            if market_type == 'us' and not skip_us:
                us_codes.append(code)
            elif market_type != 'us':
                other_codes.append(code)
            elif skip_us and code in expired_cache:
                # 跳过美股但保留过期缓存
                results[code] = expired_cache[code]

        # 第三步：美股和非美股并行查询
        # 使用 ThreadPoolExecutor 同时启动两组查询，减少总等待时间
        if use_concurrent and (other_codes or us_codes):
            with ThreadPoolExecutor(max_workers=2) as executor:
                futures = []

                # 提交非美股查询（_nested=True 避免嵌套线程池）
                if other_codes:
                    futures.append(
                        executor.submit(self._fetch_concurrent, other_codes, name_map, 5, True, asset_type_map)
                    )

                # 提交美股查询（并行执行，_nested=True 避免嵌套线程池）
                if us_codes:
                    futures.append(
                        executor.submit(self._fetch_us_batch, us_codes, name_map, expired_cache, 3, True)
                    )

                # 等待所有结果，设置总超时
                for future in as_completed(futures, timeout=25):
                    try:
                        batch_results = future.result()
                        results.update(batch_results)
                    except Exception as e:
                        print(f"[警告] 批量查询失败: {e}")

            # 诊断：标记那些被写入 expired_cache 的为 stale fallback（便于上层统计/提示）
            for code, payload in list(results.items()):
                if isinstance(payload, dict) and payload.get('is_from_cache') and code in expired_cache:
                    payload.setdefault('is_stale', True)

            # 处理未获取到的代码（使用过期缓存）
            for code in other_codes + us_codes:
                if code not in results and code in expired_cache:
                    results[code] = expired_cache[code]
        else:
            # 非并发模式：串行处理
            for code in other_codes:
                asset_name = name_map.get(code)
                result = self.fetch(code, asset_name, force_refresh, asset_type_map=asset_type_map)
                if result and 'error' not in result:
                    results[code] = result
                elif code in expired_cache:
                    results[code] = expired_cache[code]

            if us_codes:
                us_results = self._fetch_us_batch(us_codes, name_map, expired_cache)
                results.update(us_results)

        # 把 primary 的结果复制回重复出现的 code（如果有重复）
        if norm_to_codes:
            out = dict(results)
            for norm, codes_list in norm_to_codes.items():
                if len(codes_list) <= 1:
                    continue
                primary = norm_to_primary.get(norm)
                if primary and primary in results:
                    for dup in codes_list:
                        out.setdefault(dup, results[primary])
            return out

        return results

    def _fetch_concurrent(self, codes: List[str], name_map: Dict[str, str],
                          max_workers: int = 5, _nested: bool = False,
                          asset_type_map: Dict[str, Any] = None) -> Dict[str, Dict]:
        """并发批量查询（用于非美股资产）

        优化：腾讯行情支持 batch 接口，本函数优先将 cn/hk/fund(jj) 走批量，
        只对剩余资产再走单个 fetch（并发）。

        Args:
            codes: 资产代码列表
            name_map: 代码到名称映射
            max_workers: 最大并发数
            _nested: 内部标志，True 表示已在线程池中，使用顺序执行避免嵌套

        Returns:
            代码到价格数据的映射
        """
        results: Dict[str, Dict] = {}
        errors: List[str] = []

        # 1) Tencent batch for cn/hk/fund
        try:
            batch_results, leftover = self._fetch_tencent_quotes_batch(codes, name_map=name_map, asset_type_map=asset_type_map)
            results.update(batch_results)
            codes = leftover
        except Exception as e:
            # batch 失败不应影响整体；回退到逐个 fetch
            errors.append(f"tencent_batch_failed: {e}")

        def fetch_single(code: str):
            try:
                asset_name = name_map.get(code)
                return code, self.fetch(code, asset_name, force_refresh=False, asset_type_map=asset_type_map)
            except Exception as e:
                return code, {'error': str(e)}

        if not codes:
            return results

        # 2) remaining assets
        if _nested:
            for code in codes:
                c, result = fetch_single(code)
                if result and 'error' not in result:
                    results[c] = result
                elif result and 'error' in result:
                    errors.append(f"{c}: {result['error']}")
        else:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_code = {executor.submit(fetch_single, code): code for code in codes}
                for future in as_completed(future_to_code):
                    code = future_to_code[future]
                    try:
                        _, result = future.result(timeout=15)
                        if result and 'error' not in result:
                            results[code] = result
                        elif result and 'error' in result:
                            errors.append(f"{code}: {result['error']}")
                    except Exception as e:
                        errors.append(f"{code}: 并发查询异常 {e}")

        if errors and len(errors) <= 3:
            print(f"部分资产查询失败: {'; '.join(errors[:3])}")

        return results

    def _fetch_tencent_quotes_batch(
        self,
        codes: List[str],
        name_map: Dict[str, str] = None,
        asset_type_map: Dict[str, Any] = None,
    ) -> Tuple[Dict[str, Dict], List[str]]:
        """Fetch Tencent quotes in batch for cn/hk/fund(jj).

        Returns:
            (results_map, leftover_codes)
        """
        name_map = name_map or {}
        results: Dict[str, Dict] = {}
        leftover: List[str] = []

        # Build query codes
        cn_query: List[Tuple[str, str]] = []  # (orig, query)
        hk_query: List[Tuple[str, str]] = []
        fund_query: List[Tuple[str, str]] = []

        for code in codes:
            mkt = _detect_market_type_func(code)
            if asset_type_map is not None and code in asset_type_map:
                mkt = _market_type_from_asset_type(code, asset_type_map.get(code), mkt)
            c = (code or '').upper().strip()
            if mkt == 'cn':
                if c.startswith(('SH', 'SZ')):
                    q = c.lower()
                elif c.isdigit() and len(c) == 6:
                    # A股/ETF 代码前缀规则（经验）：
                    # - 6xxxxx / 688xxx：上交所
                    # - 5xxxxx：上交所基金/ETF 常见前缀（如 510/512/513/515/516/588...）
                    # - 0/1/3/2 开头多为深交所（其中 159xxx 为深交所 ETF 常见）
                    # 不能再用“非6一律SZ”，否则 5xxxxx 的 ETF 会被错查成 sz563020=新疆2437 这种离谱结果。
                    q = ('sh' + c) if c.startswith(('6', '5')) else ('sz' + c)
                else:
                    leftover.append(code)
                    continue
                cn_query.append((code, q))
            elif mkt == 'hk':
                num = c[2:] if c.startswith('HK') else c
                if not num.isdigit():
                    leftover.append(code)
                    continue
                q = 'hk' + num.zfill(5)
                hk_query.append((code, q))
            elif mkt == 'fund':
                # Tencent jj NAV
                if c.startswith(('SH', 'SZ')):
                    c = c[2:]
                if not (c.isdigit() and len(c) == 6):
                    leftover.append(code)
                    continue
                q = 'jj' + c
                fund_query.append((code, q))
            else:
                leftover.append(code)

        query_codes = [q for _, q in (cn_query + hk_query + fund_query)]
        if not query_codes:
            return results, leftover

        from .tencent_batch import fetch_batch as _tencent_fetch_batch
        parts_map, meta = _tencent_fetch_batch(self.session, query_codes, timeout=8, chunk_size=50)

        # attach meta for harness/diagnostics (counted as a best-effort hint)
        self._last_tencent_batch_meta = meta

        # FX (only needed for HK)
        try:
            rates = self._fetch_exchange_rates()
            hkd_cny = rates['HKDCNY']
        except Exception:
            hkd_cny = None

        def build_by_orig(orig: str, q: str, kind: str) -> Optional[Dict]:
            data = parts_map.get(q)
            if not data:
                return None
            if kind in ('cn', 'hk'):
                if len(data) <= 45:
                    return None
                price = float(data[3])
                payload = {
                    'code': orig,
                    'name': data[1] or name_map.get(orig) or orig,
                    'price': price,
                    'prev_close': float(data[4]) if data[4] else None,
                    'open': float(data[5]) if data[5] else None,
                    'high': float(data[33]) if data[33] else None,
                    'low': float(data[34]) if data[34] else None,
                    'change': float(data[31]) if data[31] else None,
                    'change_pct': float(data[32]) if data[32] else None,
                    'volume': float(data[36]) * 100 if len(data) > 36 and data[36] else 0,
                    'time': data[30] if len(data) > 30 else None,
                    'source': 'tencent_batch',
                }
                if kind == 'cn':
                    payload.update({'currency': 'CNY', 'cny_price': price, 'market_type': 'cn'})
                else:
                    if hkd_cny:
                        payload.update({'currency': 'HKD', 'cny_price': price * hkd_cny, 'exchange_rate': hkd_cny, 'market_type': 'hk'})
                    else:
                        payload.update({'currency': 'HKD', 'market_type': 'hk'})
                return self._normalize_price_payload(payload)

            if kind == 'fund':
                # jj payload: NAV at index 5
                if len(data) < 6:
                    return None
                try:
                    nav = float(data[5])
                except Exception:
                    return None
                if not nav or nav <= 0:
                    return None
                return self._normalize_price_payload({
                    'code': orig,
                    'name': data[1] or name_map.get(orig) or orig,
                    'price': nav,
                    'currency': 'CNY',
                    'cny_price': nav,
                    'market_type': 'fund',
                    'source': 'tencent_jj_batch',
                })

            return None

        # build results
        for orig, q in cn_query:
            r = build_by_orig(orig, q, 'cn')
            if r:
                results[orig] = r
            else:
                leftover.append(orig)

        for orig, q in hk_query:
            r = build_by_orig(orig, q, 'hk')
            if r:
                results[orig] = r
            else:
                leftover.append(orig)

        for orig, q in fund_query:
            r = build_by_orig(orig, q, 'fund')
            if r:
                results[orig] = r
            else:
                leftover.append(orig)

        return results, leftover


    def _fetch_us_batch(self, codes: List[str], name_map: Dict[str, str],
                        expired_cache: Dict[str, Dict], max_workers: int = 3,
                        _nested: bool = False) -> Dict[str, Dict]:
        """批量获取美股价格（带快速失败机制）

        策略：
        1. 使用并发但限制并发数
        2. 缩短超时时间（5秒 vs 15秒）
        3. 不重试，失败后立即使用缓存
        4. 跟踪失败率，如果过高则跳过剩余

        Args:
            codes: 美股代码列表
            name_map: 代码到名称映射
            expired_cache: 过期缓存数据，用于失败时 fallback
            max_workers: 最大并发数
            _nested: 内部标志，True 表示已在线程池中，使用顺序执行避免嵌套

        Returns:
            代码到价格数据的映射
        """
        results = {}
        if not codes:
            return results

        # batch_rates not available in this scope; always fetch on demand
        batch_rates = None

        failure_lock = threading.Lock()
        consecutive_failures = [0]  # 使用列表以便在闭包中修改
        max_consecutive_failures = 3  # 连续失败3次则认为接口不可用

        # 检查是否有 Finnhub API key
        finnhub_key = _config.get('finnhub_api_key')

        def fetch_single_us(code):
            """获取单个美股价格（快速模式）"""
            try:
                # 使用缩短的超时和不重试
                yf_code = code.replace('.', '-')

                # 尝试1: Finnhub API（如果配置了 API key，3秒超时）
                if finnhub_key:
                    try:
                        result = self._fetch_us_stock_finnhub(yf_code, finnhub_key)
                        if result:
                            result['code'] = code  # 确保使用原始代码
                            return code, result
                    except Exception:
                        pass

                # 尝试2: Yahoo API（5秒超时，不重试）
                try:
                    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{yf_code}?interval=1d&range=2d"
                    headers = {
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                        'Accept': 'application/json',
                    }
                    response = self.session.get(url, headers=headers, timeout=5)

                    if response.status_code == 200:
                        data = response.json()
                        chart = data.get('chart', {})
                        if not chart.get('error'):
                            result = chart.get('result', [{}])[0]
                            meta = result.get('meta', {})
                            quotes = result.get('indicators', {}).get('quote', [{}])[0]
                            closes = [c for c in quotes.get('close', []) if c is not None]

                            if closes:
                                current = closes[-1]
                                prev_close = meta.get('previousClose') or meta.get('chartPreviousClose') or current
                                change = current - prev_close
                                change_pct = (change / prev_close * 100) if prev_close else 0

                                usd_cny = (batch_rates or self._fetch_exchange_rates())['USDCNY']

                                return code, self._normalize_price_payload({
                                    'code': code,
                                    'name': meta.get('shortName') or meta.get('longName') or code,
                                    'price': current,
                                    'prev_close': prev_close,
                                    'change': change,
                                    'change_pct': change_pct,
                                    'currency': meta.get('currency', 'USD'),
                                    'cny_price': current * usd_cny,
                                    'exchange_rate': usd_cny,
                                    'market_type': 'us',
                                    'source': 'yahoo_api'
                                })
                except Exception:
                    pass

                # 如果 Yahoo API 失败，尝试 yfinance（3秒超时）
                try:
                    import yfinance as yf
                    ticker = yf.Ticker(yf_code)
                    info = ticker.info
                    hist = ticker.history(period="1d", timeout=3)

                    if not hist.empty:
                        latest = hist.iloc[-1]
                        prev_close = info.get('previousClose', latest['Open'])
                        current = latest['Close']
                        change = current - prev_close
                        change_pct = (change / prev_close * 100) if prev_close else 0

                        usd_cny = (batch_rates or self._fetch_exchange_rates())['USDCNY']

                        return code, self._normalize_price_payload({
                            'code': code,
                            'name': info.get('shortName', yf_code),
                            'price': current,
                            'prev_close': prev_close,
                            'change': change,
                            'change_pct': change_pct,
                            'currency': info.get('currency', 'USD'),
                            'cny_price': current * usd_cny,
                            'exchange_rate': usd_cny,
                            'market_type': 'us',
                            'source': 'yfinance'
                        })
                except Exception:
                    pass

                # 所有数据源失败
                return code, None

            except Exception:
                return code, None

        # 如果已在线程池中（嵌套调用），使用顺序执行避免死锁
        if _nested:
            for code in codes:
                _, result = fetch_single_us(code)
                if result:
                    results[code] = result
                    consecutive_failures[0] = 0
                else:
                    consecutive_failures[0] += 1
                    if code in expired_cache:
                        results[code] = expired_cache[code]
                        results[code]['source'] = 'cache_fallback'

                # 如果连续失败过多，跳过剩余查询
                if consecutive_failures[0] >= max_consecutive_failures:
                    print(f"[美股价格] 连续 {consecutive_failures[0]} 次获取失败，跳过剩余美股查询")
                    for c in codes:
                        if c not in results and c in expired_cache:
                            results[c] = expired_cache[c]
                            results[c]['source'] = 'cache_fallback'
                    break
        else:
            # 使用 ThreadPoolExecutor 并发查询，但限制并发数
            from concurrent.futures import ThreadPoolExecutor, as_completed

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # 提交所有任务
                future_to_code = {
                    executor.submit(fetch_single_us, code): code for code in codes
                }

                # 收集结果
                for future in as_completed(future_to_code):
                    code = future_to_code[future]
                    try:
                        _, result = future.result(timeout=10)  # 整体等待10秒
                        if result:
                            results[code] = result
                            with failure_lock:
                                consecutive_failures[0] = 0  # 重置失败计数
                        else:
                            with failure_lock:
                                consecutive_failures[0] += 1
                            # 使用过期缓存作为 fallback
                            if code in expired_cache:
                                results[code] = expired_cache[code]
                                results[code]['source'] = 'cache_fallback'
                    except Exception:
                        with failure_lock:
                            consecutive_failures[0] += 1
                        if code in expired_cache:
                            results[code] = expired_cache[code]
                            results[code]['source'] = 'cache_fallback'

                    # 如果连续失败过多，取消剩余任务
                    with failure_lock:
                        should_break = consecutive_failures[0] >= max_consecutive_failures
                    if should_break:
                        print(f"[美股价格] 连续 {consecutive_failures[0]} 次获取失败，跳过剩余美股查询")
                        # 剩余未完成的都使用缓存
                        for f, c in future_to_code.items():
                            if c not in results and c in expired_cache:
                                results[c] = expired_cache[c]
                                results[c]['source'] = 'cache_fallback'
                        break

        return results

    def _price_cache_to_dict(self, cached) -> Dict:
        """将PriceCache对象转为字典"""
        return self._normalize_price_payload({
            'code': cached.asset_id,
            'name': cached.asset_name,
            'price': cached.price,
            'currency': cached.currency,
            'cny_price': cached.cny_price,
            'change': cached.change,
            'change_pct': cached.change_pct,
            'exchange_rate': cached.exchange_rate,
            'source': cached.data_source or 'cache',
            'expires_at': cached.expires_at,
            'is_from_cache': True,
        })

    def _retry_with_backoff(self, func, max_retries: int = 3, base_delay: float = 1.0):
        """带指数退避的重试机制

        Args:
            func: 要执行的函数
            max_retries: 最大重试次数
            base_delay: 基础延迟秒数

        Returns:
            func的返回值

        Raises:
            最后一次重试的异常
        """
        last_exception = None
        for attempt in range(max_retries):
            try:
                return func()
            except Exception as e:
                last_exception = e
                error_msg = str(e).lower()

                # 判断是否可重试的错误
                is_retryable = (
                    'rate' in error_msg or
                    'limit' in error_msg or
                    '429' in error_msg or
                    'timeout' in error_msg or
                    'connection' in error_msg or
                    '503' in error_msg or
                    '502' in error_msg or
                    'too many requests' in error_msg
                )

                if not is_retryable:
                    raise

                if attempt < max_retries - 1:
                    # 指数退避 + 随机抖动
                    delay = base_delay * (2 ** attempt) + random.uniform(0, 0.5)
                    print(f"  请求限流，{delay:.1f}秒后重试 ({attempt + 1}/{max_retries - 1})...")
                    time.sleep(delay)

        raise last_exception

    def _get_cash_price_with_rates(self, code: str, rates: Dict[str, float]) -> Dict:
        """获取现金价格（使用外部传入的汇率，避免重复请求）"""
        currency = code.split('-')[0] if '-' in code else 'CNY'

        if currency == 'CNY':
            exchange_rate = 1.0
        else:
            rate_key = f'{currency}CNY'
            exchange_rate = rates.get(rate_key)
            if exchange_rate is None:
                # 兼容旧键（USDCNY/HKDCNY）
                exchange_rate = rates.get('USDCNY') if currency == 'USD' else rates.get('HKDCNY')
            if exchange_rate is None:
                raise KeyError(f"rates missing {rate_key}")

        return self._normalize_price_payload({
            'code': code,
            'name': f'{currency}现金',
            'price': 1.0,
            'currency': currency,
            'cny_price': exchange_rate,
            'exchange_rate': exchange_rate,
            'market_type': 'cash',
            'source': 'fixed'
        })

    def _get_cash_price(self, code: str) -> Dict:
        """获取现金价格"""
        currency = code.split('-')[0] if '-' in code else 'CNY'

        # 本币现金，cny_price = 1.0
        if currency == 'CNY':
            cny_price = 1.0
            exchange_rate = 1.0
        else:
            # 外币现金，获取实时汇率
            rates = self._fetch_exchange_rates()
            rate_key = f'{currency}CNY'
            exchange_rate = rates[rate_key]
            cny_price = exchange_rate

        return self._normalize_price_payload({
            'code': code,
            'name': f'{currency}现金',
            'price': 1.0,
            'currency': currency,
            'cny_price': cny_price,
            'exchange_rate': exchange_rate,
            'market_type': 'cash',
            'source': 'fixed'
        })

    def _get_mmf_price(self, code: str) -> Dict:
        """获取货币基金价格"""
        currency = code.split('-')[0]
        return self._normalize_price_payload({
            'code': code,
            'name': f'{currency}货币基金',
            'price': 1.0,
            'currency': currency,
            'cny_price': 1.0,
            'market_type': 'mmf',
            'source': 'fixed'
        })

    def _fetch_realtime(self, code: str, asset_name: str, asset_type: Any = None) -> Optional[Dict]:
        """获取实时价格 (内部方法)"""
        # 根据名称辅助判断类型
        name_hints = self._get_type_hints_from_name(asset_name)

        # 根据名称辅助判断并补全代码前缀
        normalized_code = self._normalize_code_with_name(code, asset_name)
        request = PriceRequest(
            code=code,
            asset_name=asset_name or "",
            asset_type=asset_type,
            normalized_code=normalized_code,
            hints=name_hints,
        )
        result = self.price_service.fetch_realtime(request)
        self._last_price_service_diagnostics = list(self.price_service.last_diagnostics)
        return result

    def _normalize_code_with_name(self, code: str, name: str) -> str:
        """根据资产名称给代码添加交易所前缀"""
        return normalize_code_with_name(code, name)

    def _get_type_hints_from_name(self, name: str) -> Dict:
        """从资产名称中提取类型提示"""
        return get_type_hints_from_name(name)

    def _is_etf(self, code: str) -> bool:
        """检测是否为ETF/场内基金"""
        return is_etf(code)

    def _is_otc_fund(self, code: str) -> bool:
        """检测是否为场外基金代码

        注意: 000/002/003 开头的代码与A股重叠（如 000001 既是平安银行也是华夏成长），
        无法仅凭代码区分。此方法仅识别不含歧义的场外基金代码。
        歧义代码需依赖 name_hints 在 _fetch_realtime 中判断。
        """
        return is_otc_fund(code)

    def _get_exchange_prefix(self, code: str) -> str:
        """获取交易所前缀"""
        return get_exchange_prefix(code)

    def _load_rate_cache_from_file(self) -> Optional[Dict]:
        """从 JSON 文件加载汇率缓存"""
        try:
            if RATE_CACHE_FILE.exists():
                with open(RATE_CACHE_FILE, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    return {
                        'rates': data.get('rates', {}),
                        'timestamp': data.get('timestamp')
                    }
        except (json.JSONDecodeError, IOError) as e:
            print(f"[警告] 加载汇率缓存文件失败: {e}")
        return None

    def _save_rate_cache_to_file(self, rates: Dict[str, float]):
        """保存汇率缓存到 JSON 文件"""
        try:
            # 确保目录存在
            RATE_CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)

            data = {
                'rates': rates,
                'timestamp': bj_now_naive().isoformat(),
                'cached_at': bj_now_naive().strftime('%Y-%m-%d %H:%M:%S')
            }
            with open(RATE_CACHE_FILE, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except IOError as e:
            print(f"[警告] 保存汇率缓存文件失败: {e}")

    def _fetch_exchange_rates(self, max_retries: int = 3) -> Dict[str, float]:
        """获取汇率 (带24小时缓存，并发请求+重试机制)

        Args:
            max_retries: 最大重试次数

        Returns:
            汇率字典。获取失败时，如果有过期缓存则使用缓存并打印警告；
            完全没有缓存时抛出 RuntimeError。
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed
        import time

        now = bj_now_naive()

        # 1. 检查内存缓存 (24小时)
        if self._rate_cache_time and (now - self._rate_cache_time).total_seconds() < 86400:
            return self._rate_cache

        # 2. 内存缓存过期，尝试从文件加载
        if not self._rate_cache_time:
            file_cache = self._load_rate_cache_from_file()
            if file_cache and file_cache['timestamp']:
                try:
                    cache_time = datetime.fromisoformat(file_cache['timestamp'])
                    # 检查文件缓存是否在24小时内
                    if (now - cache_time).total_seconds() < 86400:
                        self._rate_cache = file_cache['rates']
                        self._rate_cache_time = cache_time
                        print(f"[汇率] 从文件加载缓存: USD/CNY={self._rate_cache.get('USDCNY')}, HKD/CNY={self._rate_cache.get('HKDCNY')}")
                        return self._rate_cache
                except (ValueError, TypeError):
                    pass

        # 3. 实时获取汇率（多源备选）
        def fetch_single_rate_with_fallback(currency: str) -> tuple:
            """获取单个货币汇率，支持多源备选"""

            # 定义多个汇率 API 源
            api_sources = [
                # 源1: open.er-api.com（免 key，稳定）
                lambda: _fetch_from_open_er_api(currency),
                # 源2: exchangerate-api.com（老接口，部分地区可用）
                lambda: _fetch_from_exchangerate_api(currency),
                # 源3: 中国外汇交易中心（官方参考）
                lambda: _fetch_from_chinamoney(currency),
                # 源4: exchangerate.host（可能需要 key，作为最后兜底）
                lambda: _fetch_from_exchangerate_host(currency),
            ]

            last_error = None
            for source_idx, source_func in enumerate(api_sources):
                for attempt in range(max_retries):
                    try:
                        rate = source_func()
                        if rate:
                            return currency, round(rate, 4), None
                    except Exception as e:
                        last_error = str(e)
                        if attempt < max_retries - 1:
                            wait_time = 2 ** attempt
                            time.sleep(wait_time)
                            continue
                        # 切换到下一个源
                        break

            return currency, None, f"所有API源失败: {last_error}"

        def _fetch_from_open_er_api(currency: str) -> float:
            """从 open.er-api.com 获取汇率（免 key）"""
            url = f"https://open.er-api.com/v6/latest/{currency}"
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()
            if data.get('result') != 'success':
                raise ValueError(f"open.er-api 返回异常: {data}")
            return data['rates']['CNY']

        def _fetch_from_exchangerate_api(currency: str) -> float:
            """从 exchangerate-api.com 获取汇率"""
            url = f"https://api.exchangerate-api.com/v4/latest/{currency}"
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()
            return data['rates']['CNY']

        def _fetch_from_chinamoney(currency: str) -> float:
            """从中国外汇交易中心获取汇率（官方数据源）"""
            # 使用新浪财经接口作为官方参考
            currency_pair = f"{currency}CNY"
            url = f"https://hq.sinajs.cn/list=fx_s{currency_pair.lower()}"
            response = self.session.get(url, timeout=10, headers={'Referer': 'https://finance.sina.com.cn'})
            response.raise_for_status()
            # 解析新浪返回格式: var hq_str_fx_susdcny="..."
            content = response.text
            if 'var hq_str_' in content:
                # 提取买入价和卖出价的平均值
                parts = content.split('"')[1].split(',')
                if len(parts) >= 8:
                    buy = float(parts[0])  # 买入价
                    sell = float(parts[2])  # 卖出价
                    return (buy + sell) / 2
            raise ValueError(f"无法解析新浪汇率数据: {content[:100]}")

        def _fetch_from_exchangerate_host(currency: str) -> float:
            """从 exchangerate.host 获取汇率（备用源）"""
            url = f"https://api.exchangerate.host/convert?from={currency}&to=CNY&amount=1"
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()
            return data['result']

        # 并发获取 USD 和 HKD 汇率
        currencies = ['USD', 'HKD']
        results = {}
        errors = []

        try:
            with ThreadPoolExecutor(max_workers=2) as executor:
                futures = {executor.submit(fetch_single_rate_with_fallback, c): c for c in currencies}
                for future in as_completed(futures):
                    currency, rate, error = future.result()
                    if error:
                        errors.append(f"{currency}: {error}")
                    else:
                        results[currency] = rate

            # 检查是否所有汇率都获取成功
            if len(results) != len(currencies):
                raise RuntimeError(f"获取汇率失败: {'; '.join(errors)}")

            # 更新内存缓存
            self._rate_cache = {
                'USDCNY': results['USD'],
                'HKDCNY': results['HKD']
            }
            self._rate_cache_time = now

            # 保存到文件
            self._save_rate_cache_to_file(self._rate_cache)
            print(f"[汇率] 已更新缓存: USD/CNY={self._rate_cache['USDCNY']}, HKD/CNY={self._rate_cache['HKDCNY']}")

            return self._rate_cache

        except Exception as e:
            # 获取失败，检查是否有任何缓存可用（内存或文件）
            fallback_cache = self._rate_cache or self._load_rate_cache_from_file()
            if fallback_cache:
                rates = fallback_cache.get('rates', fallback_cache) if isinstance(fallback_cache, dict) else fallback_cache
                cache_time = self._rate_cache_time
                if not cache_time and isinstance(fallback_cache, dict):
                    try:
                        cache_time = datetime.fromisoformat(fallback_cache.get('timestamp', ''))
                    except (ValueError, TypeError):
                        cache_time = None

                if cache_time:
                    cache_age_hours = (now - cache_time).total_seconds() / 3600
                    age_str = f"{cache_age_hours:.1f}"
                else:
                    age_str = "未知"
                print(f"[⚠️ 警告] 获取实时汇率失败: {e}")
                print(f"[⚠️ 警告] 使用 {age_str} 小时前的过期汇率: USD/CNY={rates.get('USDCNY')}, HKD/CNY={rates.get('HKDCNY')}")

                # 更新内存缓存
                self._rate_cache = rates
                self._rate_cache_time = cache_time or now
                return rates

            # 完全没有缓存，抛出异常
            raise RuntimeError(f"获取汇率失败且没有可用缓存: {e}")

    # ========== Provider compatibility adapters ==========

    def _fetch_a_stock(self, code: str) -> Optional[Dict]:
        """兼容旧调用：A 股实时价格实现已迁移到 CNStockProvider。"""
        from .pricing.providers.cn import CNStockProvider

        return CNStockProvider(self).fetch_a_stock(code)

    def _fetch_a_stock_from_tencent(self, code: str) -> Optional[Dict]:
        """兼容旧调用：腾讯 A 股源已迁移到 CNStockProvider。"""
        from .pricing.providers.cn import CNStockProvider

        return CNStockProvider(self).fetch_from_tencent(code)

    def _fetch_a_stock_from_akshare(self, code: str) -> Optional[Dict]:
        """兼容旧调用：AKShare A 股源已迁移到 CNStockProvider。"""
        from .pricing.providers.cn import CNStockProvider

        return CNStockProvider(self).fetch_from_akshare(code)

    def _fetch_hk_stock(self, code: str) -> Optional[Dict]:
        """兼容旧调用：港股实时价格实现已迁移到 HKStockProvider。"""
        from .pricing.providers.hk import HKStockProvider

        return HKStockProvider(self).fetch_hk_stock(code)

    def _fetch_hk_stock_from_tencent(self, code: str) -> Optional[Dict]:
        """兼容旧调用：腾讯港股源已迁移到 HKStockProvider。"""
        from .pricing.providers.hk import HKStockProvider

        return HKStockProvider(self).fetch_from_tencent(code)

    def _fetch_hk_stock_from_akshare(self, code: str) -> Optional[Dict]:
        """兼容旧调用：AKShare 港股源已迁移到 HKStockProvider。"""
        from .pricing.providers.hk import HKStockProvider

        return HKStockProvider(self).fetch_from_akshare(code)

    def _fetch_us_stock(self, code: str) -> Optional[Dict]:
        """兼容旧调用：美股实时价格实现已迁移到 USStockProvider。"""
        from .pricing.providers.us import USStockProvider

        return USStockProvider(self).fetch_us_stock(code)

    def _fetch_us_stock_finnhub(self, code: str, api_key: str) -> Optional[Dict]:
        """兼容旧调用：Finnhub 源已迁移到 USStockProvider。"""
        from .pricing.providers.us import USStockProvider

        return USStockProvider(self).fetch_finnhub(code, api_key)

    def _fetch_us_stock_yahoo_api(self, code: str) -> Optional[Dict]:
        """兼容旧调用：Yahoo 源已迁移到 USStockProvider。"""
        from .pricing.providers.us import USStockProvider

        return USStockProvider(self).fetch_yahoo_api(code)

    def _fetch_etf(self, code: str) -> Optional[Dict]:
        """兼容旧调用：ETF 实时价格实现已迁移到 ETFProvider。"""
        from .pricing.providers.etf import ETFProvider

        return ETFProvider(self).fetch_etf(code)

    def _fetch_fund(self, code: str) -> Optional[Dict]:
        """兼容旧调用：场外基金净值实现已迁移到 FundProvider。"""
        from .pricing.providers.fund import FundProvider

        return FundProvider(self).fetch_fund(code)

    def _fetch_fund_from_tencent(self, code: str) -> Optional[Dict]:
        """兼容旧调用：腾讯基金源已迁移到 FundProvider。"""
        from .pricing.providers.fund import FundProvider

        return FundProvider(self).fetch_from_tencent(code)

    def _fetch_fund_from_eastmoney(self, code: str) -> Optional[Dict]:
        """兼容旧调用：东方财富基金源已迁移到 FundProvider。"""
        from .pricing.providers.fund import FundProvider

        return FundProvider(self).fetch_from_eastmoney(code)
