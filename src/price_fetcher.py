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
from decimal import Decimal, ROUND_HALF_UP
import re
import os
import time
import random
from datetime import datetime, timedelta
from typing import Dict, Optional, List
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import threading
from pathlib import Path

from .market_time import MarketTimeUtil
from .asset_utils import detect_market_type as _detect_market_type_func
from . import config as _config


# 汇率缓存文件路径（使用项目相对路径）
RATE_CACHE_FILE = Path(__file__).parent.parent / '.data' / 'rate_cache.json'

class PriceFetcher:
    """统一价格获取器 (带缓存优化，支持飞书多维表)"""

    MONEY_QUANT = Decimal('0.01')
    RATE_QUANT = Decimal('0.000001')
    PCT_QUANT = Decimal('0.01')

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
    def _to_decimal(value) -> Decimal:
        if value is None:
            return Decimal('0')
        if isinstance(value, Decimal):
            return value
        return Decimal(str(value))

    @classmethod
    def _quantize_money(cls, value) -> float:
        return float(cls._to_decimal(value).quantize(cls.MONEY_QUANT, rounding=ROUND_HALF_UP))

    @classmethod
    def _quantize_rate(cls, value) -> float:
        return float(cls._to_decimal(value).quantize(cls.RATE_QUANT, rounding=ROUND_HALF_UP))

    @classmethod
    def _quantize_pct(cls, value) -> float:
        return float(cls._to_decimal(value).quantize(cls.PCT_QUANT, rounding=ROUND_HALF_UP))

    @classmethod
    def _normalize_price_payload(cls, payload: Dict) -> Dict:
        result = dict(payload)
        for key in ('price', 'prev_close', 'open', 'high', 'low', 'change', 'cny_price'):
            if key in result and result[key] is not None:
                result[key] = cls._quantize_money(result[key])
        if 'change_pct' in result and result['change_pct'] is not None:
            result['change_pct'] = cls._quantize_pct(result['change_pct'])
        if 'exchange_rate' in result and result['exchange_rate'] is not None:
            result['exchange_rate'] = cls._quantize_rate(result['exchange_rate'])
        return result

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

    def fetch(self, code: str, asset_name: str = None, force_refresh: bool = False) -> Optional[Dict]:
        """获取资产价格 (带缓存)

        Args:
            code: 资产代码
            asset_name: 资产名称（用于辅助判断）
            force_refresh: 强制刷新缓存
        """
        code = code.upper().strip()
        asset_name = (asset_name or '').strip()

        # 现金和货币基金直接返回，不缓存
        if code == 'CASH' or code.endswith('-CASH'):
            return self._get_cash_price(code)

        if code.endswith('-MMF'):
            return self._get_mmf_price(code)

        # 检查缓存
        if self.use_cache and not force_refresh:
            from .models import PriceCache
            cached = self.storage.get_price(code)
            if cached:
                return self._normalize_price_payload({
                    'code': cached.asset_id,
                    'name': cached.asset_name,
                    'price': cached.price,
                    'currency': cached.currency,
                    'cny_price': cached.cny_price,
                    'change': cached.change,
                    'change_pct': cached.change_pct,
                    'exchange_rate': cached.exchange_rate,
                    'source': cached.data_source,
                    'expires_at': cached.expires_at
                })

        # 获取实时价格
        result = self._fetch_realtime(code, asset_name)
        if result:
            result = self._normalize_price_payload(result)

        # 写入缓存
        if result and self.use_cache:
            from .models import PriceCache, AssetType
            from datetime import datetime, timedelta

            market_type = _detect_market_type_func(code)
            ttl = MarketTimeUtil.get_cache_ttl(market_type)

            # 计算过期时间
            expires_at = datetime.now() + timedelta(seconds=ttl)

            # 检测资产类型
            asset_type = AssetType.OTHER
            if market_type == 'cn':
                asset_type = AssetType.A_STOCK
            elif market_type == 'hk':
                asset_type = AssetType.HK_STOCK
            elif market_type == 'us':
                asset_type = AssetType.US_STOCK
            elif market_type == 'fund':
                asset_type = AssetType.FUND

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
                expires_at=expires_at
            )
            self.storage.save_price(price_cache)
            result['market_type'] = market_type

        return result

    def fetch_batch(self, codes: List[str], name_map: Dict[str, str] = None,
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
        results = {}

        # 第一步：智能检查缓存，分离需要查询和已有缓存的
        to_fetch = []
        expired_cache = {}  # 记录过期缓存，用于 fallback

        for code in codes:
            normalized_code = (code or '').upper().strip()

            # 现金/货基优先直接生成价格，避免在缓存回退路径中漏掉外币现金汇率
            if normalized_code == 'CASH' or normalized_code.endswith('-CASH'):
                try:
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
                cached = self.storage.get_price(code)
                if cached:
                    cached_dict = self._price_cache_to_dict(cached)
                    # 检查是否过期
                    is_expired = True
                    if cached.expires_at:
                        try:
                            expire_dt = datetime.fromisoformat(cached.expires_at.replace('Z', '+00:00')) if isinstance(cached.expires_at, str) else cached.expires_at
                            is_expired = expire_dt <= datetime.now()
                        except:
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
            return results

        # 第二步：区分美股和非美股
        us_codes = []
        other_codes = []
        for code in to_fetch:
            market_type = _detect_market_type_func(code)
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
                        executor.submit(self._fetch_concurrent, other_codes, name_map, 5, True)
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

            # 处理未获取到的代码（使用过期缓存）
            for code in other_codes + us_codes:
                if code not in results and code in expired_cache:
                    results[code] = expired_cache[code]
        else:
            # 非并发模式：串行处理
            for code in other_codes:
                asset_name = name_map.get(code)
                result = self.fetch(code, asset_name, force_refresh)
                if result and 'error' not in result:
                    results[code] = result
                elif code in expired_cache:
                    results[code] = expired_cache[code]

            if us_codes:
                us_results = self._fetch_us_batch(us_codes, name_map, expired_cache)
                results.update(us_results)

        return results

    def _fetch_concurrent(self, codes: List[str], name_map: Dict[str, str],
                          max_workers: int = 5, _nested: bool = False) -> Dict[str, Dict]:
        """并发批量查询（用于非美股资产）

        Args:
            codes: 资产代码列表
            name_map: 代码到名称映射
            max_workers: 最大并发数
            _nested: 内部标志，True 表示已在线程池中，使用顺序执行避免嵌套

        Returns:
            代码到价格数据的映射
        """
        results = {}
        errors = []

        def fetch_single(code):
            try:
                asset_name = name_map.get(code)
                return code, self.fetch(code, asset_name, force_refresh=False)
            except Exception as e:
                return code, {'error': str(e)}

        # 如果已在线程池中（嵌套调用），使用顺序执行避免死锁
        if _nested:
            for code in codes:
                code, result = fetch_single(code)
                if result and 'error' not in result:
                    results[code] = result
                elif result and 'error' in result:
                    errors.append(f"{code}: {result['error']}")
        else:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_code = {
                    executor.submit(fetch_single, code): code for code in codes
                }

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

                                rates = self._fetch_exchange_rates()
                                usd_cny = rates['USDCNY']

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

                        rates = self._fetch_exchange_rates()
                        usd_cny = rates['USDCNY']

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
        return {
            'code': cached.asset_id,
            'name': cached.asset_name,
            'price': cached.price,
            'currency': cached.currency,
            'cny_price': cached.cny_price,
            'change': cached.change,
            'change_pct': cached.change_pct,
            'exchange_rate': cached.exchange_rate,
            'source': cached.data_source or 'cache',
            'expires_at': cached.expires_at
        }

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

    def _fetch_realtime(self, code: str, asset_name: str) -> Optional[Dict]:
        """获取实时价格 (内部方法)"""
        # 根据名称辅助判断类型
        name_hints = self._get_type_hints_from_name(asset_name)

        # 根据名称辅助判断并补全代码前缀
        code = self._normalize_code_with_name(code, asset_name)

        # 1. ETF/场内基金
        if self._is_etf(code):
            return self._fetch_etf(code)

        # 2. A股和场外基金
        elif code.startswith(('SH', 'SZ')) or (code.isdigit() and len(code) == 6 and
                                              (code.startswith('6') or code.startswith('0') or
                                               code.startswith('3') or code.startswith('1') or
                                               code.startswith('2'))):
            is_likely_fund = name_hints.get('is_fund', False) or self._is_otc_fund(code)
            if is_likely_fund and not name_hints.get('is_stock', False):
                return self._fetch_fund(code)
            return self._fetch_a_stock(code)

        # 3. 港股
        elif code.startswith('HK') or (code.isdigit() and 4 <= len(code) <= 5):
            return self._fetch_hk_stock(code)

        # 4. 美股
        else:
            return self._fetch_us_stock(code)

    def _normalize_code_with_name(self, code: str, name: str) -> str:
        """根据资产名称给代码添加交易所前缀"""
        if code.startswith(('SH', 'SZ')):
            return code

        if not (code.isdigit() and len(code) == 6):
            return code

        name_lower = (name or '').lower()

        is_stock = any(kw in name_lower for kw in self.STOCK_KEYWORDS)
        is_fund = any(kw in name_lower for kw in self.FUND_KEYWORDS)

        if is_stock and not is_fund:
            if code.startswith('6'):
                return f'SH{code}'
            elif code.startswith(('0', '3')):
                return f'SZ{code}'

        return code

    def _get_type_hints_from_name(self, name: str) -> Dict:
        """从资产名称中提取类型提示"""
        if not name:
            return {}

        name_lower = name.lower()
        hints = {}

        hints['is_fund'] = any(kw in name_lower for kw in self.FUND_KEYWORDS)
        hints['is_etf'] = 'etf' in name_lower
        hints['is_stock'] = any(kw in name_lower for kw in self.STOCK_KEYWORDS) and not hints['is_fund']
        hints['is_cash'] = any(kw in name_lower for kw in self.CASH_KEYWORDS)

        return hints

    def _is_etf(self, code: str) -> bool:
        """检测是否为ETF/场内基金"""
        if not code.isdigit() or len(code) != 6:
            return False
        if code.startswith('5'):
            return True
        if code.startswith('15') and not code.startswith('16'):
            return True
        return False

    def _is_otc_fund(self, code: str) -> bool:
        """检测是否为场外基金代码

        注意: 000/002/003 开头的代码与A股重叠（如 000001 既是平安银行也是华夏成长），
        无法仅凭代码区分。此方法仅识别不含歧义的场外基金代码。
        歧义代码需依赖 name_hints 在 _fetch_realtime 中判断。
        """
        if not code.isdigit() or len(code) != 6:
            return False
        # 明确是A股的前缀: 600/601/603/605/688/689(沪市), 300/301(创业板)
        if code.startswith(('600', '601', '603', '605', '688', '689', '300', '301')):
            return False
        # 明确是场内ETF的前缀: 5xx(沪市ETF), 15x(深市ETF)
        if code.startswith('5') or code.startswith('15'):
            return False
        # 明确是场外基金的前缀 (不与A股重叠)
        if code.startswith(('004', '005', '006', '007', '008', '009')):
            return True
        if code.startswith(('01', '27', '16')):
            return True
        # 000/001/002/003 开头: 与A股重叠，返回 False 交给 name_hints 判断
        return False

    def _get_exchange_prefix(self, code: str) -> str:
        """获取交易所前缀"""
        if code.startswith(('6', '5')):
            return 'sh'
        return 'sz'

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
                'timestamp': datetime.now().isoformat(),
                'cached_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
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

        now = datetime.now()

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
                # 源1: exchangerate-api.com
                lambda: _fetch_from_exchangerate_api(currency),
                # 源2: 中国外汇交易中心（官方）
                lambda: _fetch_from_chinamoney(currency),
                # 源3: 汇率转换备用接口
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

    # ========== 具体数据源获取方法 ==========

    def _fetch_a_stock(self, code: str) -> Optional[Dict]:
        """获取A股价格 (腾讯主源 + AKShare备用)"""
        # 1. 先尝试腾讯API
        try:
            result = self._fetch_a_stock_from_tencent(code)
            if result:
                return result
        except requests.Timeout:
            print(f"[超时] 腾讯API获取A股价格 {code}")
        except Exception as e:
            print(f"[腾讯API失败] 获取A股价格 {code}: {e}")

        # 2. 腾讯失败，尝试AKShare
        print(f"[备用源] 尝试AKShare获取A股 {code}...")
        try:
            result = self._fetch_a_stock_from_akshare(code)
            if result:
                return result
        except Exception as e:
            print(f"[AKShare失败] 获取A股价格 {code}: {e}")

        return None

    def _fetch_a_stock_from_tencent(self, code: str) -> Optional[Dict]:
        """从腾讯获取A股价格"""
        if code.startswith('SH'):
            query_code = code.lower()
        elif code.startswith('SZ'):
            query_code = code.lower()
        elif code.isdigit():
            query_code = f'sh{code}' if code.startswith('6') else f'sz{code}'
        else:
            query_code = code

        url = f"http://qt.gtimg.cn/q={query_code}"
        response = self.session.get(url, timeout=5)
        response.encoding = 'gb2312'
        text = response.text

        pattern = rf'v_{query_code}="([^"]+)"'
        match = re.search(pattern, text)

        if match:
            data = match.group(1).split('~')
            if len(data) > 45:
                return self._normalize_price_payload({
                    'code': code,
                    'name': data[1],
                    'price': float(data[3]),
                    'prev_close': float(data[4]),
                    'open': float(data[5]),
                    'high': float(data[33]),
                    'low': float(data[34]),
                    'change': float(data[31]),
                    'change_pct': float(data[32]),
                    'volume': float(data[36]) * 100 if data[36] else 0,
                    'time': data[30],
                    'currency': 'CNY',
                    'cny_price': float(data[3]),
                    'market_type': 'cn',
                    'source': 'tencent'
                })
        return None

    def _fetch_a_stock_from_akshare(self, code: str) -> Optional[Dict]:
        """从AKShare获取A股价格 (备用源)"""
        try:
            import akshare as ak
            import pandas as pd

            # 标准化代码
            if code.startswith('SH') or code.startswith('SZ'):
                pure_code = code[2:]
            else:
                pure_code = code

            # 使用akshare获取实时行情
            df = ak.stock_zh_a_spot_em()

            # 查找对应代码
            row = df[df['代码'] == pure_code]
            if row.empty:
                return None

            data = row.iloc[0]

            return self._normalize_price_payload({
                'code': code,
                'name': data['名称'],
                'price': float(data['最新价']) if pd.notna(data['最新价']) else 0.0,
                'prev_close': float(data['昨收']) if pd.notna(data['昨收']) else 0.0,
                'open': float(data['今开']) if pd.notna(data['今开']) else 0.0,
                'high': float(data['最高']) if pd.notna(data['最高']) else 0.0,
                'low': float(data['最低']) if pd.notna(data['最低']) else 0.0,
                'change': float(data['涨跌额']) if pd.notna(data['涨跌额']) else 0.0,
                'change_pct': float(data['涨跌幅']) if pd.notna(data['涨跌幅']) else 0.0,
                'volume': float(data['成交量']) if pd.notna(data['成交量']) else 0.0,
                'time': data.get('时间', datetime.now().strftime('%H:%M:%S')),
                'currency': 'CNY',
                'cny_price': float(data['最新价']) if pd.notna(data['最新价']) else 0.0,
                'market_type': 'cn',
                'source': 'akshare'
            })
        except ImportError:
            print("[AKShare] 未安装akshare，跳过备用源")
            return None
        except Exception as e:
            print(f"[AKShare] 获取A股失败: {e}")
            return None

    def _fetch_hk_stock(self, code: str) -> Optional[Dict]:
        """获取港股价格 (腾讯主源 + AKShare备用)"""
        # 1. 先尝试腾讯API
        try:
            result = self._fetch_hk_stock_from_tencent(code)
            if result:
                return result
        except requests.Timeout:
            print(f"[超时] 腾讯API获取港股价格 {code}")
        except Exception as e:
            print(f"[腾讯API失败] 获取港股价格 {code}: {e}")

        # 2. 腾讯失败，尝试AKShare
        print(f"[备用源] 尝试AKShare获取港股 {code}...")
        try:
            result = self._fetch_hk_stock_from_akshare(code)
            if result:
                return result
        except Exception as e:
            print(f"[AKShare失败] 获取港股价格 {code}: {e}")

        return None

    def _fetch_hk_stock_from_tencent(self, code: str) -> Optional[Dict]:
        """从腾讯获取港股价格"""
        if code.startswith('HK'):
            numeric_part = code[2:].zfill(5)
        else:
            numeric_part = code.zfill(5)

        query_code = f'hk{numeric_part}'

        url = f"http://qt.gtimg.cn/q={query_code}"
        response = self.session.get(url, timeout=5)
        response.encoding = 'gb2312'
        text = response.text

        pattern = rf'v_{query_code}="([^"]+)"'
        match = re.search(pattern, text)

        if match:
            data = match.group(1).split('~')
            if len(data) > 45:
                price = float(data[3])
                rates = self._fetch_exchange_rates()
                hkd_cny = rates['HKDCNY']

                return self._normalize_price_payload({
                    'code': code,
                    'name': data[1],
                    'price': price,
                    'prev_close': float(data[4]),
                    'open': float(data[5]),
                    'high': float(data[33]),
                    'low': float(data[34]),
                    'change': float(data[31]),
                    'change_pct': float(data[32]),
                    'volume': float(data[36]) * 100 if data[36] else 0,
                    'time': data[30],
                    'currency': 'HKD',
                    'cny_price': price * hkd_cny,
                    'exchange_rate': hkd_cny,
                    'market_type': 'hk',
                    'source': 'tencent'
                })
        return None

    def _fetch_hk_stock_from_akshare(self, code: str) -> Optional[Dict]:
        """从AKShare获取港股价格 (备用源)"""
        try:
            import akshare as ak
            import pandas as pd

            # 标准化代码
            if code.startswith('HK'):
                pure_code = code[2:].zfill(5)
            else:
                pure_code = code.zfill(5)

            # 使用akshare获取港股实时行情
            df = ak.stock_hk_spot_em()

            # 查找对应代码
            row = df[df['代码'] == pure_code]
            if row.empty:
                return None

            data = row.iloc[0]
            price = float(data['最新价']) if pd.notna(data['最新价']) else 0.0

            # 获取汇率
            rates = self._fetch_exchange_rates()
            hkd_cny = rates['HKDCNY']

            return self._normalize_price_payload({
                'code': code,
                'name': data['名称'],
                'price': price,
                'prev_close': float(data['昨收']) if pd.notna(data['昨收']) else 0.0,
                'open': float(data['今开']) if pd.notna(data['今开']) else 0.0,
                'high': float(data['最高']) if pd.notna(data['最高']) else 0.0,
                'low': float(data['最低']) if pd.notna(data['最低']) else 0.0,
                'change': float(data['涨跌额']) if pd.notna(data['涨跌额']) else 0.0,
                'change_pct': float(data['涨跌幅']) if pd.notna(data['涨跌幅']) else 0.0,
                'volume': float(data['成交量']) if pd.notna(data['成交量']) else 0.0,
                'time': data.get('时间', datetime.now().strftime('%H:%M:%S')),
                'currency': 'HKD',
                'cny_price': price * hkd_cny,
                'exchange_rate': hkd_cny,
                'market_type': 'hk',
                'source': 'akshare'
            })
        except ImportError:
            print("[AKShare] 未安装akshare，跳过备用源")
            return None
        except Exception as e:
            print(f"[AKShare] 获取港股失败: {e}")
            return None

    def _fetch_us_stock(self, code: str) -> Optional[Dict]:
        """获取美股价格 (带多数据源备选和重试机制)

        数据源优先级:
        1. Finnhub API (如果配置了 API key) - 首选，稳定快速
        2. Yahoo Finance API - 免费备选
        3. yfinance 库 - 最后备选
        """
        yf_code = code.replace('.', '-')
        errors = []

        # 尝试1: Finnhub API (如果配置了 API key)
        finnhub_key = _config.get('finnhub_api_key')
        if finnhub_key:
            try:
                result = self._fetch_us_stock_finnhub(yf_code, finnhub_key)
                if result:
                    return result
            except Exception as e:
                errors.append(f"Finnhub: {e}")

        # 尝试2: Yahoo Finance 直接API (带重试)
        try:
            result = self._retry_with_backoff(
                lambda: self._fetch_us_stock_yahoo_api(yf_code),
                max_retries=2,
                base_delay=1.0
            )
            if result:
                return result
        except Exception as e:
            errors.append(f"Yahoo API: {e}")

        # 尝试3: yfinance库
        try:
            import yfinance as yf
            ticker = yf.Ticker(yf_code)
            info = ticker.info
            hist = ticker.history(period="1d")

            if not hist.empty:
                latest = hist.iloc[-1]
                prev_close = info.get('previousClose', latest['Open'])
                current = latest['Close']
                change = current - prev_close
                change_pct = (change / prev_close * 100) if prev_close else 0

                rates = self._fetch_exchange_rates()
                usd_cny = rates['USDCNY']

                return self._normalize_price_payload({
                    'code': code,
                    'name': info.get('shortName', yf_code),
                    'price': current,
                    'prev_close': prev_close,
                    'open': latest['Open'],
                    'high': latest['High'],
                    'low': latest['Low'],
                    'change': change,
                    'change_pct': change_pct,
                    'volume': int(latest['Volume']),
                    'currency': info.get('currency', 'USD'),
                    'cny_price': current * usd_cny,
                    'exchange_rate': usd_cny,
                    'market_type': 'us',
                    'source': 'yfinance'
                })
        except ImportError:
            errors.append("yfinance未安装")
        except Exception as e:
            errors.append(f"yfinance: {e}")

        # 所有数据源都失败
        print(f"获取美股价格失败 {code}: {'; '.join(errors)}")
        return None

    def _fetch_us_stock_finnhub(self, code: str, api_key: str) -> Optional[Dict]:
        """通过 Finnhub API 获取美股价格

        Args:
            code: 股票代码（如 AAPL, TSLA）
            api_key: Finnhub API key

        Returns:
            价格数据字典或 None
        """
        url = f"https://finnhub.io/api/v1/quote"
        params = {
            'symbol': code,
            'token': api_key
        }

        response = self.session.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()

        # Finnhub 返回字段：c(当前价), d(涨跌额), dp(涨跌幅%), h(最高), l(最低), o(开盘), pc(昨收)
        current = data.get('c')
        prev_close = data.get('pc')

        if not current:
            return None

        change = data.get('d', current - prev_close if prev_close else 0)
        change_pct = data.get('dp', (change / prev_close * 100) if prev_close else 0)

        rates = self._fetch_exchange_rates()
        usd_cny = rates['USDCNY']

        return self._normalize_price_payload({
            'code': code,
            'name': code,  # Finnhub quote 接口不返回名称，需要单独调用
            'price': current,
            'prev_close': prev_close if prev_close else current,
            'open': data.get('o', current),
            'high': data.get('h', current),
            'low': data.get('l', current),
            'change': change,
            'change_pct': change_pct,
            'currency': 'USD',
            'cny_price': current * usd_cny,
            'exchange_rate': usd_cny,
            'market_type': 'us',
            'source': 'finnhub'
        })

    def _fetch_us_stock_yahoo_api(self, code: str) -> Optional[Dict]:
        """通过Yahoo Finance直接API获取美股价格"""
        # Yahoo Finance 实时报价API
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{code}?interval=1d&range=2d"

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json',
        }

        response = self.session.get(url, headers=headers, timeout=15)

        if response.status_code == 429:
            raise Exception("Rate limited")

        response.raise_for_status()
        data = response.json()

        chart = data.get('chart', {})
        if chart.get('error'):
            raise Exception(chart['error'].get('description', 'Unknown error'))

        result = chart.get('result', [{}])[0]
        meta = result.get('meta', {})
        timestamps = result.get('timestamp', [])
        quotes = result.get('indicators', {}).get('quote', [{}])[0]

        if not timestamps or not quotes.get('close'):
            return None

        # 获取最新数据
        closes = quotes['close']
        opens = quotes.get('open', [])
        highs = quotes.get('high', [])
        lows = quotes.get('low', [])
        volumes = quotes.get('volume', [])

        # 过滤None值，获取最后一个有效价格
        valid_closes = [c for c in closes if c is not None]
        if not valid_closes:
            return None

        current = valid_closes[-1]
        prev_close = meta.get('previousClose') or meta.get('chartPreviousClose')

        # 如果只有一天数据，用开盘价作为昨收
        if prev_close is None and len(valid_closes) >= 2:
            prev_close = valid_closes[-2]
        elif prev_close is None and opens:
            prev_close = opens[0]
        else:
            prev_close = current

        change = current - prev_close
        change_pct = (change / prev_close * 100) if prev_close else 0

        # 获取最新有效的高低价和成交量
        valid_highs = [h for h in highs if h is not None]
        valid_lows = [l for l in lows if l is not None]
        valid_volumes = [v for v in volumes if v is not None]

        rates = self._fetch_exchange_rates()
        usd_cny = rates['USDCNY']

        return self._normalize_price_payload({
            'code': code,
            'name': meta.get('shortName') or meta.get('longName') or meta.get('symbol'),
            'price': current,
            'prev_close': prev_close,
            'open': opens[-1] if opens and opens[-1] else current,
            'high': valid_highs[-1] if valid_highs else current,
            'low': valid_lows[-1] if valid_lows else current,
            'change': change,
            'change_pct': change_pct,
            'volume': int(valid_volumes[-1]) if valid_volumes else 0,
            'currency': meta.get('currency', 'USD'),
            'cny_price': current * usd_cny,
            'exchange_rate': usd_cny,
            'market_type': 'us',
            'source': 'yahoo_api'
        })

    def _fetch_etf(self, code: str) -> Optional[Dict]:
        """获取ETF价格"""
        try:
            prefix = self._get_exchange_prefix(code)
            query_code = f'{prefix}{code}'

            url = f"http://qt.gtimg.cn/q={query_code}"
            response = self.session.get(url, timeout=10)
            response.encoding = 'gb2312'
            text = response.text

            pattern = rf'v_{query_code}="([^"]+)"'
            match = re.search(pattern, text)

            if match:
                data = match.group(1).split('~')
                if len(data) > 45:
                    return self._normalize_price_payload({
                        'code': code,
                        'name': data[1],
                        'price': float(data[3]),
                        'prev_close': float(data[4]),
                        'open': float(data[5]),
                        'high': float(data[33]),
                        'low': float(data[34]),
                        'change': float(data[31]),
                        'change_pct': float(data[32]),
                        'volume': float(data[36]) * 100 if data[36] else 0,
                        'time': data[30],
                        'currency': 'CNY',
                        'cny_price': float(data[3]),
                        'market_type': 'cn',
                        'source': 'tencent_etf'
                    })
            return None

        except Exception as e:
            print(f"获取ETF价格失败 {code}: {e}")
            return None

    def _fetch_fund(self, code: str) -> Optional[Dict]:
        """获取场外基金净值（优化版）

        优化策略：
        1. 优先使用单个基金查询接口（<1秒）
        2. 单个查询失败时，再使用全量排行接口（20-30秒）作为备用
        """
        import akshare as ak

        try:
            # 尝试1: 单个基金查询（快，<1秒）- 优先使用
            try:
                df = ak.fund_open_fund_info_em(symbol=code)
                if not df.empty and len(df) > 0:
                    latest = df.iloc[-1]
                    nav = float(latest['单位净值'])

                    # 过滤无效数据
                    if nav > 0:
                        change_pct = None
                        if '日增长率' in latest and latest['日增长率'] is not None:
                            try:
                                change_pct = float(latest['日增长率'])
                            except (ValueError, TypeError):
                                pass

                        # 尝试获取基金名称
                        name = None
                        try:
                            name_df = ak.fund_individual_basic_info_xq(symbol=code)
                            if not name_df.empty and '基金简称' in name_df.columns:
                                name = name_df['基金简称'].values[0]
                        except Exception:
                            pass

                        return self._normalize_price_payload({
                            'code': code,
                            'name': name,
                            'price': nav,
                            'nav_date': latest['净值日期'],
                            'change_pct': change_pct,
                            'currency': 'CNY',
                            'cny_price': nav,
                            'market_type': 'fund',
                            'source': 'akshare_info'  # 单个查询接口
                        })
            except Exception as e:
                print(f"[基金] 单个查询失败 {code}: {e}，尝试备用方案...")

            # 尝试2: 全量排行查询（慢，20-30秒）- 备用方案
            try:
                print(f"[基金] 正在从全量排行获取 {code}（可能需要20-30秒）...")
                df = ak.fund_open_fund_rank_em()
                fund_data = df[df['基金代码'] == code]

                if not fund_data.empty:
                    row = fund_data.iloc[0]
                    try:
                        change_pct = float(row['日增长率'])
                    except (ValueError, TypeError):
                        change_pct = None

                    return self._normalize_price_payload({
                        'code': code,
                        'name': row['基金简称'],
                        'price': float(row['单位净值']),
                        'nav_date': row['日期'],
                        'change_pct': change_pct,
                        'currency': 'CNY',
                        'cny_price': float(row['单位净值']),
                        'market_type': 'fund',
                        'source': 'akshare_rank'  # 全量排行接口
                    })
            except Exception:
                pass

            # 尝试3: 从东方财富网抓取
            try:
                result = self._fetch_fund_from_eastmoney(code)
                if result:
                    result['market_type'] = 'fund'
                    return result
            except Exception:
                pass

            return None

        except ImportError:
            return {'error': '请先安装 akshare: pip install akshare'}
        except Exception as e:
            print(f"获取基金价格失败 {code}: {e}")
            return None

    def _fetch_fund_from_eastmoney(self, code: str) -> Optional[Dict]:
        """从东方财富网获取基金净值"""
        try:
            url = f"http://fund.eastmoney.com/{code}.html"
            response = self.session.get(url, timeout=10)
            response.encoding = 'utf-8'
            text = response.text

            name_match = re.search(r'<h1[^>]*>([^<]+)</h1>', text)
            name = name_match.group(1).strip() if name_match else None

            nav_match = re.search(r'class="dataNums"[^>]*>\s*<span[^>]*>([\d.]+)</span>', text)
            if nav_match:
                nav = float(nav_match.group(1))

                date_match = re.search(r'(\d{4}-\d{2}-\d{2})', text)
                nav_date = date_match.group(1) if date_match else None

                change_match = re.search(r'class="(?:(?:ui-color-red)|(?:ui-color-green))"[^>]*>([+-]?[\d.]+)%', text)
                change_pct = float(change_match.group(1)) if change_match else None

                return self._normalize_price_payload({
                    'code': code,
                    'name': name,
                    'price': nav,
                    'nav_date': nav_date,
                    'change_pct': change_pct,
                    'currency': 'CNY',
                    'cny_price': nav,
                    'source': 'eastmoney'
                })
            return None

        except Exception as e:
            print(f"从东方财富获取基金价格失败 {code}: {e}")
            return None
