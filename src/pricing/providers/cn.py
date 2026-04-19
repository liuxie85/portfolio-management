"""China A-share quote provider."""
from __future__ import annotations

import time
from typing import Optional

import requests

from src.time_utils import bj_now_naive

from ..classifier import is_otc_fund
from ..payload import normalize_price_payload
from ..types import PriceRequest, ProviderResult


class CNStockProvider:
    name = "cn-stock"

    def __init__(self, fetcher):
        self.fetcher = fetcher

    def supports(self, request: PriceRequest) -> bool:
        code = request.normalized_code or request.code
        if not (
            code.startswith(("SH", "SZ"))
            or (
                code.isdigit()
                and len(code) == 6
                and code.startswith(("6", "0", "3", "1", "2"))
            )
        ):
            return False
        hints = request.hints or {}
        is_likely_fund = hints.get("is_fund", False) or is_otc_fund(code)
        return not (is_likely_fund and not hints.get("is_stock", False))

    def fetch_one(self, request: PriceRequest) -> ProviderResult:
        started = time.time()
        code = request.normalized_code or request.code
        try:
            return ProviderResult(self.fetch_a_stock(code), self.name, latency_ms=int((time.time() - started) * 1000))
        except Exception as exc:
            return ProviderResult(None, self.name, f"{type(exc).__name__}: {exc}", int((time.time() - started) * 1000))

    def fetch_a_stock(self, code: str) -> Optional[dict]:
        try:
            result = self.fetch_from_tencent(code)
            if result:
                return result
        except requests.Timeout:
            print(f"[超时] 腾讯API获取A股价格 {code}")
        except Exception as exc:
            print(f"[腾讯API失败] 获取A股价格 {code}: {exc}")

        print(f"[备用源] 尝试AKShare获取A股 {code}...")
        try:
            result = self.fetch_from_akshare(code)
            if result:
                return result
        except Exception as exc:
            print(f"[AKShare失败] 获取A股价格 {code}: {exc}")

        return None

    def fetch_from_tencent(self, code: str) -> Optional[dict]:
        if code.startswith(("SH", "SZ")):
            query_code = code.lower()
        elif code.isdigit():
            query_code = f"sh{code}" if code.startswith("6") else f"sz{code}"
        else:
            query_code = code

        from src.tencent_batch import fetch_batch as tencent_fetch_batch

        parts_map, _meta = tencent_fetch_batch(self.fetcher.session, [query_code], timeout=5, chunk_size=1)
        data = parts_map.get(query_code)
        if data and len(data) > 45:
            return normalize_price_payload(
                {
                    "code": code,
                    "name": data[1],
                    "price": float(data[3]),
                    "prev_close": float(data[4]),
                    "open": float(data[5]),
                    "high": float(data[33]),
                    "low": float(data[34]),
                    "change": float(data[31]),
                    "change_pct": float(data[32]),
                    "volume": float(data[36]) * 100 if data[36] else 0,
                    "time": data[30],
                    "currency": "CNY",
                    "cny_price": float(data[3]),
                    "market_type": "cn",
                    "source": "tencent",
                }
            )
        return None

    def fetch_from_akshare(self, code: str) -> Optional[dict]:
        """Fallback: downloads full A-share market data (~5000 rows) for a single stock."""
        try:
            import akshare as ak
            import pandas as pd
            import logging
            logging.getLogger(__name__).debug("akshare fallback: downloading full A-share market for %s", code)

            pure_code = code[2:] if code.startswith(("SH", "SZ")) else code
            df = ak.stock_zh_a_spot_em()
            row = df[df["代码"] == pure_code]
            if row.empty:
                return None

            data = row.iloc[0]
            return normalize_price_payload(
                {
                    "code": code,
                    "name": data["名称"],
                    "price": float(data["最新价"]) if pd.notna(data["最新价"]) else 0.0,
                    "prev_close": float(data["昨收"]) if pd.notna(data["昨收"]) else 0.0,
                    "open": float(data["今开"]) if pd.notna(data["今开"]) else 0.0,
                    "high": float(data["最高"]) if pd.notna(data["最高"]) else 0.0,
                    "low": float(data["最低"]) if pd.notna(data["最低"]) else 0.0,
                    "change": float(data["涨跌额"]) if pd.notna(data["涨跌额"]) else 0.0,
                    "change_pct": float(data["涨跌幅"]) if pd.notna(data["涨跌幅"]) else 0.0,
                    "volume": float(data["成交量"]) if pd.notna(data["成交量"]) else 0.0,
                    "time": data.get("时间", bj_now_naive().strftime("%H:%M:%S")),
                    "currency": "CNY",
                    "cny_price": float(data["最新价"]) if pd.notna(data["最新价"]) else 0.0,
                    "market_type": "cn",
                    "source": "akshare",
                }
            )
        except ImportError:
            print("[AKShare] 未安装akshare，跳过备用源")
            return None
        except Exception as exc:
            print(f"[AKShare] 获取A股失败: {exc}")
            return None
