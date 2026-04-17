"""Hong Kong stock quote provider."""
from __future__ import annotations

import time
from typing import Optional

import requests

from src.time_utils import bj_now_naive

from ..payload import normalize_price_payload
from ..types import PriceRequest, ProviderResult


class HKStockProvider:
    name = "hk-stock"

    def __init__(self, fetcher):
        self.fetcher = fetcher

    def supports(self, request: PriceRequest) -> bool:
        code = request.normalized_code or request.code
        return code.startswith("HK") or (code.isdigit() and 4 <= len(code) <= 5)

    def fetch_one(self, request: PriceRequest) -> ProviderResult:
        started = time.time()
        code = request.normalized_code or request.code
        try:
            return ProviderResult(self.fetch_hk_stock(code), self.name, latency_ms=int((time.time() - started) * 1000))
        except Exception as exc:
            return ProviderResult(None, self.name, f"{type(exc).__name__}: {exc}", int((time.time() - started) * 1000))

    def fetch_hk_stock(self, code: str) -> Optional[dict]:
        try:
            result = self.fetch_from_tencent(code)
            if result:
                return result
        except requests.Timeout:
            print(f"[超时] 腾讯API获取港股价格 {code}")
        except Exception as exc:
            print(f"[腾讯API失败] 获取港股价格 {code}: {exc}")

        print(f"[备用源] 尝试AKShare获取港股 {code}...")
        try:
            result = self.fetch_from_akshare(code)
            if result:
                return result
        except Exception as exc:
            print(f"[AKShare失败] 获取港股价格 {code}: {exc}")
        return None

    def fetch_from_tencent(self, code: str) -> Optional[dict]:
        numeric_part = code[2:].zfill(5) if code.startswith("HK") else code.zfill(5)
        query_code = f"hk{numeric_part}"

        from src.tencent_batch import fetch_batch as tencent_fetch_batch

        parts_map, _meta = tencent_fetch_batch(self.fetcher.session, [query_code], timeout=5, chunk_size=1)
        data = parts_map.get(query_code)
        if data and len(data) > 45:
            price = float(data[3])
            hkd_cny = self.fetcher._fetch_exchange_rates()["HKDCNY"]
            return normalize_price_payload(
                {
                    "code": code,
                    "name": data[1],
                    "price": price,
                    "prev_close": float(data[4]),
                    "open": float(data[5]),
                    "high": float(data[33]),
                    "low": float(data[34]),
                    "change": float(data[31]),
                    "change_pct": float(data[32]),
                    "volume": float(data[36]) * 100 if data[36] else 0,
                    "time": data[30],
                    "currency": "HKD",
                    "cny_price": price * hkd_cny,
                    "exchange_rate": hkd_cny,
                    "market_type": "hk",
                    "source": "tencent",
                }
            )
        return None

    def fetch_from_akshare(self, code: str) -> Optional[dict]:
        try:
            import akshare as ak
            import pandas as pd

            pure_code = code[2:].zfill(5) if code.startswith("HK") else code.zfill(5)
            df = ak.stock_hk_spot_em()
            row = df[df["代码"] == pure_code]
            if row.empty:
                return None

            data = row.iloc[0]
            price = float(data["最新价"]) if pd.notna(data["最新价"]) else 0.0
            hkd_cny = self.fetcher._fetch_exchange_rates()["HKDCNY"]

            return normalize_price_payload(
                {
                    "code": code,
                    "name": data["名称"],
                    "price": price,
                    "prev_close": float(data["昨收"]) if pd.notna(data["昨收"]) else 0.0,
                    "open": float(data["今开"]) if pd.notna(data["今开"]) else 0.0,
                    "high": float(data["最高"]) if pd.notna(data["最高"]) else 0.0,
                    "low": float(data["最低"]) if pd.notna(data["最低"]) else 0.0,
                    "change": float(data["涨跌额"]) if pd.notna(data["涨跌额"]) else 0.0,
                    "change_pct": float(data["涨跌幅"]) if pd.notna(data["涨跌幅"]) else 0.0,
                    "volume": float(data["成交量"]) if pd.notna(data["成交量"]) else 0.0,
                    "time": data.get("时间", bj_now_naive().strftime("%H:%M:%S")),
                    "currency": "HKD",
                    "cny_price": price * hkd_cny,
                    "exchange_rate": hkd_cny,
                    "market_type": "hk",
                    "source": "akshare",
                }
            )
        except ImportError:
            print("[AKShare] 未安装akshare，跳过备用源")
            return None
        except Exception as exc:
            print(f"[AKShare] 获取港股失败: {exc}")
            return None
