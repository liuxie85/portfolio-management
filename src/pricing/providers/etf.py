"""ETF quote provider."""
from __future__ import annotations

import re
import time
from typing import Optional

from ..types import PriceRequest, ProviderResult


class ETFProvider:
    name = "etf"

    def __init__(self, fetcher):
        self.fetcher = fetcher

    def supports(self, request: PriceRequest) -> bool:
        return self.fetcher._is_etf(request.normalized_code or request.code)

    def fetch_one(self, request: PriceRequest) -> ProviderResult:
        started = time.time()
        code = request.normalized_code or request.code
        try:
            return ProviderResult(
                payload=self.fetch_etf(code),
                provider=self.name,
                latency_ms=int((time.time() - started) * 1000),
            )
        except Exception as exc:
            return ProviderResult(None, self.name, f"{type(exc).__name__}: {exc}", int((time.time() - started) * 1000))

    def fetch_etf(self, code: str) -> Optional[dict]:
        try:
            prefix = self.fetcher._get_exchange_prefix(code)
            query_code = f"{prefix}{code}"

            url = f"http://qt.gtimg.cn/q={query_code}"
            response = self.fetcher.session.get(url, timeout=10)
            response.encoding = "gb2312"
            text = response.text

            match = re.search(rf'v_{query_code}="([^"]+)"', text)
            if not match:
                return None

            data = match.group(1).split("~")
            if len(data) <= 45:
                return None

            return self.fetcher._normalize_price_payload(
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
                    "source": "tencent_etf",
                }
            )
        except Exception as exc:
            print(f"获取ETF价格失败 {code}: {exc}")
            return None
