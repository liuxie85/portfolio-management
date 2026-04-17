"""US stock quote provider."""
from __future__ import annotations

import time
from typing import Optional

from src import config as _config

from ..payload import normalize_price_payload
from ..types import PriceRequest, ProviderResult


class USStockProvider:
    name = "us-stock"

    def __init__(self, fetcher):
        self.fetcher = fetcher

    def supports(self, request: PriceRequest) -> bool:
        return True

    def fetch_one(self, request: PriceRequest) -> ProviderResult:
        started = time.time()
        code = request.normalized_code or request.code
        try:
            return ProviderResult(self.fetch_us_stock(code), self.name, latency_ms=int((time.time() - started) * 1000))
        except Exception as exc:
            return ProviderResult(None, self.name, f"{type(exc).__name__}: {exc}", int((time.time() - started) * 1000))

    def fetch_us_stock(self, code: str) -> Optional[dict]:
        yf_code = code.replace(".", "-")
        errors = []

        finnhub_key = _config.get("finnhub_api_key")
        if finnhub_key:
            try:
                result = self.fetch_finnhub(yf_code, finnhub_key)
                if result:
                    return result
            except Exception as exc:
                errors.append(f"Finnhub: {exc}")

        try:
            result = self.fetcher._retry_with_backoff(
                lambda: self.fetch_yahoo_api(yf_code),
                max_retries=2,
                base_delay=1.0,
            )
            if result:
                return result
        except Exception as exc:
            errors.append(f"Yahoo API: {exc}")

        try:
            import yfinance as yf

            ticker = yf.Ticker(yf_code)
            info = ticker.info
            hist = ticker.history(period="1d")
            if not hist.empty:
                latest = hist.iloc[-1]
                prev_close = info.get("previousClose", latest["Open"])
                current = latest["Close"]
                change = current - prev_close
                change_pct = (change / prev_close * 100) if prev_close else 0

                usd_cny = self.fetcher._fetch_exchange_rates()["USDCNY"]
                return normalize_price_payload(
                    {
                        "code": code,
                        "name": info.get("shortName", yf_code),
                        "price": current,
                        "prev_close": prev_close,
                        "open": latest["Open"],
                        "high": latest["High"],
                        "low": latest["Low"],
                        "change": change,
                        "change_pct": change_pct,
                        "volume": int(latest["Volume"]),
                        "currency": info.get("currency", "USD"),
                        "cny_price": current * usd_cny,
                        "exchange_rate": usd_cny,
                        "market_type": "us",
                        "source": "yfinance",
                    }
                )
        except ImportError:
            errors.append("yfinance未安装")
        except Exception as exc:
            errors.append(f"yfinance: {exc}")

        print(f"获取美股价格失败 {code}: {'; '.join(errors)}")
        return None

    def fetch_finnhub(self, code: str, api_key: str) -> Optional[dict]:
        response = self.fetcher.session.get(
            "https://finnhub.io/api/v1/quote",
            params={"symbol": code, "token": api_key},
            timeout=10,
        )
        response.raise_for_status()
        data = response.json()

        current = data.get("c")
        prev_close = data.get("pc")
        if not current:
            return None

        change = data.get("d", current - prev_close if prev_close else 0)
        change_pct = data.get("dp", (change / prev_close * 100) if prev_close else 0)
        usd_cny = self.fetcher._fetch_exchange_rates()["USDCNY"]

        return normalize_price_payload(
            {
                "code": code,
                "name": code,
                "price": current,
                "prev_close": prev_close if prev_close else current,
                "open": data.get("o", current),
                "high": data.get("h", current),
                "low": data.get("l", current),
                "change": change,
                "change_pct": change_pct,
                "currency": "USD",
                "cny_price": current * usd_cny,
                "exchange_rate": usd_cny,
                "market_type": "us",
                "source": "finnhub",
            }
        )

    def fetch_yahoo_api(self, code: str) -> Optional[dict]:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{code}?interval=1d&range=2d"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json",
        }
        response = self.fetcher.session.get(url, headers=headers, timeout=15)
        if response.status_code == 429:
            raise Exception("Rate limited")
        response.raise_for_status()
        data = response.json()

        chart = data.get("chart", {})
        if chart.get("error"):
            raise Exception(chart["error"].get("description", "Unknown error"))

        result = chart.get("result", [{}])[0]
        meta = result.get("meta", {})
        timestamps = result.get("timestamp", [])
        quotes = result.get("indicators", {}).get("quote", [{}])[0]
        if not timestamps or not quotes.get("close"):
            return None

        closes = quotes["close"]
        opens = quotes.get("open", [])
        highs = quotes.get("high", [])
        lows = quotes.get("low", [])
        volumes = quotes.get("volume", [])
        valid_closes = [c for c in closes if c is not None]
        if not valid_closes:
            return None

        current = valid_closes[-1]
        prev_close = meta.get("previousClose") or meta.get("chartPreviousClose")
        if prev_close is None and len(valid_closes) >= 2:
            prev_close = valid_closes[-2]
        elif prev_close is None and opens:
            prev_close = opens[0]
        else:
            prev_close = current

        change = current - prev_close
        change_pct = (change / prev_close * 100) if prev_close else 0
        valid_highs = [h for h in highs if h is not None]
        valid_lows = [l for l in lows if l is not None]
        valid_volumes = [v for v in volumes if v is not None]

        usd_cny = self.fetcher._fetch_exchange_rates()["USDCNY"]
        return normalize_price_payload(
            {
                "code": code,
                "name": meta.get("shortName") or meta.get("longName") or meta.get("symbol"),
                "price": current,
                "prev_close": prev_close,
                "open": opens[-1] if opens and opens[-1] else current,
                "high": valid_highs[-1] if valid_highs else current,
                "low": valid_lows[-1] if valid_lows else current,
                "change": change,
                "change_pct": change_pct,
                "volume": int(valid_volumes[-1]) if valid_volumes else 0,
                "currency": meta.get("currency", "USD"),
                "cny_price": current * usd_cny,
                "exchange_rate": usd_cny,
                "market_type": "us",
                "source": "yahoo_api",
            }
        )
