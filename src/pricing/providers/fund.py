"""Fund quote provider."""
from __future__ import annotations

import re
import time
from typing import Optional

from ..classifier import is_otc_fund
from ..payload import normalize_price_payload
from ..types import PriceRequest, ProviderResult


class FundProvider:
    name = "fund"

    def __init__(self, fetcher):
        self.fetcher = fetcher

    def supports(self, request: PriceRequest) -> bool:
        code = request.normalized_code or request.code
        hints = request.hints or {}
        return bool(hints.get("is_fund", False) or is_otc_fund(code))

    def fetch_one(self, request: PriceRequest) -> ProviderResult:
        started = time.time()
        code = request.normalized_code or request.code
        try:
            return ProviderResult(self.fetch_fund(code), self.name, latency_ms=int((time.time() - started) * 1000))
        except Exception as exc:
            return ProviderResult(None, self.name, f"{type(exc).__name__}: {exc}", int((time.time() - started) * 1000))

    def fetch_fund(self, code: str) -> Optional[dict]:
        try:
            result = self.fetch_from_tencent(code)
            if result:
                result["market_type"] = "fund"
                return result
        except Exception:
            pass

        try:
            import akshare as ak
            import pandas as pd

            try:
                df = ak.fund_open_fund_info_em(symbol=code)
                if not df.empty and len(df) > 0:
                    latest = df.iloc[-1]
                    nav = float(latest["单位净值"])
                    if nav > 0:
                        change_pct = None
                        if "日增长率" in latest and latest["日增长率"] is not None:
                            try:
                                change_pct = float(latest["日增长率"])
                            except (ValueError, TypeError):
                                pass

                        name = None
                        try:
                            name_df = ak.fund_individual_basic_info_xq(symbol=code)
                            if not name_df.empty and "基金简称" in name_df.columns:
                                name = name_df["基金简称"].values[0]
                        except Exception:
                            pass

                        return normalize_price_payload(
                            {
                                "code": code,
                                "name": name,
                                "price": nav,
                                "nav_date": str(latest.get("净值日期") or ""),
                                "change_pct": change_pct,
                                "currency": "CNY",
                                "cny_price": nav,
                                "market_type": "fund",
                                "source": "akshare_info",
                            }
                        )
            except Exception as exc:
                print(f"[基金] akshare 单基金查询失败 {code}: {exc}，尝试备用方案...")

            try:
                print(f"[基金] 正在从全量排行获取 {code}（可能需要20-30秒）...")
                df = ak.fund_open_fund_rank_em()
                fund_data = df[df["基金代码"] == code]
                if not fund_data.empty:
                    row = fund_data.iloc[0]
                    try:
                        change_pct = float(row["日增长率"])
                    except (ValueError, TypeError):
                        change_pct = None

                    nav = float(row["单位净值"]) if pd.notna(row["单位净值"]) else None
                    if nav and nav > 0:
                        return normalize_price_payload(
                            {
                                "code": code,
                                "name": row.get("基金简称"),
                                "price": nav,
                                "nav_date": str(row.get("日期") or ""),
                                "change_pct": change_pct,
                                "currency": "CNY",
                                "cny_price": nav,
                                "market_type": "fund",
                                "source": "akshare_rank",
                            }
                        )
            except Exception:
                pass
        except ImportError:
            pass
        except Exception as exc:
            print(f"获取基金价格失败 {code}: {exc}")

        try:
            result = self.fetch_from_eastmoney(code)
            if result:
                result["market_type"] = "fund"
                return result
        except Exception:
            pass
        return None

    def fetch_from_tencent(self, code: str) -> Optional[dict]:
        code = (code or "").strip().upper()
        if code.startswith(("SH", "SZ")):
            code = code[2:]
        if not (code.isdigit() and len(code) == 6):
            return None

        query_code = f"jj{code}"
        url = f"http://qt.gtimg.cn/q={query_code}"
        response = self.fetcher.session.get(url, timeout=5)
        response.encoding = "gb2312"
        text = response.text

        match = re.search(rf'v_{query_code}="([^"]+)"', text)
        if not match:
            return None

        parts = match.group(1).split("~")
        if len(parts) < 9:
            return None

        try:
            nav = float(parts[5])
        except Exception:
            return None
        if not nav or nav <= 0:
            return None

        return normalize_price_payload(
            {
                "code": code,
                "name": parts[1],
                "price": nav,
                "nav_date": parts[8] if len(parts) > 8 else None,
                "currency": "CNY",
                "cny_price": nav,
                "source": "tencent_jj",
            }
        )

    def fetch_from_eastmoney(self, code: str) -> Optional[dict]:
        try:
            url = f"http://fund.eastmoney.com/{code}.html"
            response = self.fetcher.session.get(url, timeout=10)
            response.encoding = "utf-8"
            text = response.text

            name_match = re.search(r"<h1[^>]*>([^<]+)</h1>", text)
            name = name_match.group(1).strip() if name_match else None
            nav_match = re.search(r'class="dataNums"[^>]*>\s*<span[^>]*>([\d.]+)</span>', text)
            if not nav_match:
                return None

            nav = float(nav_match.group(1))
            date_match = re.search(r"(\d{4}-\d{2}-\d{2})", text)
            change_match = re.search(r'class="(?:(?:ui-color-red)|(?:ui-color-green))"[^>]*>([+-]?[\d.]+)%', text)
            return normalize_price_payload(
                {
                    "code": code,
                    "name": name,
                    "price": nav,
                    "nav_date": date_match.group(1) if date_match else None,
                    "change_pct": float(change_match.group(1)) if change_match else None,
                    "currency": "CNY",
                    "cny_price": nav,
                    "source": "eastmoney",
                }
            )
        except Exception as exc:
            print(f"从东方财富获取基金价格失败 {code}: {exc}")
            return None
