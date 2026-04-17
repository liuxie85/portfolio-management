"""Pricing request classification helpers.

These helpers are provider-facing. ``PriceFetcher`` keeps compatibility wrapper
methods, but routing/classification should live in ``src.pricing``.
"""
from __future__ import annotations

from typing import Dict


STOCK_KEYWORDS = ["股份", "银行", "证券", "保险", "科技", "控股", "集团", "stock"]
FUND_KEYWORDS = ["基金", "混合", "债券", "货币", "指数", "ETF", "etf", "fund"]
CASH_KEYWORDS = ["现金", "货币", "mmf", "cash", "余额宝"]


def normalize_code_with_name(code: str, name: str = "") -> str:
    code = (code or "").upper().strip()
    if code.startswith(("SH", "SZ")):
        return code

    if not (code.isdigit() and len(code) == 6):
        return code

    hints = get_type_hints_from_name(name)
    if hints.get("is_stock") and not hints.get("is_fund"):
        if code.startswith("6"):
            return f"SH{code}"
        if code.startswith(("0", "3")):
            return f"SZ{code}"

    return code


def get_type_hints_from_name(name: str = "") -> Dict[str, bool]:
    if not name:
        return {}

    name_lower = name.lower()
    hints: Dict[str, bool] = {}
    hints["is_fund"] = any(kw.lower() in name_lower for kw in FUND_KEYWORDS)
    hints["is_etf"] = "etf" in name_lower
    hints["is_stock"] = any(kw.lower() in name_lower for kw in STOCK_KEYWORDS) and not hints["is_fund"]
    hints["is_cash"] = any(kw.lower() in name_lower for kw in CASH_KEYWORDS)
    return hints


def is_etf(code: str) -> bool:
    code = (code or "").upper().strip()
    if not code.isdigit() or len(code) != 6:
        return False
    if code.startswith("5"):
        return True
    if code.startswith("15") and not code.startswith("16"):
        return True
    return False


def is_otc_fund(code: str) -> bool:
    code = (code or "").upper().strip()
    if not code.isdigit() or len(code) != 6:
        return False
    if code.startswith(("600", "601", "603", "605", "688", "689", "300", "301")):
        return False
    if code.startswith("5") or code.startswith("15"):
        return False
    if code.startswith(("004", "005", "006", "007", "008", "009")):
        return True
    if code.startswith(("01", "27", "16")):
        return True
    return False


def get_exchange_prefix(code: str) -> str:
    code = (code or "").upper().strip()
    if code.startswith(("6", "5")):
        return "sh"
    return "sz"
