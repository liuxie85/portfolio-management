#!/usr/bin/env python3
"""Environment doctor for portfolio-management.

Checks:
- Python deps (pydantic/requests/akshare/yfinance)
- Network reachability for quote sources
- Feishu credentials sanity (can list fields for holdings)

Usage:
  . .venv/bin/activate
  python scripts/doctor.py
"""

# Ensure repo root is on sys.path when executed as a script.
import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import json
import socket
import ssl
import urllib.request
from typing import Dict, Any


def _check_import(name: str) -> Dict[str, Any]:
    try:
        mod = __import__(name)
        ver = getattr(mod, '__version__', None)
        return {"ok": True, "version": ver}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def _http_head(url: str, timeout: int = 5) -> Dict[str, Any]:
    try:
        req = urllib.request.Request(url, method="HEAD")
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return {"ok": True, "status": resp.status}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def main() -> None:
    report: Dict[str, Any] = {
        "imports": {
            "pydantic": _check_import("pydantic"),
            "requests": _check_import("requests"),
            "akshare": _check_import("akshare"),
            "yfinance": _check_import("yfinance"),
        },
        "network": {
            "tencent_qt": _http_head("http://qt.gtimg.cn/q=sh600519"),
            "yahoo_chart": _http_head("https://query1.finance.yahoo.com/v8/finance/chart/AAPL?interval=1d&range=2d"),
            "fx_erapi": _http_head("https://open.er-api.com/v6/latest/USD"),
        },
        "feishu": {},
        "ok": True,
    }

    # Feishu quick sanity
    try:
        from src.feishu_client import FeishuClient
        c = FeishuClient()
        app_token, table_id = c._get_table_config('holdings')
        endpoint = f"/bitable/v1/apps/{app_token}/tables/{table_id}/fields"
        data = c._request("GET", endpoint, params={"page_size": 5})
        report["feishu"] = {"ok": True, "holdings": f"{app_token}/{table_id}", "code": data.get('code')}
    except Exception as e:
        report["feishu"] = {"ok": False, "error": str(e)}

    # overall ok
    if not report["imports"]["pydantic"]["ok"] or not report["imports"]["requests"]["ok"]:
        report["ok"] = False
    if not report["feishu"].get("ok"):
        report["ok"] = False

    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
