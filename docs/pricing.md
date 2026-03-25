# Pricing & Caching

## Sources (priority)

1) **Tencent batch** for CN/HK/ETF: `qt.gtimg.cn` (fast, low dependency)
2) **Tencent jj** for open-end fund NAV (fast, low dependency)
3) US: Finnhub (if API key) → Yahoo chart API fallback
4) akshare / eastmoney as slow fallbacks for specific assets

## Cache policy

- If cache is valid (not expired): MUST use cache (unless `force_refresh=True`).
- If cache expired: try realtime.
  - If realtime succeeds: update cache.
  - If realtime fails: may fallback to stale cache, but MUST set `is_stale=true`.

## TTL

TTL is computed by `src/market_time.py::MarketTimeUtil.get_cache_ttl()`.

- Market open → 30 minutes
- Market closed → until next open (or fund next update)

## Observability

- `PortfolioManager.calculate_valuation()` appends a warning line:
  - `[价格汇总] realtime=..., cache=..., stale_fallback=..., missing=...`
  - plus Tencent batch meta when available.

- Use `scripts/diagnose_pricing.py` to inspect per-asset states.
