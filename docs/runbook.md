# Runbook

## 1) Daily report link returns 502

- Confirm an HTTP server is listening on `0.0.0.0:3000` inside the container.
- Confirm publish root points to the directory containing `investment-daily-YYYY-MM-DD/index.html`.

## 2) "Report didn't refresh prices"

- Run `python scripts/diagnose_pricing.py --account lx`.
- Check:
  - `summary.realtime/cache/stale_fallback/missing`
  - `tencent_batch` meta (requests/elapsed/coverage)
  - per-asset `state` and `source`

## 3) Missing price for US tickers

- Ensure `finnhub_api_key` is set.
- If Finnhub fails, Yahoo chart API may be rate-limited.

## 4) Date off by one day

- Business dates are Beijing time.
- Check code uses `src/time_utils.py` helpers.

## 5) Feishu field not found

- Compare actual Bitable fields with `docs/schema.md`.
- (Planned) use `scripts/schema_doctor.py`.
