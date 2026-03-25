# Valuation (CNY)

## Goal
Compute portfolio valuation in CNY with stable, auditable rules.

## Identity (sanity check)
Within tolerance:

`total_value_cny ≈ cash_value_cny + stock_value_cny + fund_value_cny`

## Classification
- cash-like: `*-CASH`, `*-MMF` or `asset_type=cash`
- CN stocks/ETFs: detected as `cn`
- HK stocks: detected as `hk`
- US stocks: detected as `us`
- open-end funds / fund NAV: detected as `fund`

## FX
- Convert non-CNY assets with current FX rate (USDCNY / HKDCNY).
- Missing FX MUST surface as warning (do not silently treat as 0).

## Timezone
All business dates are Beijing time (Asia/Shanghai).
