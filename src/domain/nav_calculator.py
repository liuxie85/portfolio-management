"""Pure NAV calculation and validation helpers."""
from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, Optional

from src.models import NAVHistory


class NavCalculator:
    MONEY_QUANT = Decimal("0.01")
    NAV_QUANT = Decimal("0.000001")
    WEIGHT_QUANT = Decimal("0.000001")

    @staticmethod
    def to_decimal(value: Any) -> Decimal:
        if value is None:
            return Decimal("0")
        if isinstance(value, Decimal):
            return value
        return Decimal(str(value))

    @classmethod
    def quantize_money(cls, value: Any) -> Decimal:
        return cls.to_decimal(value).quantize(cls.MONEY_QUANT, rounding=ROUND_HALF_UP)

    @classmethod
    def quantize_nav(cls, value: Any) -> Decimal:
        return cls.to_decimal(value).quantize(cls.NAV_QUANT, rounding=ROUND_HALF_UP)

    @classmethod
    def quantize_weight(cls, value: Any) -> Decimal:
        return cls.to_decimal(value).quantize(cls.WEIGHT_QUANT, rounding=ROUND_HALF_UP)

    @classmethod
    def calc_period_return(cls, current_value: float, base_value: Optional[float]) -> float:
        if base_value is None:
            return 0.0
        current_dec = cls.to_decimal(current_value)
        base_dec = cls.to_decimal(base_value)
        if base_dec <= 0:
            return 0.0
        return float((current_dec - base_dec) / base_dec)

    @classmethod
    def calc_mtd_nav_change(cls, nav: float, prev_month_end_nav) -> Optional[float]:
        base_nav = prev_month_end_nav.nav if prev_month_end_nav else None
        if base_nav is None or base_nav <= 0:
            return None
        return cls.calc_period_return(nav, base_nav)

    @classmethod
    def calc_ytd_nav_change(cls, nav: float, prev_year_end_nav) -> Optional[float]:
        base_nav = prev_year_end_nav.nav if prev_year_end_nav else None
        if base_nav is None or base_nav <= 0:
            return None
        return cls.calc_period_return(nav, base_nav)

    @classmethod
    def calc_mtd_pnl(cls, total_value: float, prev_month_end_nav, monthly_cash_flow: float) -> Optional[float]:
        if not prev_month_end_nav:
            return None
        return float(
            cls.to_decimal(total_value)
            - cls.to_decimal(prev_month_end_nav.total_value)
            - cls.to_decimal(monthly_cash_flow)
        )

    @classmethod
    def calc_ytd_pnl(cls, total_value: float, prev_year_end_nav, yearly_cash_flow: float) -> Optional[float]:
        if not prev_year_end_nav:
            return None
        return float(
            cls.to_decimal(total_value)
            - cls.to_decimal(prev_year_end_nav.total_value)
            - cls.to_decimal(yearly_cash_flow)
        )

    @classmethod
    def calc_nav_metrics(
        cls,
        *,
        today,
        total_value,
        yesterday_nav,
        prev_year_end_nav,
        prev_month_end_nav,
        last_nav,
        yearly_data,
        daily_cash_flow,
        monthly_cash_flow,
        yearly_cash_flow,
        cumulative_cash_flow,
        start_year,
        initial_value: Optional[float],
        gap_cash_flow=None,
    ) -> dict:
        """Calculate shares, NAV deltas, PnL, and CAGR.

        Mutates ``yearly_data`` by filling ``nav_change`` and ``appreciation``
        to preserve the existing PortfolioManager contract.
        """
        cf_for_shares = gap_cash_flow if gap_cash_flow is not None else daily_cash_flow
        cf_for_shares_dec = cls.to_decimal(cf_for_shares)
        total_value_dec = cls.to_decimal(total_value)
        last_nav_nav_dec = cls.to_decimal(last_nav.nav) if (last_nav and last_nav.nav is not None) else None
        last_nav_shares_dec = cls.to_decimal(last_nav.shares) if (last_nav and last_nav.shares is not None) else None

        if last_nav and last_nav_nav_dec is not None and last_nav_nav_dec > 0:
            shares_change_dec = cf_for_shares_dec / last_nav_nav_dec
            shares_dec = (last_nav_shares_dec or Decimal("0")) + shares_change_dec
        else:
            shares_change_dec = cf_for_shares_dec
            shares_dec = total_value_dec

        nav_dec = (total_value_dec / shares_dec) if shares_dec > 0 else Decimal("1.0")
        nav_dec = cls.quantize_nav(nav_dec)

        shares_change = float(shares_change_dec)
        shares = float(shares_dec)
        nav = float(nav_dec)

        month_nav_change = cls.calc_mtd_nav_change(nav, prev_month_end_nav)
        year_nav_change = cls.calc_ytd_nav_change(nav, prev_year_end_nav)

        for yd in yearly_data.values():
            base, e = yd["prev_end"], yd["end"]
            if e and base and base.nav is not None and base.nav > 0:
                yd["nav_change"] = cls.calc_period_return(e.nav, base.nav)
            else:
                yd["nav_change"] = None

        cumulative_nav_change = 0.0
        first_year_data = yearly_data.get(str(start_year))
        if first_year_data and first_year_data["prev_end"]:
            cumulative_nav_change = cls.calc_period_return(nav, first_year_data["prev_end"].nav)

        if yesterday_nav and yesterday_nav.date and (today - yesterday_nav.date).days == 1:
            daily_appreciation = float(total_value_dec - cls.to_decimal(yesterday_nav.total_value) - cf_for_shares_dec)
        else:
            daily_appreciation = None

        month_appreciation = cls.calc_mtd_pnl(total_value, prev_month_end_nav, monthly_cash_flow)
        year_appreciation = cls.calc_ytd_pnl(total_value, prev_year_end_nav, yearly_cash_flow)

        sorted_years = sorted(yearly_data.keys())
        for i, yr_str in enumerate(sorted_years):
            yd = yearly_data[yr_str]
            if i == 0:
                if yd["end"] and initial_value is not None:
                    yd["appreciation"] = yd["end"].total_value - initial_value - yd["cash_flow"]
                else:
                    yd["appreciation"] = None
            else:
                prev_yd = yearly_data[sorted_years[i - 1]]
                if yd["end"] and prev_yd["end"]:
                    yd["appreciation"] = yd["end"].total_value - prev_yd["end"].total_value - yd["cash_flow"]
                else:
                    yd["appreciation"] = None

        cumulative_appreciation = (total_value - initial_value - cumulative_cash_flow) if initial_value else 0.0

        cagr = 0.0
        if first_year_data and first_year_data["prev_end"] and first_year_data["prev_end"].nav > 0 and nav > 0:
            days_since_start = (today - first_year_data["prev_end"].date).days
            years_since_start = days_since_start / 365.25
            if years_since_start > 0:
                cagr = (nav / first_year_data["prev_end"].nav) ** (1 / years_since_start) - 1

        return {
            "shares": shares,
            "shares_change": shares_change,
            "nav": nav,
            "month_nav_change": month_nav_change,
            "year_nav_change": year_nav_change,
            "cumulative_nav_change": cumulative_nav_change,
            "daily_appreciation": daily_appreciation,
            "month_appreciation": month_appreciation,
            "year_appreciation": year_appreciation,
            "cumulative_appreciation": cumulative_appreciation,
            "initial_value": initial_value,
            "first_year_data": first_year_data,
            "cagr": cagr,
        }

    @classmethod
    def approx_equal(cls, a: Optional[float], b: Optional[float], tolerance: float = 1e-6) -> bool:
        if a is None or b is None:
            return a is b
        return abs(cls.to_decimal(a) - cls.to_decimal(b)) <= cls.to_decimal(tolerance)

    @classmethod
    def approx_equal_quantized(cls, a: Optional[float], b: Optional[float], quantizer, *, tolerance: float = 0.0) -> bool:
        if a is None or b is None:
            return a is b
        qa = quantizer(a)
        qb = quantizer(b)
        if tolerance and tolerance > 0:
            return cls.approx_equal(float(qa), float(qb), tolerance=tolerance)
        return qa == qb

    @classmethod
    def money_equal(cls, a: Optional[float], b: Optional[float]) -> bool:
        if a is None or b is None:
            return a is b
        return cls.quantize_money(a) == cls.quantize_money(b)

    @classmethod
    def nav_equal(cls, a: Optional[float], b: Optional[float]) -> bool:
        if a is None or b is None:
            return a is b
        return cls.quantize_nav(a) == cls.quantize_nav(b)

    @classmethod
    def validate_nav_record(
        cls,
        *,
        nav_record: NAVHistory,
        last_nav=None,
        prev_month_end_nav=None,
        prev_year_end_nav=None,
        daily_cash_flow: float = 0.0,
        monthly_cash_flow: float = 0.0,
        yearly_cash_flow: float = 0.0,
        gap_cash_flow: Optional[float] = None,
        initial_value: Optional[float] = None,
        cumulative_cash_flow: float = 0.0,
    ) -> None:
        errors = []

        if nav_record.cash_value is None or nav_record.stock_value is None:
            errors.append("cash_value/stock_value 缺失（必填）")
        else:
            expected_total = float(cls.quantize_money(cls.to_decimal(nav_record.stock_value) + cls.to_decimal(nav_record.cash_value)))
            if not cls.approx_equal(nav_record.total_value, expected_total, tolerance=0.06):
                errors.append(f"total_value 不等于 stock_value + cash_value: {nav_record.total_value} != {expected_total}")

        if nav_record.total_value and nav_record.total_value > 0 and nav_record.stock_weight is not None and nav_record.cash_weight is not None:
            weights_sum = nav_record.stock_weight + nav_record.cash_weight
            if not cls.approx_equal(weights_sum, 1.0, tolerance=1e-4):
                errors.append(f"stock_weight + cash_weight 不接近 1: {weights_sum}")

        if nav_record.shares and nav_record.shares > 0 and nav_record.nav is not None:
            expected_nav = float(cls.quantize_nav(cls.to_decimal(nav_record.total_value) / cls.to_decimal(nav_record.shares)))
            if not cls.approx_equal(nav_record.nav, expected_nav, tolerance=1e-6):
                errors.append(f"nav 不等于 total_value / shares: {nav_record.nav} != {expected_nav}")

        effective_cash_flow = gap_cash_flow if gap_cash_flow is not None else daily_cash_flow
        if last_nav and last_nav.shares is not None and (effective_cash_flow == 0 or cls.approx_equal(effective_cash_flow, 0.0, tolerance=0.01)):
            expected_shares = float(cls.quantize_money(last_nav.shares))
            if not cls.approx_equal(nav_record.shares, expected_shares, tolerance=0.01):
                errors.append(f"无资金流时 shares 不应变化: {nav_record.shares} != {expected_shares}")
            if not cls.money_equal(nav_record.share_change, 0.0):
                errors.append(f"无资金流时 share_change 不应变化: {nav_record.share_change}")

        expected_mtd = cls.calc_mtd_nav_change(nav_record.nav, prev_month_end_nav) if nav_record.nav is not None else None
        if not cls.approx_equal_quantized(nav_record.mtd_nav_change, expected_mtd, cls.quantize_nav):
            errors.append(f"mtd_nav_change 不一致: {nav_record.mtd_nav_change} != {expected_mtd}")

        expected_ytd = cls.calc_ytd_nav_change(nav_record.nav, prev_year_end_nav) if nav_record.nav is not None else None
        if not cls.approx_equal_quantized(nav_record.ytd_nav_change, expected_ytd, cls.quantize_nav):
            errors.append(f"ytd_nav_change 不一致: {nav_record.ytd_nav_change} != {expected_ytd}")

        expected_mtd_pnl = cls.calc_mtd_pnl(nav_record.total_value, prev_month_end_nav, monthly_cash_flow)
        if expected_mtd_pnl is not None:
            expected_mtd_pnl = float(cls.quantize_money(expected_mtd_pnl))
        if not cls.money_equal(nav_record.mtd_pnl, expected_mtd_pnl):
            errors.append(f"mtd_pnl 不一致: {nav_record.mtd_pnl} != {expected_mtd_pnl}")

        expected_ytd_pnl = cls.calc_ytd_pnl(nav_record.total_value, prev_year_end_nav, yearly_cash_flow)
        if expected_ytd_pnl is not None:
            expected_ytd_pnl = float(cls.quantize_money(expected_ytd_pnl))
        if not cls.money_equal(nav_record.ytd_pnl, expected_ytd_pnl):
            errors.append(f"ytd_pnl 不一致: {nav_record.ytd_pnl} != {expected_ytd_pnl}")

        if initial_value is not None and nav_record.details is not None:
            expected_cum_pnl = float(
                cls.quantize_money(
                    cls.to_decimal(nav_record.total_value)
                    - cls.to_decimal(initial_value)
                    - cls.to_decimal(cumulative_cash_flow)
                )
            )
            stored_cum_pnl = nav_record.details.get("cumulative_appreciation")
            if stored_cum_pnl is not None and not cls.money_equal(stored_cum_pnl, expected_cum_pnl):
                errors.append(f"details.cumulative_appreciation 不一致: {stored_cum_pnl} != {expected_cum_pnl}")

        if errors:
            raise ValueError("NAV 记录自校验失败: " + " | ".join(errors))

    @classmethod
    def build_nav_record(
        cls,
        *,
        today,
        account,
        valuation,
        stock_value,
        cash_value,
        total_value,
        stock_ratio,
        cash_ratio,
        daily_cash_flow,
        monthly_cash_flow,
        yearly_cash_flow,
        yearly_data,
        cumulative_cash_flow,
        start_year,
        shares,
        shares_change,
        nav,
        month_nav_change,
        year_nav_change,
        cumulative_nav_change,
        daily_appreciation,
        month_appreciation,
        year_appreciation,
        cumulative_appreciation,
        initial_value,
        first_year_data,
        cagr=0.0,
    ) -> NAVHistory:
        details = {
            "monthly_cash_flow": float(cls.quantize_money(monthly_cash_flow)),
            "year_cash_flow": float(cls.quantize_money(yearly_cash_flow)),
            "cumulative_nav_change": float(cls.quantize_nav(cumulative_nav_change)),
            "cumulative_appreciation": float(cls.quantize_money(cumulative_appreciation)),
            "initial_value": float(cls.quantize_money(initial_value)) if initial_value is not None else None,
            "cumulative_cash_flow": float(cls.quantize_money(cumulative_cash_flow)),
            "cagr": float(cls.quantize_nav(cagr)),
            "cagr_pct": float(cls.quantize_money(cagr * 100)),
        }
        for yr_str, yd in yearly_data.items():
            nav_change = yd.get("nav_change")
            appreciation = yd.get("appreciation")
            details[f"nav_change_{yr_str}"] = float(cls.quantize_nav(nav_change)) if nav_change is not None else None
            details[f"appreciation_{yr_str}"] = float(cls.quantize_money(appreciation)) if appreciation is not None else None
            details[f"cash_flow_{yr_str}"] = float(cls.quantize_money(yd.get("cash_flow", 0)))

        return NAVHistory(
            date=today,
            account=account,
            total_value=float(cls.quantize_money(total_value)),
            cash_value=float(cls.quantize_money(cash_value)),
            stock_value=float(cls.quantize_money(stock_value)),
            fund_value=float(cls.quantize_money(valuation.fund_value_cny)),
            cn_stock_value=float(cls.quantize_money(valuation.cn_asset_value)),
            us_stock_value=float(cls.quantize_money(valuation.us_asset_value)),
            hk_stock_value=float(cls.quantize_money(valuation.hk_asset_value)),
            stock_weight=float(cls.quantize_weight(stock_ratio)),
            cash_weight=float(cls.quantize_weight(cash_ratio)),
            shares=float(cls.quantize_money(shares)),
            nav=float(cls.quantize_nav(nav)),
            cash_flow=float(cls.quantize_money(daily_cash_flow)),
            share_change=float(cls.quantize_money(shares_change)),
            mtd_nav_change=float(cls.quantize_nav(month_nav_change)) if month_nav_change is not None else None,
            ytd_nav_change=float(cls.quantize_nav(year_nav_change)) if year_nav_change is not None else None,
            pnl=float(cls.quantize_money(daily_appreciation)) if daily_appreciation is not None else None,
            mtd_pnl=float(cls.quantize_money(month_appreciation)) if month_appreciation is not None else None,
            ytd_pnl=float(cls.quantize_money(year_appreciation)) if year_appreciation is not None else None,
            details=details,
        )
