"""NAV summary output formatter."""
from __future__ import annotations


class NavSummaryPrinter:
    def print_summary(
        self,
        *,
        today,
        stock_value,
        cash_value,
        total_value,
        stock_ratio,
        cash_ratio,
        current_year,
        start_year,
        yesterday_nav,
        prev_year_end_nav,
        prev_month_end_nav,
        yearly_data,
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
        cumulative_cash_flow=0,
        daily_cash_flow=0,
        monthly_cash_flow=0,
        cagr=0.0,
        **_extra,
    ) -> None:
        print(f"\n净值记录已保存 ({today}):")
        print(f"  股票市值: ¥{stock_value:,.2f} ({stock_ratio*100:.2f}%)")
        print(f"  现金结余: ¥{cash_value:,.2f} ({cash_ratio*100:.2f}%)")
        print(f"  账户净值: ¥{total_value:,.2f}")
        print(f"  总份额: {shares:,.2f}")
        print(f"  单位净值: {nav:.4f}")
        print(f"  当日资金变动: ¥{daily_cash_flow:,.2f}")
        print(f"  份额变动: {shares_change:,.2f}")
        if prev_month_end_nav:
            print(f"  当月净值涨幅: {month_nav_change*100:.2f}%")
        if prev_year_end_nav:
            print(f"  当年({current_year})净值涨幅: {year_nav_change*100:.2f}%")
        for year, data in sorted(yearly_data.items()):
            if data["prev_end"] and data["end"]:
                print(f"  {year}年净值涨幅: {data['nav_change']*100:.2f}%")
        if first_year_data and first_year_data["prev_end"]:
            print(f"  累计净值涨幅({start_year}起): {cumulative_nav_change*100:.2f}%")
            if cagr != 0.0:
                print(f"  成立以来年化收益(CAGR): {cagr*100:.2f}%")
        if initial_value:
            print(
                f"  累计资产升值: ¥{cumulative_appreciation:,.2f} "
                f"({total_value:,.0f} - {initial_value:,.0f} - {cumulative_cash_flow:,.0f})"
            )
