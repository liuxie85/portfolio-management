"""NAV record orchestration service."""
from __future__ import annotations

from datetime import date
from typing import Any, Optional

from src import config
from src.models import NAVHistory, PortfolioValuation
from src.time_utils import bj_today


class NavRecordService:
    """Coordinate NAV calculation, snapshot persistence, validation, and storage.

    ``manager`` remains the compatibility facade for helper methods that are
    still patched directly by existing tests and callers.
    """

    def __init__(self, manager: Any, storage: Any):
        self.manager = manager
        self.storage = storage

    def record_nav(
        self,
        account: str,
        valuation: Optional[PortfolioValuation] = None,
        nav_date: Optional[date] = None,
        persist: bool = True,
        overwrite_existing: bool = True,
        dry_run: bool = False,
        use_bulk_persist: bool = False,
    ) -> NAVHistory:
        if valuation is None:
            valuation = self.manager.calculate_valuation(account)

        today = nav_date or bj_today()
        current_year = today.strftime("%Y")
        start_year = config.get_start_year()

        stock_value = valuation.stock_value_cny + valuation.fund_value_cny
        cash_value = valuation.cash_value_cny
        total_value = stock_value + cash_value
        stock_ratio = stock_value / total_value if total_value > 0 else 0
        cash_ratio = cash_value / total_value if total_value > 0 else 0

        self.storage.preload_nav_index(account)
        nav_idx_payload = self.storage.get_nav_index(account)
        all_navs = list(nav_idx_payload.get("_nav_objects") or [])
        nav_index = self.manager._build_nav_lookup(all_navs)

        yesterday_nav = self.manager._find_latest_nav_before(all_navs, today, nav_index=nav_index)
        prev_year_end_nav = self.manager._find_year_end_nav(all_navs, str(today.year - 1), nav_index=nav_index)
        prev_month_end_nav = self.manager._find_prev_month_end_nav(all_navs, today.year, today.month, nav_index=nav_index)
        last_nav = yesterday_nav

        yearly_data = {}
        for yr in range(start_year, today.year + 1):
            yr_str = str(yr)
            yearly_data[yr_str] = {
                "prev_end": self.manager._find_year_end_nav(all_navs, str(yr - 1), nav_index=nav_index),
                "end": self.manager._find_year_end_nav(all_navs, yr_str, nav_index=nav_index),
            }

        cash_flow_summary = self.manager._summarize_cash_flows(
            account=account,
            today=today,
            start_year=start_year,
            last_nav=last_nav,
        )
        daily_cash_flow = cash_flow_summary["daily"]
        monthly_cash_flow = cash_flow_summary["monthly"]
        yearly_cash_flow = cash_flow_summary["yearly"].get(current_year, 0.0)
        for yr_str, yd in yearly_data.items():
            yd["cash_flow"] = cash_flow_summary["yearly"].get(yr_str, 0.0)
        cumulative_cash_flow = cash_flow_summary["cumulative"]
        gap_cash_flow = cash_flow_summary["gap"]

        calc = self.manager._calc_nav_metrics(
            account=account,
            today=today,
            total_value=total_value,
            yesterday_nav=yesterday_nav,
            prev_year_end_nav=prev_year_end_nav,
            prev_month_end_nav=prev_month_end_nav,
            last_nav=last_nav,
            yearly_data=yearly_data,
            daily_cash_flow=daily_cash_flow,
            monthly_cash_flow=monthly_cash_flow,
            yearly_cash_flow=yearly_cash_flow,
            cumulative_cash_flow=cumulative_cash_flow,
            start_year=start_year,
            gap_cash_flow=gap_cash_flow,
            all_navs=all_navs,
        )

        nav_record = self.manager._build_nav_record(
            today=today,
            account=account,
            valuation=valuation,
            stock_value=stock_value,
            cash_value=cash_value,
            total_value=total_value,
            stock_ratio=stock_ratio,
            cash_ratio=cash_ratio,
            daily_cash_flow=daily_cash_flow,
            monthly_cash_flow=monthly_cash_flow,
            yearly_cash_flow=yearly_cash_flow,
            yearly_data=yearly_data,
            cumulative_cash_flow=cumulative_cash_flow,
            start_year=start_year,
            **calc,
        )

        if not bool(config.get("nav.disable_runtime_validation", False)):
            self.manager._validate_nav_record(
                nav_record=nav_record,
                last_nav=last_nav,
                prev_month_end_nav=prev_month_end_nav,
                prev_year_end_nav=prev_year_end_nav,
                daily_cash_flow=daily_cash_flow,
                monthly_cash_flow=monthly_cash_flow,
                yearly_cash_flow=yearly_cash_flow,
                gap_cash_flow=gap_cash_flow,
                initial_value=calc.get("initial_value"),
                cumulative_cash_flow=cumulative_cash_flow,
            )

        if persist:
            if use_bulk_persist and (not dry_run) and overwrite_existing:
                self.storage.upsert_nav_bulk([nav_record], mode="replace", allow_partial=False)
            else:
                self.storage.save_nav(nav_record, overwrite_existing=overwrite_existing, dry_run=dry_run)

        # Snapshot after NAV record to avoid orphaned snapshots on NAV write failure
        if persist:
            try:
                self.manager.snapshot_service.persist_holdings_snapshot(
                    account=account,
                    today=today,
                    valuation=valuation,
                    dry_run=dry_run,
                )
            except Exception as exc:
                import logging
                logging.getLogger(__name__).warning(
                    "holdings_snapshot write failed for %s (%s): %s — NAV record was saved successfully",
                    today, account, exc,
                )

        if persist and not dry_run:
            self.manager._print_nav_summary(
                today=today,
                stock_value=stock_value,
                cash_value=cash_value,
                total_value=total_value,
                stock_ratio=stock_ratio,
                cash_ratio=cash_ratio,
                current_year=current_year,
                start_year=start_year,
                yesterday_nav=yesterday_nav,
                prev_year_end_nav=prev_year_end_nav,
                prev_month_end_nav=prev_month_end_nav,
                yearly_data=yearly_data,
                daily_cash_flow=daily_cash_flow,
                cumulative_cash_flow=cumulative_cash_flow,
                **calc,
            )

        return nav_record
