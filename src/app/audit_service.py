"""NAV history audit, reconciliation, and repair service.

Extracted from skill_api.PortfolioSkill to keep the Skill class lean.
"""
from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

from src.time_utils import bj_today, bj_now_naive


class AuditService:
    """Audit and repair nav_history derived fields."""

    def __init__(self, *, storage: Any, portfolio: Any, account: str, report_dir: Path, api: Any = None):
        self.storage = storage
        self.portfolio = portfolio
        self.account = account
        self.report_dir = report_dir
        self.api = api

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------

    def _write_report(self, result: dict, prefix: str, account: str) -> str:
        self.report_dir.mkdir(parents=True, exist_ok=True)
        stamp = bj_now_naive().strftime('%Y%m%d_%H%M%S')
        out = self.report_dir / f'{prefix}_{account}_{stamp}.json'
        out.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding='utf-8')
        return str(out)

    @staticmethod
    def _neq(a, b, kind: str, portfolio) -> bool:
        if a is None or b is None:
            return (a is not None and b is None) or (a is None and b is not None)
        if kind == 'nav':
            return not portfolio._nav_equal(a, b)
        return not portfolio._money_equal(a, b)

    # ------------------------------------------------------------------
    # metrics audit
    # ------------------------------------------------------------------

    def audit_nav_history_metrics(self, account: Optional[str] = None, days: int = 900, write_report: bool = True) -> Dict[str, Any]:
        """审计 nav_history 四个核心派生字段，与当前代码公式逐条比对。"""
        audit_account = account or self.account
        all_navs = sorted(self.storage.get_nav_history(audit_account, days=9999), key=lambda n: n.date)
        if days and days > 0:
            cutoff = bj_today() - timedelta(days=days)
            target_navs = [n for n in all_navs if n.date >= cutoff]
        else:
            target_navs = list(all_navs)

        rows: List[Dict[str, Any]] = []
        nav_index = self.portfolio._build_nav_lookup(all_navs)
        for n in target_navs:
            pm = self.portfolio._find_prev_month_end_nav(all_navs, n.date.year, n.date.month, nav_index=nav_index)
            py = self.portfolio._find_year_end_nav(all_navs, str(n.date.year - 1), nav_index=nav_index)
            monthly_cf = self.portfolio._get_monthly_cash_flow(audit_account, n.date.year, n.date.month) if pm else None
            yearly_cf = self.portfolio._get_yearly_cash_flow(audit_account, str(n.date.year)) if py else None
            raw_mtd_nav_change = self.portfolio._calc_mtd_nav_change(n.nav, pm) if (n.nav is not None and pm) else None
            raw_ytd_nav_change = self.portfolio._calc_ytd_nav_change(n.nav, py) if (n.nav is not None and py) else None
            raw_mtd_pnl = self.portfolio._calc_mtd_pnl(n.total_value, pm, monthly_cf) if (n.total_value is not None and pm is not None and monthly_cf is not None) else None
            raw_ytd_pnl = self.portfolio._calc_ytd_pnl(n.total_value, py, yearly_cf) if (n.total_value is not None and py is not None and yearly_cf is not None) else None
            recomputed_mtd_nav_change = round(raw_mtd_nav_change, 6) if raw_mtd_nav_change is not None else None
            recomputed_ytd_nav_change = round(raw_ytd_nav_change, 6) if raw_ytd_nav_change is not None else None
            recomputed_mtd_pnl = round(raw_mtd_pnl, 2) if raw_mtd_pnl is not None else None
            recomputed_ytd_pnl = round(raw_ytd_pnl, 2) if raw_ytd_pnl is not None else None

            is_initial_without_month_base = (pm is None)
            is_january_same_period_return = (
                n.date.month == 1
                and recomputed_mtd_nav_change is not None
                and recomputed_ytd_nav_change is not None
                and recomputed_mtd_nav_change == recomputed_ytd_nav_change
            )
            rows.append({
                "record_id": n.record_id,
                "date": n.date.isoformat(),
                "pm_base_date": pm.date.isoformat() if pm else None,
                "py_base_date": py.date.isoformat() if py else None,
                "stored_mtd_nav_change": n.mtd_nav_change,
                "recomputed_mtd_nav_change": recomputed_mtd_nav_change,
                "stored_ytd_nav_change": n.ytd_nav_change,
                "recomputed_ytd_nav_change": recomputed_ytd_nav_change,
                "stored_mtd_pnl": n.mtd_pnl,
                "recomputed_mtd_pnl": recomputed_mtd_pnl,
                "stored_ytd_pnl": n.ytd_pnl,
                "recomputed_ytd_pnl": recomputed_ytd_pnl,
                "base_missing": {"month": pm is None, "year": py is None},
                "audit_exemptions": {
                    "initial_without_month_base": is_initial_without_month_base,
                    "january_mtd_equals_ytd": is_january_same_period_return,
                },
            })

        def _neq_mtd_nav(r):
            if r.get("audit_exemptions", {}).get("initial_without_month_base"):
                return False
            return self._neq(r["stored_mtd_nav_change"], r["recomputed_mtd_nav_change"], 'nav', self.portfolio)

        sign_flip_mtd = [r["date"] for r in rows if r["stored_mtd_pnl"] is not None and r["recomputed_mtd_pnl"] is not None and r["stored_mtd_pnl"] * r["recomputed_mtd_pnl"] < 0]
        sign_flip_ytd = [r["date"] for r in rows if r["stored_ytd_pnl"] is not None and r["recomputed_ytd_pnl"] is not None and r["stored_ytd_pnl"] * r["recomputed_ytd_pnl"] < 0]
        swapped_dates = [
            r["date"] for r in rows
            if r["stored_mtd_nav_change"] == r["recomputed_ytd_nav_change"]
            and r["stored_ytd_nav_change"] == r["recomputed_mtd_nav_change"]
            and not r.get("audit_exemptions", {}).get("january_mtd_equals_ytd")
        ]
        summary = {
            "mtd_nav_change_mismatch": sum(1 for r in rows if _neq_mtd_nav(r)),
            "ytd_nav_change_mismatch": sum(1 for r in rows if self._neq(r["stored_ytd_nav_change"], r["recomputed_ytd_nav_change"], 'nav', self.portfolio)),
            "mtd_pnl_mismatch": sum(1 for r in rows if self._neq(r["stored_mtd_pnl"], r["recomputed_mtd_pnl"], 'money', self.portfolio)),
            "ytd_pnl_mismatch": sum(1 for r in rows if self._neq(r["stored_ytd_pnl"], r["recomputed_ytd_pnl"], 'money', self.portfolio)),
            "base_missing_month": sum(1 for r in rows if r.get("base_missing", {}).get("month")),
            "base_missing_year": sum(1 for r in rows if r.get("base_missing", {}).get("year")),
            "sign_flip_mtd_pnl": len(sign_flip_mtd),
            "sign_flip_ytd_pnl": len(sign_flip_ytd),
            "swapped_nav_change_like": len(swapped_dates),
            "sign_flip_mtd_pnl_dates": sign_flip_mtd,
            "sign_flip_ytd_pnl_dates": sign_flip_ytd,
            "swapped_nav_change_dates": swapped_dates,
        }
        result = {"success": True, "account": audit_account, "count": len(rows), "summary": summary, "rows": rows}
        if write_report:
            result['report_file'] = self._write_report(result, 'nav_history_audit', audit_account)
        return result

    # ------------------------------------------------------------------
    # reconcile audit
    # ------------------------------------------------------------------

    def audit_nav_history_reconcile(self, account: Optional[str] = None, days: int = 900, write_report: bool = True) -> Dict[str, Any]:
        """按日期顺序对 nav_history 做历史对账，输出 ok / exempt / anomaly。"""
        audit_account = account or self.account
        all_navs = sorted(self.storage.get_nav_history(audit_account, days=9999), key=lambda n: n.date)
        nav_index = self.portfolio._build_nav_lookup(all_navs)
        if days and days > 0:
            cutoff = bj_today() - timedelta(days=days)
            target_navs = [n for n in all_navs if n.date >= cutoff]
        else:
            target_navs = list(all_navs)

        rows: List[Dict[str, Any]] = []
        for n in target_navs:
            prev_nav = self.portfolio._find_latest_nav_before(all_navs, n.date, nav_index=nav_index)
            pm = self.portfolio._find_prev_month_end_nav(all_navs, n.date.year, n.date.month, nav_index=nav_index)
            py = self.portfolio._find_year_end_nav(all_navs, str(n.date.year - 1), nav_index=nav_index)
            daily_cf = self.portfolio._get_daily_cash_flow(audit_account, n.date)
            monthly_cf = self.portfolio._get_monthly_cash_flow(audit_account, n.date.year, n.date.month) if pm else None
            yearly_cf = self.portfolio._get_yearly_cash_flow(audit_account, str(n.date.year)) if py else None

            anomalies: List[str] = []
            exemptions: List[str] = []

            expected_total = float(self.portfolio._quantize_money((n.stock_value or 0.0) + (n.cash_value or 0.0)))
            if not self.portfolio._money_equal(n.total_value, expected_total):
                anomalies.append(f"total_value != stock_value + cash_value ({n.total_value} != {expected_total})")

            if n.total_value and n.total_value > 0 and n.stock_weight is not None and n.cash_weight is not None:
                weights_sum = n.stock_weight + n.cash_weight
                if not self.portfolio._approx_equal(weights_sum, 1.0, tolerance=1e-4):
                    anomalies.append(f"stock_weight + cash_weight != 1 ({weights_sum})")

            if n.shares and n.shares > 0 and n.nav is not None:
                expected_nav = float(self.portfolio._quantize_nav(self.portfolio._to_decimal(n.total_value) / self.portfolio._to_decimal(n.shares)))
                if not self.portfolio._nav_equal(n.nav, expected_nav):
                    legacy_expected_nav_4dp = float(round(expected_nav, 4))
                    if self.portfolio._approx_equal(n.nav, legacy_expected_nav_4dp, tolerance=1e-6):
                        exemptions.append('legacy_nav_precision_4dp')
                    else:
                        anomalies.append(f"nav != total_value / shares ({n.nav} != {expected_nav})")

            raw_mtd = self.portfolio._calc_mtd_nav_change(n.nav, pm) if (n.nav is not None and pm) else None
            raw_ytd = self.portfolio._calc_ytd_nav_change(n.nav, py) if (n.nav is not None and py) else None
            raw_mtd_pnl = self.portfolio._calc_mtd_pnl(n.total_value, pm, monthly_cf) if (n.total_value is not None and pm is not None and monthly_cf is not None) else None
            raw_ytd_pnl = self.portfolio._calc_ytd_pnl(n.total_value, py, yearly_cf) if (n.total_value is not None and py is not None and yearly_cf is not None) else None

            recomputed_mtd = round(raw_mtd, 6) if raw_mtd is not None else None
            recomputed_ytd = round(raw_ytd, 6) if raw_ytd is not None else None
            recomputed_mtd_pnl = round(raw_mtd_pnl, 2) if raw_mtd_pnl is not None else None
            recomputed_ytd_pnl = round(raw_ytd_pnl, 2) if raw_ytd_pnl is not None else None

            if pm is None:
                exemptions.append('missing_month_base')
            if py is None:
                exemptions.append('missing_year_base')
            if n.date.month == 1 and recomputed_mtd is not None and recomputed_ytd is not None and recomputed_mtd == recomputed_ytd:
                exemptions.append('january_mtd_equals_ytd')

            if not self.portfolio._nav_equal(n.mtd_nav_change, recomputed_mtd) and 'missing_month_base' not in exemptions:
                anomalies.append(f"mtd_nav_change mismatch ({n.mtd_nav_change} != {recomputed_mtd})")
            if not self.portfolio._nav_equal(n.ytd_nav_change, recomputed_ytd) and 'missing_year_base' not in exemptions:
                anomalies.append(f"ytd_nav_change mismatch ({n.ytd_nav_change} != {recomputed_ytd})")
            if not self.portfolio._money_equal(n.mtd_pnl, recomputed_mtd_pnl) and 'missing_month_base' not in exemptions:
                anomalies.append(f"mtd_pnl mismatch ({n.mtd_pnl} != {recomputed_mtd_pnl})")
            if not self.portfolio._money_equal(n.ytd_pnl, recomputed_ytd_pnl) and 'missing_year_base' not in exemptions:
                anomalies.append(f"ytd_pnl mismatch ({n.ytd_pnl} != {recomputed_ytd_pnl})")

            if prev_nav and prev_nav.date and (n.date - prev_nav.date).days == 1 and n.pnl is not None:
                expected_daily_pnl = float(self.portfolio._quantize_money(
                    self.portfolio._to_decimal(n.total_value) - self.portfolio._to_decimal(prev_nav.total_value) - self.portfolio._to_decimal(daily_cf)
                ))
                if not self.portfolio._money_equal(n.pnl, expected_daily_pnl):
                    anomalies.append(f"pnl mismatch ({n.pnl} != {expected_daily_pnl})")
            elif n.pnl is not None and (not prev_nav or (n.date - prev_nav.date).days != 1):
                exemptions.append('non_consecutive_daily_pnl')

            if prev_nav and prev_nav.shares is not None and self.portfolio._approx_equal(daily_cf, 0.0, tolerance=0.01):
                if (n.date - prev_nav.date).days == 1:
                    if not self.portfolio._approx_equal(n.shares, prev_nav.shares, tolerance=0.01):
                        anomalies.append(f"shares changed without daily cash flow ({n.shares} != {prev_nav.shares})")
                elif not self.portfolio._approx_equal(n.shares, prev_nav.shares, tolerance=0.01):
                    exemptions.append('non_consecutive_share_change')

            status = 'anomaly' if anomalies else ('exempt' if exemptions else 'ok')
            rows.append({
                'record_id': n.record_id,
                'date': n.date.isoformat(),
                'status': status,
                'anomalies': anomalies,
                'exemptions': exemptions,
                'basis': {
                    'prev_nav_date': prev_nav.date.isoformat() if prev_nav else None,
                    'prev_nav_total_value': prev_nav.total_value if prev_nav else None,
                    'prev_nav_shares': prev_nav.shares if prev_nav else None,
                    'prev_month_end_date': pm.date.isoformat() if pm else None,
                    'prev_month_end_nav': pm.nav if pm else None,
                    'prev_month_end_total_value': pm.total_value if pm else None,
                    'prev_year_end_date': py.date.isoformat() if py else None,
                    'prev_year_end_nav': py.nav if py else None,
                    'prev_year_end_total_value': py.total_value if py else None,
                },
                'cash_flow_basis': {
                    'daily_cash_flow': daily_cf,
                    'monthly_cash_flow': monthly_cf,
                    'yearly_cash_flow': yearly_cf,
                },
                'stored': {
                    'total_value': n.total_value,
                    'cash_value': n.cash_value,
                    'stock_value': n.stock_value,
                    'nav': n.nav,
                    'shares': n.shares,
                    'pnl': n.pnl,
                    'mtd_nav_change': n.mtd_nav_change,
                    'ytd_nav_change': n.ytd_nav_change,
                    'mtd_pnl': n.mtd_pnl,
                    'ytd_pnl': n.ytd_pnl,
                },
                'recomputed': {
                    'daily_cash_flow': daily_cf,
                    'mtd_nav_change': recomputed_mtd,
                    'ytd_nav_change': recomputed_ytd,
                    'mtd_pnl': recomputed_mtd_pnl,
                    'ytd_pnl': recomputed_ytd_pnl,
                    'expected_daily_pnl': round(n.total_value - prev_nav.total_value - daily_cf, 2) if (prev_nav and prev_nav.date and (n.date - prev_nav.date).days == 1) else None,
                },
            })

        anomaly_rows = [r for r in rows if r['status'] == 'anomaly']
        summary = {
            'ok': sum(1 for r in rows if r['status'] == 'ok'),
            'exempt': sum(1 for r in rows if r['status'] == 'exempt'),
            'anomaly': len(anomaly_rows),
            'anomaly_dates': [r['date'] for r in anomaly_rows],
            'anomaly_examples': [
                {
                    'date': r['date'],
                    'anomalies': r['anomalies'],
                    'basis': r['basis'],
                }
                for r in anomaly_rows[:10]
            ],
        }
        result = {'success': True, 'account': audit_account, 'count': len(rows), 'summary': summary, 'rows': rows}
        if write_report:
            result['report_file'] = self._write_report(result, 'nav_history_reconcile', audit_account)
        return result

    # ------------------------------------------------------------------
    # accuracy audit (unified entry)
    # ------------------------------------------------------------------

    def audit_nav_history_accuracy(self, account: Optional[str] = None, days: int = 900, write_report: bool = True) -> Dict[str, Any]:
        """统一准确性审计入口：汇总 metrics / reconcile / repair candidates。"""
        audit_account = account or self.account
        metrics_func = getattr(self.api, 'audit_nav_history_metrics', None) if self.api is not None else None
        reconcile_func = getattr(self.api, 'audit_nav_history_reconcile', None) if self.api is not None else None
        metrics = (
            metrics_func(account=audit_account, days=days, write_report=False)
            if callable(metrics_func)
            else self.audit_nav_history_metrics(account=audit_account, days=days, write_report=False)
        )
        if not metrics.get('success'):
            return metrics
        reconcile = (
            reconcile_func(account=audit_account, days=days, write_report=False)
            if callable(reconcile_func)
            else self.audit_nav_history_reconcile(account=audit_account, days=days, write_report=False)
        )
        if not reconcile.get('success'):
            return reconcile

        reconcile_by_date = {row['date']: row for row in reconcile.get('rows', [])}
        repair_candidates: List[Dict[str, Any]] = []
        exempt_rows: List[Dict[str, Any]] = []
        ok_rows: List[Dict[str, Any]] = []
        for row in metrics.get('rows', []):
            rec = reconcile_by_date.get(row['date'], {})
            status = rec.get('status', 'unknown')
            item = {
                'record_id': row['record_id'],
                'date': row['date'],
                'status': status,
                'base_missing': row.get('base_missing'),
                'audit_exemptions': row.get('audit_exemptions'),
                'anomalies': rec.get('anomalies', []),
                'exemptions': rec.get('exemptions', []),
                'expected_daily_pnl': ((rec.get('recomputed') or {}).get('expected_daily_pnl')),
            }
            if status == 'anomaly':
                repair_candidates.append(item)
            elif status == 'exempt':
                exempt_rows.append(item)
            else:
                ok_rows.append(item)

        summary = {
            'metrics': metrics.get('summary', {}),
            'reconcile': reconcile.get('summary', {}),
            'repair_candidates': len(repair_candidates),
            'exempt_rows': len(exempt_rows),
            'ok_rows': len(ok_rows),
        }
        result = {
            'success': True,
            'account': audit_account,
            'days': days,
            'summary': summary,
            'metrics': metrics,
            'reconcile': reconcile,
            'repair_candidates': repair_candidates,
            'exempt_rows': exempt_rows,
            'ok_rows': ok_rows,
        }
        if write_report:
            result['report_file'] = self._write_report(result, 'nav_history_accuracy', audit_account)
        return result

    # ------------------------------------------------------------------
    # repair
    # ------------------------------------------------------------------

    def repair_nav_history_metrics(self, account: Optional[str] = None, days: int = 900, dry_run: bool = True, write_report: bool = True) -> Dict[str, Any]:
        """按统一准确性审计结果修复 nav_history 派生字段；仅修复真正 anomaly，默认 dry_run。"""
        accuracy_func = getattr(self.api, 'audit_nav_history_accuracy', None) if self.api is not None else None
        accuracy = (
            accuracy_func(account=account, days=days, write_report=False)
            if callable(accuracy_func)
            else self.audit_nav_history_accuracy(account=account, days=days, write_report=False)
        )
        if not accuracy.get("success"):
            return accuracy

        metrics_rows = {row['date']: row for row in accuracy.get('metrics', {}).get('rows', [])}
        repair_candidates = accuracy.get('repair_candidates', [])
        exempt_rows = accuracy.get('exempt_rows', [])
        ok_rows = accuracy.get('ok_rows', [])

        updates: List[Dict[str, Any]] = []
        skipped: List[Dict[str, Any]] = []

        for item in repair_candidates:
            row = metrics_rows.get(item['date'])
            if not row:
                skipped.append({
                    'record_id': item.get('record_id'),
                    'date': item.get('date'),
                    'reason': 'missing_metrics_row',
                    'exemptions': item.get('exemptions'),
                })
                continue

            fields: Dict[str, Any] = {}
            expected_daily_pnl = item.get('expected_daily_pnl')

            if 'non_consecutive_daily_pnl' not in (item.get('exemptions') or []):
                fields['pnl'] = expected_daily_pnl

            if row.get('base_missing', {}).get('month'):
                fields['mtd_nav_change'] = None
                fields['mtd_pnl'] = None
            else:
                fields['mtd_nav_change'] = row.get('recomputed_mtd_nav_change')
                fields['mtd_pnl'] = row.get('recomputed_mtd_pnl')

            if row.get('base_missing', {}).get('year'):
                fields['ytd_nav_change'] = None
                fields['ytd_pnl'] = None
            else:
                fields['ytd_nav_change'] = row.get('recomputed_ytd_nav_change')
                fields['ytd_pnl'] = row.get('recomputed_ytd_pnl')

            updates.append({
                'record_id': row.get('record_id'),
                'date': row.get('date'),
                'fields': fields,
                'base_missing': row.get('base_missing'),
                'reconcile_status': item.get('status'),
                'anomalies': item.get('anomalies'),
                'exemptions': item.get('exemptions'),
            })

        for item in exempt_rows + ok_rows:
            skipped.append({
                'record_id': item.get('record_id'),
                'date': item.get('date'),
                'reason': f"status={item.get('status')}",
                'exemptions': item.get('exemptions'),
            })

        if not dry_run:
            for item in updates:
                self.storage.patch_nav_derived_fields(
                    item['record_id'],
                    item['fields'],
                    dry_run=False,
                )

        repair_account = account or self.account
        result = {
            'success': True,
            'dry_run': dry_run,
            'account': repair_account,
            'count': len(updates),
            'summary': accuracy.get('summary'),
            'repair_policy': 'anomaly_only_via_accuracy_audit',
            'updates': updates,
            'skipped': skipped,
            'skipped_count': len(skipped),
            'accuracy_report': {
                'repair_candidates': len(repair_candidates),
                'exempt_rows': len(exempt_rows),
                'ok_rows': len(ok_rows),
            },
        }
        if write_report:
            suffix = 'dryrun' if dry_run else 'applied'
            result['report_file'] = self._write_report(result, f'nav_history_repair_{suffix}', repair_account)
        return result
