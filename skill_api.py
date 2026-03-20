#!/usr/bin/env python3
"""
Portfolio Management Skill API
投资组合管理 Skill 统一入口

基于飞书多维表作为数据存储，支持多端同步
"""
import sys
import json
from pathlib import Path
from datetime import date, datetime, timedelta
from typing import Dict, Any, Optional

# 确保能 import 到 src 模块
SKILL_DIR = Path(__file__).parent.resolve()
sys.path.insert(0, str(SKILL_DIR))

from src.feishu_storage import FeishuStorage, FeishuClient
from src.portfolio import PortfolioManager
from src.price_fetcher import PriceFetcher
from src.storage import create_storage
from src.reporting_utils import normalize_asset_type, normalization_warning
from src.models import AssetType, AssetClass, Industry, Holding, NAVHistory
from src.asset_utils import (
    validate_code as validate_asset_code,
    detect_asset_type,
    parse_date,
)
from src import config


# ========== 配置 ==========

DEFAULT_ACCOUNT = config.get_account()


# ========== 核心 API 类 ==========

class PortfolioSkill:
    """投资组合管理 Skill 核心类"""

    def build_snapshot(self) -> Dict[str, Any]:
        """构建统一估值快照，供 full_report / record_nav 复用，避免时点差。"""
        valuation = self.portfolio.calculate_valuation(self.account)
        holdings = valuation.holdings or []
        holdings_list = []
        for h in holdings:
            holdings_list.append({
                "code": h.asset_id,
                "name": h.asset_name,
                "quantity": h.quantity,
                "type": h.asset_type.value if h.asset_type else None,
                "normalized_type": normalize_asset_type(h.asset_type, h.asset_id),
                "market": h.market,
                "currency": h.currency,
                "price": h.current_price,
                "cny_price": h.cny_price,
                "market_value": h.market_value_cny,
                "weight": h.weight,
            })
        holdings_list.sort(key=lambda x: x.get("market_value") or 0, reverse=True)

        return {
            "snapshot_time": datetime.now().isoformat(),
            "valuation": valuation,
            "holdings_data": {
                "success": True,
                "holdings": holdings_list,
                "count": len(holdings_list),
                "total_value": valuation.total_value_cny,
                "cash_value": valuation.cash_value_cny,
                "stock_value": valuation.stock_value_cny + valuation.fund_value_cny,
                "cash_ratio": valuation.cash_ratio,
                "warnings": valuation.warnings,
            },
            "position_data": {
                "cash_ratio": valuation.cash_ratio,
                "stock_ratio": valuation.stock_ratio,
                "fund_ratio": valuation.fund_ratio,
            }
        }

    # backward compatibility
    def _build_snapshot(self) -> Dict[str, Any]:
        return self.build_snapshot()

    def audit_nav_history_metrics(self, account: Optional[str] = None, days: int = 900, write_report: bool = True) -> Dict[str, Any]:
        """审计 nav_history 四个核心派生字段，与当前代码公式逐条比对。

        注意：目标记录范围可由 days 限制，但月/年基准一律从全量历史中查找；
        若缺少基准，则重算结果返回 None，绝不偷补 0。
        """
        audit_account = account or self.account
        all_navs = sorted(self.storage.get_nav_history(audit_account, days=9999), key=lambda n: n.date)
        if days and days > 0:
            cutoff = date.today() - timedelta(days=days)
            target_navs = [n for n in all_navs if n.date >= cutoff]
        else:
            target_navs = list(all_navs)

        rows = []
        for n in target_navs:
            pm = self.portfolio._find_prev_month_end_nav(all_navs, n.date.year, n.date.month)
            py = self.portfolio._find_year_end_nav(all_navs, str(n.date.year - 1))
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

            # 审计去误报：
            # 1) 初始记录若缺少月基准，不视为异常
            # 2) 1 月份 mtd == ytd 属于正常，不视为 swapped
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
        def _neq(a, b, kind='money'):
            if a is None or b is None:
                return (a is not None and b is None) or (a is None and b is not None)
            if kind == 'nav':
                return not self.portfolio._nav_equal(a, b)
            return not self.portfolio._money_equal(a, b)

        def _neq_mtd_nav(r):
            if r.get("audit_exemptions", {}).get("initial_without_month_base"):
                return False
            return _neq(r["stored_mtd_nav_change"], r["recomputed_mtd_nav_change"], kind='nav')

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
            "ytd_nav_change_mismatch": sum(1 for r in rows if _neq(r["stored_ytd_nav_change"], r["recomputed_ytd_nav_change"], kind='nav')),
            "mtd_pnl_mismatch": sum(1 for r in rows if _neq(r["stored_mtd_pnl"], r["recomputed_mtd_pnl"], kind='money')),
            "ytd_pnl_mismatch": sum(1 for r in rows if _neq(r["stored_ytd_pnl"], r["recomputed_ytd_pnl"], kind='money')),
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
            audit_dir = SKILL_DIR / 'audit'
            audit_dir.mkdir(parents=True, exist_ok=True)
            stamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            out = audit_dir / f'nav_history_audit_{audit_account}_{stamp}.json'
            out.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding='utf-8')
            result['report_file'] = str(out)
        return result

    def audit_nav_history_reconcile(self, account: Optional[str] = None, days: int = 900, write_report: bool = True) -> Dict[str, Any]:
        """按日期顺序对 nav_history 做历史对账，输出 ok / exempt / anomaly。"""
        audit_account = account or self.account
        all_navs = sorted(self.storage.get_nav_history(audit_account, days=9999), key=lambda n: n.date)
        nav_index = self.portfolio._build_nav_lookup(all_navs)
        if days and days > 0:
            cutoff = date.today() - timedelta(days=days)
            target_navs = [n for n in all_navs if n.date >= cutoff]
        else:
            target_navs = list(all_navs)

        rows = []
        for n in target_navs:
            prev_nav = self.portfolio._find_latest_nav_before(all_navs, n.date, nav_index=nav_index)
            pm = self.portfolio._find_prev_month_end_nav(all_navs, n.date.year, n.date.month, nav_index=nav_index)
            py = self.portfolio._find_year_end_nav(all_navs, str(n.date.year - 1), nav_index=nav_index)
            daily_cf = self.portfolio._get_daily_cash_flow(audit_account, n.date)
            monthly_cf = self.portfolio._get_monthly_cash_flow(audit_account, n.date.year, n.date.month) if pm else None
            yearly_cf = self.portfolio._get_yearly_cash_flow(audit_account, str(n.date.year)) if py else None

            anomalies = []
            exemptions = []

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
                    # 兼容历史月度快照使用 4 位 NAV 精度的旧口径：
                    # 若存量值与 4 位四舍五入结果一致，则视为 legacy precision，不判 anomaly。
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
                # 仅在相邻日记录下，才用 daily cash flow 规则约束 shares 变化。
                # 对月度/阶段性快照，shares 变化可能来自期间累计资金流，不应直接判 anomaly。
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
            audit_dir = SKILL_DIR / 'audit'
            audit_dir.mkdir(parents=True, exist_ok=True)
            stamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            out = audit_dir / f'nav_history_reconcile_{audit_account}_{stamp}.json'
            out.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding='utf-8')
            result['report_file'] = str(out)
        return result

    def audit_nav_history_accuracy(self, account: Optional[str] = None, days: int = 900, write_report: bool = True) -> Dict[str, Any]:
        """统一准确性审计入口：汇总 metrics / reconcile / repair candidates。"""
        audit_account = account or self.account
        metrics = self.audit_nav_history_metrics(account=audit_account, days=days, write_report=False)
        if not metrics.get('success'):
            return metrics
        reconcile = self.audit_nav_history_reconcile(account=audit_account, days=days, write_report=False)
        if not reconcile.get('success'):
            return reconcile

        reconcile_by_date = {row['date']: row for row in reconcile.get('rows', [])}
        repair_candidates = []
        exempt_rows = []
        ok_rows = []
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
            audit_dir = SKILL_DIR / 'audit'
            audit_dir.mkdir(parents=True, exist_ok=True)
            stamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            out = audit_dir / f'nav_history_accuracy_{audit_account}_{stamp}.json'
            out.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding='utf-8')
            result['report_file'] = str(out)
        return result

    def repair_nav_history_metrics(self, account: Optional[str] = None, days: int = 900, dry_run: bool = True, write_report: bool = True) -> Dict[str, Any]:
        """按统一准确性审计结果修复 nav_history 派生字段；仅修复真正 anomaly，默认 dry_run。"""
        accuracy = self.audit_nav_history_accuracy(account=account, days=days, write_report=False)
        if not accuracy.get("success"):
            return accuracy

        metrics_rows = {row['date']: row for row in accuracy.get('metrics', {}).get('rows', [])}
        repair_candidates = accuracy.get('repair_candidates', [])
        exempt_rows = accuracy.get('exempt_rows', [])
        ok_rows = accuracy.get('ok_rows', [])

        updates = []
        skipped = []

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

            fields = {}

            # 缺基准时明确清空，不再保留历史伪 0
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
                self.storage.update_nav_fields(item['record_id'], item['fields'], dry_run=False)
        result = {
            'success': True,
            'dry_run': dry_run,
            'account': account or self.account,
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
            audit_dir = SKILL_DIR / 'audit'
            audit_dir.mkdir(parents=True, exist_ok=True)
            stamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            suffix = 'dryrun' if dry_run else 'applied'
            out = audit_dir / f'nav_history_repair_{account or self.account}_{suffix}_{stamp}.json'
            out.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding='utf-8')
            result['report_file'] = str(out)
        return result

    def __init__(self, account: str = DEFAULT_ACCOUNT, feishu_client: FeishuClient = None):
        """
        初始化 Skill

        Args:
            account: 账户标识，默认 "lx"
            feishu_client: 飞书客户端实例（可选，用于自定义配置）
        """
        self.account = account
        self.storage = FeishuStorage(feishu_client) if feishu_client else create_storage()
        self.portfolio = PortfolioManager(self.storage)
        self.price_fetcher = PriceFetcher(storage=self.storage)

    # ---------- 交易记录 ----------

    def buy(self, code: str, name: str, quantity: float, price: float,
            date_str: str = None, market: str = "平安证券", fee: float = 0,
            auto_deduct_cash: bool = False, request_id: str = None,
            skip_validation: bool = False) -> Dict[str, Any]:
        """
        记录买入交易

        Args:
            code: 资产代码（如 600519、AAPL）
            name: 资产名称
            quantity: 买入数量
            price: 买入价格
            date_str: 交易日期 (YYYY-MM-DD)，默认今天
            market: 券商/平台，默认 "平安证券"
            fee: 手续费
            auto_deduct_cash: 是否自动扣减现金，默认 False
            request_id: 请求唯一标识（用于幂等性控制）
            skip_validation: 是否跳过代码有效性校验（默认校验）

        Returns:
            {"success": bool, "transaction": dict, "message": str}
        """
        try:
            tx_date = parse_date(date_str)

            # 代码格式校验（不自动补齐，格式错误直接报错）
            validated_code = validate_asset_code(code)

            asset_type, currency, asset_class = detect_asset_type(validated_code)

            # 代码有效性校验（通过价格接口验证）
            if not skip_validation:
                price_data = self.price_fetcher.fetch(validated_code)
                if not price_data or 'error' in price_data or not price_data.get('price'):
                    return {
                        "success": False,
                        "error": f"代码 {validated_code} 无效或无法获取价格",
                        "message": f"代码 {validated_code} 无效或无法获取价格，请检查代码是否正确。如需强制记录，请设置 skip_validation=True"
                    }

            tx = self.portfolio.buy(
                tx_date=tx_date,
                asset_id=validated_code,
                asset_name=name,
                asset_type=asset_type,
                account=self.account,
                quantity=quantity,
                price=price,
                currency=currency,
                market=market,
                fee=fee,
                asset_class=asset_class,
                industry=Industry.OTHER,
                auto_deduct_cash=auto_deduct_cash,
                request_id=request_id
            )

            # 使用实际保存的完整名称（可能已从接口自动获取）
            saved_name = tx.asset_name or name
            return {
                "success": True,
                "transaction": {
                    "record_id": tx.record_id,
                    "date": tx.tx_date.isoformat(),
                    "type": tx.tx_type.value,
                    "code": tx.asset_id,
                    "name": saved_name,
                    "quantity": tx.quantity,
                    "price": tx.price,
                    "amount": tx.quantity * tx.price,
                    "fee": tx.fee,
                    "total_cost": tx.quantity * tx.price + tx.fee
                },
                "message": f"买入记录已保存: {saved_name} {quantity}股 @ ¥{price}"
            }
        except Exception as e:
            return {"success": False, "error": str(e), "message": f"记录失败: {e}"}

    def sell(self, code: str, quantity: float, price: float,
             date_str: str = None, market: str = None, fee: float = 0,
             auto_add_cash: bool = False, request_id: str = None) -> Dict[str, Any]:
        """
        记录卖出交易

        Args:
            code: 资产代码
            quantity: 卖出数量
            price: 卖出价格
            date_str: 交易日期 (YYYY-MM-DD)
            market: 券商/平台
            fee: 手续费
            auto_add_cash: 是否自动增加现金
            request_id: 请求唯一标识（用于幂等性控制）

        Returns:
            {"success": bool, "transaction": dict, "message": str}
        """
        try:
            tx_date = parse_date(date_str)

            # 代码格式校验（不自动补齐，格式错误直接报错）
            validated_code = validate_asset_code(code)

            # 获取持仓信息
            holding = self.storage.get_holding(validated_code, self.account, market)
            if not holding:
                return {
                    "success": False,
                    "error": f"未找到持仓: {validated_code}",
                    "message": f"未找到持仓: {validated_code}"
                }

            tx = self.portfolio.sell(
                tx_date=tx_date,
                asset_id=validated_code,
                account=self.account,
                quantity=quantity,
                price=price,
                currency=holding.currency,
                market=market or holding.market,
                fee=fee,
                auto_add_cash=auto_add_cash,
                request_id=request_id
            )

            return {
                "success": True,
                "transaction": {
                    "record_id": tx.record_id,
                    "date": tx.tx_date.isoformat(),
                    "code": tx.asset_id,
                    "name": tx.asset_name,
                    "quantity": quantity,
                    "price": price,
                    "proceeds": quantity * price - fee,
                    "fee": fee
                },
                "message": f"卖出记录已保存: {tx.asset_name} {quantity}股 @ ¥{price}"
            }
        except Exception as e:
            return {"success": False, "error": str(e), "message": f"记录失败: {e}"}

    def deposit(self, amount: float, date_str: str = None,
                remark: str = "入金", currency: str = "CNY") -> Dict[str, Any]:
        """记录入金"""
        try:
            flow_date = parse_date(date_str)
            cf = self.portfolio.deposit(
                flow_date=flow_date,
                account=self.account,
                amount=amount,
                currency=currency,
                remark=remark
            )
            return {
                "success": True,
                "cashflow": {
                    "record_id": cf.record_id,
                    "date": cf.flow_date.isoformat(),
                    "amount": cf.amount,
                    "currency": cf.currency,
                    "remark": remark
                },
                "message": f"入金记录已保存: ¥{amount:,.2f}"
            }
        except Exception as e:
            return {"success": False, "error": str(e)}

    def withdraw(self, amount: float, date_str: str = None,
                 remark: str = "出金", currency: str = "CNY") -> Dict[str, Any]:
        """记录出金"""
        try:
            flow_date = parse_date(date_str)
            cf = self.portfolio.withdraw(
                flow_date=flow_date,
                account=self.account,
                amount=amount,
                currency=currency,
                remark=remark
            )
            return {
                "success": True,
                "cashflow": {
                    "record_id": cf.record_id,
                    "date": cf.flow_date.isoformat(),
                    "amount": -amount,
                    "currency": cf.currency,
                    "remark": remark
                },
                "message": f"出金记录已保存: ¥{amount:,.2f}"
            }
        except Exception as e:
            return {"success": False, "error": str(e)}

    # ---------- 持仓查询 ----------

    def get_holdings(self, include_cash: bool = True, group_by_market: bool = False,
                     include_price: bool = False, timeout: int = 10) -> Dict[str, Any]:
        """获取持仓列表

        Args:
            include_cash: 是否包含现金资产
            group_by_market: 是否按券商分组
            include_price: 是否包含实时价格
            timeout: 价格获取超时时间（秒）
        """
        try:
            holdings = self.storage.get_holdings(account=self.account)

            # 只有需要价格时才获取
            prices = {}
            price_errors = []

            if include_price and holdings:
                # 获取所有需要价格的资产代码（包括外币现金，需要获取汇率）
                codes = [h.asset_id for h in holdings]
                name_map = {h.asset_id: h.asset_name for h in holdings}

                if codes:
                    # 用独立守护线程实现超时，避免嵌套 ThreadPoolExecutor 死锁
                    import threading
                    _fetch_result = {'prices': None, 'error': None}

                    def _do_fetch():
                        try:
                            _fetch_result['prices'] = self.price_fetcher.fetch_batch(
                                codes, name_map,
                                use_concurrent=True,
                                skip_us=False
                            )
                        except Exception as e:
                            _fetch_result['error'] = e

                    t = threading.Thread(target=_do_fetch, daemon=True)
                    t.start()
                    t.join(timeout=timeout)

                    if _fetch_result['prices'] is not None:
                        prices = _fetch_result['prices']
                    else:
                        if t.is_alive():
                            price_errors.append(f"价格获取超时（{timeout}秒），使用缓存数据")
                        elif _fetch_result['error']:
                            price_errors.append(f"价格获取异常: {_fetch_result['error']}")
                        # fallback: 仅用缓存，不启动并发避免线程泄漏
                        prices = self.price_fetcher.fetch_batch(
                            codes, name_map,
                            use_concurrent=False,
                            skip_us=True,
                            use_cache_only=True
                        )

            total_cny = 0
            cash_value = 0
            result_holdings = []
            normalization_warnings = []

            for h in holdings:
                # 获取价格（如果已获取）
                price_data = prices.get(h.asset_id, {})

                # 统一资产分类口径
                normalized_type = normalize_asset_type(h.asset_type, h.asset_id)
                warn = normalization_warning(h.asset_type, h.asset_id)
                if warn and warn not in normalization_warnings:
                    normalization_warnings.append(warn)
                is_cash_asset = normalized_type == 'cash'

                if is_cash_asset:
                    current_price = 1.0
                    # 现金的 cny_price 应该是汇率，对于本币是 1.0
                    if h.currency == 'CNY':
                        cny_price = 1.0
                    elif price_data and 'cny_price' in price_data:
                        cny_price = price_data['cny_price']
                    else:
                        # 无法获取汇率，添加到错误列表
                        price_errors.append(f"{h.asset_name}({h.asset_id}): 无法获取汇率")
                        cny_price = None

                    if cny_price is not None:
                        market_value = h.quantity * cny_price
                        cash_value += market_value
                    else:
                        market_value = None
                elif price_data and 'price' in price_data:
                    current_price = price_data['price']
                    cny_price = price_data.get('cny_price', price_data['price'])
                    market_value = h.quantity * cny_price
                else:
                    current_price = None
                    cny_price = None
                    market_value = None

                # 校验：持仓不为0但市值为0，说明价格获取失败
                if include_price and h.quantity != 0 and (market_value is None or market_value == 0):
                    if normalized_type != 'cash':
                        price_errors.append(f"{h.asset_name}({h.asset_id}): 持仓{h.quantity}但市值为0，价格获取失败")

                if market_value is not None:
                    total_cny += market_value

                if include_cash or normalized_type != 'cash':
                    item = {
                        "code": h.asset_id,
                        "name": h.asset_name,
                        "quantity": h.quantity,
                        "type": h.asset_type.value if h.asset_type else None,
                        "normalized_type": normalized_type,
                        "market": h.market,
                        "currency": h.currency
                    }
                    # 只有在包含价格时才添加价格字段
                    if include_price:
                        item.update({
                            "price": current_price,
                            "cny_price": cny_price,
                            "market_value": market_value,
                        })
                    result_holdings.append(item)

            # 只有在包含价格时才计算权重和排序
            if include_price:
                for item in result_holdings:
                    mv = item.get("market_value") or 0
                    item["weight"] = mv / total_cny if total_cny > 0 else 0
                result_holdings.sort(key=lambda x: x.get("market_value") or 0, reverse=True)

            result = {
                "success": True,
                "count": len(result_holdings)
            }

            # 只有包含价格时才返回市值信息
            if include_price:
                result.update({
                    "total_value": total_cny,
                    "cash_value": cash_value,
                    "stock_value": total_cny - cash_value,
                    "cash_ratio": cash_value / total_cny if total_cny > 0 else 0,
                })

            # 添加警告信息
            all_warnings = []
            if normalization_warnings:
                all_warnings.extend([f"分类兜底: {w}" for w in normalization_warnings])
            if price_errors:
                all_warnings.extend(price_errors)
            if all_warnings:
                result["warnings"] = all_warnings

            if group_by_market:
                # 按券商分组
                by_market = {}
                for h in result_holdings:
                    market = h.get("market") or "未指定券商"
                    if market not in by_market:
                        by_market[market] = []
                    by_market[market].append(h)

                if include_price:
                    # 计算各券商市值
                    market_values = {}
                    for market, items in by_market.items():
                        market_values[market] = sum((item.get("market_value") or 0) for item in items)

                    # 按市值排序券商
                    sorted_markets = sorted(by_market.keys(),
                                            key=lambda m: market_values[m],
                                            reverse=True)

                    result["by_market"] = {m: by_market[m] for m in sorted_markets}
                    result["market_values"] = {m: market_values[m] for m in sorted_markets}
                else:
                    result["by_market"] = by_market
                result["market_count"] = len(by_market)
            else:
                result["holdings"] = result_holdings

            return result
        except Exception as e:
            return {"success": False, "error": str(e)}


    def get_position(self, holdings_data: Dict[str, Any] = None) -> Dict[str, Any]:
        """获取仓位分析

        Args:
            holdings_data: 已获取的持仓数据，如果提供则直接使用，避免重复查询
        """
        if holdings_data is not None:
            result = holdings_data
        else:
            result = self.get_holdings(include_price=True)

        if not result.get("success"):
            return result if isinstance(result, dict) else {"success": False, "error": "获取持仓失败"}

        holdings = result.get("holdings", [])
        stock_value = sum((h.get("market_value") or 0) for h in holdings
                         if h.get("normalized_type") == "stock")
        fund_value = sum((h.get("market_value") or 0) for h in holdings if h.get("normalized_type") == "fund")
        total_value = result.get("total_value", 0)
        cash_value = sum((h.get("market_value") or 0) for h in holdings if h.get("normalized_type") == "cash")
        cash_ratio = result.get("cash_ratio", 0)

        return {
            "success": True,
            "total_value": total_value,
            "stock_value": stock_value,
            "fund_value": fund_value,
            "cash_value": cash_value,
            "stock_ratio": stock_value / total_value if total_value > 0 else 0,
            "fund_ratio": fund_value / total_value if total_value > 0 else 0,
            "cash_ratio": cash_ratio,
        }

    def get_distribution(self, holdings_data: Dict[str, Any] = None) -> Dict[str, Any]:
        """获取资产分布

        Args:
            holdings_data: 已获取的持仓数据，如果提供则直接使用，避免重复查询
        """
        if holdings_data is not None:
            result = holdings_data
        else:
            result = self.get_holdings(include_price=True)

        if not result.get("success"):
            return result if isinstance(result, dict) else {"success": False, "error": "获取持仓失败"}

        type_dist = {}
        market_dist = {}
        currency_dist = {}

        holdings = result.get("holdings", [])
        for h in holdings:
            # 按类型求和
            t = h.get("normalized_type") or "other"
            market_value = h.get("market_value") or 0
            type_dist[t] = type_dist.get(t, 0) + market_value

            # 按券商求和
            market = h.get("market") or "未指定券商"
            market_dist[market] = market_dist.get(market, 0) + market_value

            # 按币种求和
            currency = h.get("currency") or "CNY"
            currency_dist[currency] = currency_dist.get(currency, 0) + market_value

        total = result.get("total_value", 0)

        def sort_by_value(items_dict):
            return sorted(items_dict.items(), key=lambda x: x[1], reverse=True)

        return {
            "success": True,
            "total_value": total,
            "by_type": [{"type": k, "value": v, "ratio": v/total if total > 0 else 0} for k, v in sort_by_value(type_dist)],
            "by_market": [{"market": k, "value": v, "ratio": v/total if total > 0 else 0} for k, v in sort_by_value(market_dist)],
            "by_currency": [{"currency": k, "value": v, "ratio": v/total if total > 0 else 0} for k, v in sort_by_value(currency_dist)]
        }

    # ---------- 净值和收益 ----------

    def get_nav(self) -> Dict[str, Any]:
        """获取账户净值"""
        try:
            # 一次 API 调用获取最近 30 天，从中取 latest
            navs = self.storage.get_nav_history(self.account, days=30)
            if not navs:
                return {"success": False, "message": "无净值记录"}

            latest = navs[-1]  # navs 已按日期升序排列

            # 构建 latest 响应，核心指标已为顶层字段
            latest_data = {
                "date": latest.date.isoformat(),
                "nav": latest.nav,
                "shares": latest.shares,
                "total_value": latest.total_value,
                "stock_value": latest.stock_value,
                "cash_value": latest.cash_value,
                "stock_weight": latest.stock_weight,
                "cash_weight": latest.cash_weight,
                "cash_flow": latest.cash_flow,
                "share_change": latest.share_change,
                "mtd_nav_change": latest.mtd_nav_change,
                "ytd_nav_change": latest.ytd_nav_change,
                "mtd_pnl": latest.mtd_pnl,
                "ytd_pnl": latest.ytd_pnl,
            }

            # 添加 details 中的扩展数据（各年份明细、累计等）
            if latest.details:
                latest_data["details"] = latest.details
                # 动态展开各年份数据 (nav_change_YYYY, appreciation_YYYY, ...)
                for k, v in latest.details.items():
                    if k.startswith(("nav_change_", "appreciation_", "cash_flow_")) and k not in latest_data:
                        latest_data[k] = v
                # 展开累计数据
                for key in ("cumulative_appreciation", "cumulative_nav_change",
                            "year_cash_flow", "initial_value"):
                    if key in latest.details:
                        latest_data[key] = latest.details[key]

            # 构建 history，包含关键指标
            history = []
            for n in navs[:30]:
                item = {
                    "date": n.date.isoformat(),
                    "nav": n.nav,
                    "share_change": n.share_change,
                }
                history.append(item)

            return {
                "success": True,
                "latest": latest_data,
                "history": history
            }
        except Exception as e:
            return {"success": False, "error": str(e)}

    def get_return(self, period_type: str, period: str = None) -> Dict[str, Any]:
        """
        获取收益率

        Args:
            period_type: "month", "year", "since_inception"
            period: 月份(2025-03) 或 年份(2025)
        """
        try:
            if period_type == "month":
                return self._calc_month_return(period)
            elif period_type == "year":
                return self._calc_year_return(period)
            elif period_type in ("since_inception", "since2024"):
                return self._calc_since_inception_return()
            else:
                return {"success": False, "error": f"不支持的周期类型: {period_type}"}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def _calc_month_return(self, month: str, _navs: list = None) -> Dict:
        """计算月收益率（环比：较上月末的变化）"""
        navs = _navs if _navs is not None else self.storage.get_nav_history(self.account, days=365)
        month_navs = [n for n in navs if n.date.strftime('%Y-%m') == month]

        if len(month_navs) < 1:
            return {"success": False, "message": f"{month} 数据不足"}

        # 当月最后一天净值
        end_nav = max(month_navs, key=lambda x: x.date)

        # 获取上月最后一天净值（作为基准）
        year, mon = int(month[:4]), int(month[5:7])
        if mon == 1:
            prev_month = f"{year-1}-12"
        else:
            prev_month = f"{year}-{mon-1:02d}"

        prev_month_navs = [n for n in navs if n.date.strftime('%Y-%m') == prev_month]
        if prev_month_navs:
            # 上月有数据，使用上月最后一天作为基准
            start_nav = max(prev_month_navs, key=lambda x: x.date)
            start_nav_label = "上月末"
        else:
            # 上月无数据，使用当月第一天作为基准（首次记录）
            start_nav = min(month_navs, key=lambda x: x.date)
            start_nav_label = "月初"

        ret = (end_nav.nav - start_nav.nav) / start_nav.nav * 100 if start_nav.nav > 0 else 0

        return {
            "success": True,
            "period": month,
            "return_pct": ret,
            "start_nav": start_nav.nav,
            "end_nav": end_nav.nav,
            "start_date": start_nav.date.isoformat(),
            "end_date": end_nav.date.isoformat(),
            "base": start_nav_label
        }

    def _calc_year_return(self, year: str, _navs: list = None) -> Dict:
        """计算年收益率（环比：较上年末的变化）"""
        navs = _navs if _navs is not None else self.storage.get_nav_history(self.account, days=730)
        year_navs = [n for n in navs if n.date.strftime('%Y') == year]

        if len(year_navs) < 1:
            return {"success": False, "message": f"{year} 数据不足"}

        # 当年最后一天净值
        end_nav = max(year_navs, key=lambda x: x.date)

        # 获取上年最后一天净值（作为基准）
        prev_year = str(int(year) - 1)
        prev_year_navs = [n for n in navs if n.date.strftime('%Y') == prev_year]

        if prev_year_navs:
            # 上年有数据，使用上年最后一天作为基准
            start_nav = max(prev_year_navs, key=lambda x: x.date)
            start_nav_label = "上年末"
        else:
            # 上年无数据，使用当年第一天作为基准（首次记录）
            start_nav = min(year_navs, key=lambda x: x.date)
            start_nav_label = "年初"

        ret = (end_nav.nav - start_nav.nav) / start_nav.nav * 100 if start_nav.nav > 0 else 0

        return {
            "success": True,
            "period": year,
            "return_pct": ret,
            "start_nav": start_nav.nav,
            "end_nav": end_nav.nav,
            "start_date": start_nav.date.isoformat(),
            "end_date": end_nav.date.isoformat(),
            "base": start_nav_label
        }

    def _calc_since_inception_return(self, _navs: list = None) -> Dict:
        """计算自 start_year 以来收益（以上年末净值为基准，标准化为1）"""
        start_year = config.get_start_year()
        BASE_DATE = date(start_year - 1, 12, 31)

        if _navs is not None:
            # 使用预获取的净值数据
            base_candidates = [n for n in _navs if n.date <= BASE_DATE]
            base_nav = max(base_candidates, key=lambda n: n.date) if base_candidates else None
            latest = _navs[-1] if _navs else None
        else:
            base_nav = self.storage.get_nav_on_date(self.account, BASE_DATE)
            if not base_nav:
                base_nav = self.storage.get_latest_nav_before(self.account, BASE_DATE)
            latest = self.storage.get_latest_nav(self.account)

        if not base_nav or not latest:
            return {"success": False, "message": "数据不足"}

        actual_start_nav = base_nav.nav
        actual_latest_nav = latest.nav
        if not actual_start_nav or actual_start_nav <= 0:
            return {"success": False, "message": "基准净值无效"}
        normalized_nav = actual_latest_nav / actual_start_nav

        total_ret = (normalized_nav - 1.0) * 100
        days = (latest.date - BASE_DATE).days
        years = days / 365.25
        cagr = ((normalized_nav) ** (1/years) - 1) * 100 if years > 0 else 0

        return {
            "success": True,
            "period": f"{start_year}至今",
            "return_pct": total_ret,
            "total_return_pct": total_ret,
            "cagr": cagr,
            "cagr_pct": cagr,
            "days": days,
            "start_nav": 1.0,
            "start_date": BASE_DATE.isoformat(),
            "latest_nav": round(normalized_nav, 4),
            "actual_start_nav": actual_start_nav,
            "actual_latest_nav": actual_latest_nav,
            "base": f"{start_year - 1}年末"
        }

    # ---------- 现金管理 ----------

    def get_cash(self) -> Dict[str, Any]:
        """获取现金资产明细"""
        try:
            holdings = self.storage.get_holdings(account=self.account)
            cash_holdings = [h for h in holdings if h.asset_type in [AssetType.CASH, AssetType.MMF]]

            items = []
            by_currency = {}
            for h in cash_holdings:
                currency = h.currency or 'CNY'
                items.append({
                    "code": h.asset_id,
                    "name": h.asset_name,
                    "amount": h.quantity,
                    "currency": currency,
                    "type": h.asset_type.value
                })
                by_currency[currency] = by_currency.get(currency, 0) + h.quantity

            return {
                "success": True,
                "by_currency": by_currency,
                "items": items,
                "count": len(items)
            }
        except Exception as e:
            return {"success": False, "error": str(e)}

    def add_cash(self, amount: float, asset: str = "CNY-CASH") -> Dict[str, Any]:
        """增加现金"""
        try:
            holding = self.storage.get_holding(asset, self.account)
            if holding:
                new_qty = holding.quantity + amount
                self.storage.update_holding_quantity(asset, self.account, amount, getattr(holding, 'market', None))
                return {
                    "success": True,
                    "asset": asset,
                    "amount": amount,
                    "balance": new_qty,
                    "message": f"{asset} 增加 ¥{amount:,.2f}，当前余额: ¥{new_qty:,.2f}"
                }
            else:
                return {"success": False, "error": f"未找到 {asset}，需要先创建"}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def sub_cash(self, amount: float, asset: str = "CNY-CASH") -> Dict[str, Any]:
        """减少现金"""
        try:
            holding = self.storage.get_holding(asset, self.account)
            if not holding:
                return {"success": False, "error": f"未找到 {asset}"}

            if holding.quantity < amount:
                return {
                    "success": False,
                    "error": f"余额不足，当前: ¥{holding.quantity:,.2f}"
                }

            new_qty = holding.quantity - amount
            self.storage.update_holding_quantity(asset, self.account, -amount, getattr(holding, 'market', None))
            return {
                "success": True,
                "asset": asset,
                "amount": amount,
                "balance": new_qty,
                "message": f"{asset} 减少 ¥{amount:,.2f}，当前余额: ¥{new_qty:,.2f}"
            }
        except Exception as e:
            return {"success": False, "error": str(e)}

    # ---------- 完整报告 ----------

    def generate_report(self, report_type: str = "daily",
                        record_nav: bool = False, price_timeout: int = 30,
                        snapshot: Optional[Dict[str, Any]] = None,
                        overwrite_existing: bool = True,
                        dry_run: bool = False) -> Dict[str, Any]:
        """生成日报/月报/年报

        Args:
            report_type: "daily" | "monthly" | "yearly"
            record_nav: 是否自动记录今日净值
            price_timeout: 价格获取超时时间（秒）
        """
        snapshot = snapshot or self.build_snapshot()
        full = self.full_report(price_timeout=price_timeout, snapshot=snapshot)
        if not full.get("success"):
            return full

        # 记录净值在报告生成之后，避免影响报告数据
        nav_recorded = None
        if record_nav:
            nav_recorded = self.record_nav(
                price_timeout=price_timeout,
                snapshot=snapshot,
                overwrite_existing=overwrite_existing,
                dry_run=dry_run,
            )

        nav = full.get("nav") or {}
        nav_details = nav.get("details") or {}
        returns = full.get("returns") or {}
        since_inception = returns.get("since_inception") or {}
        cagr_value = nav_details.get("cagr")
        cagr_pct_value = nav_details.get("cagr_pct")
        if cagr_value is None and since_inception.get("success"):
            # 兼容 nav_history 表没有 details 字段的情况
            cagr_pct_value = since_inception.get("cagr_pct")
            cagr_value = (cagr_pct_value / 100) if cagr_pct_value is not None else since_inception.get("cagr")

        report_warnings = list(full.get("warnings") or [])

        if report_type == "daily":
            return {
                "success": True,
                "snapshot_time": snapshot.get("snapshot_time"),
                "report_type": "日报",
                "date": nav.get("date"),
                "overview": full["overview"],
                "nav": nav.get("nav"),
                "total_value": nav.get("total_value"),
                "cash_flow": nav.get("cash_flow"),
                "top_holdings": full.get("top_holdings"),
                "cagr": cagr_value,
                "cagr_pct": cagr_pct_value,
                "warnings": report_warnings,
            }

        elif report_type == "monthly":
            return {
                "success": True,
                "snapshot_time": snapshot.get("snapshot_time"),
                "report_type": "月报",
                "date": nav.get("date"),
                "overview": full["overview"],
                "nav": nav.get("nav"),
                "total_value": nav.get("total_value"),
                "monthly_return": returns.get("monthly"),
                "mtd_nav_change": nav.get("mtd_nav_change"),
                "mtd_pnl": nav.get("mtd_pnl"),
                "top_holdings": full.get("top_holdings"),
                "distribution": full.get("distribution"),
                "cagr": cagr_value,
                "cagr_pct": cagr_pct_value,
            }

        elif report_type == "yearly":
            # 收集各年份涨幅和升值（从 nav.details 读取）
            yearly_breakdown = {}
            for k, v in nav_details.items():
                if k.startswith(("nav_change_", "appreciation_", "cash_flow_")):
                    yearly_breakdown[k] = v

            return {
                "success": True,
                "snapshot_time": snapshot.get("snapshot_time"),
                "report_type": "年报",
                "date": nav.get("date"),
                "overview": full["overview"],
                "nav": nav.get("nav"),
                "total_value": nav.get("total_value"),
                "yearly_return": returns.get("yearly"),
                "ytd_nav_change": nav.get("ytd_nav_change"),
                "ytd_pnl": nav.get("ytd_pnl"),
                "since_inception": returns.get("since_inception"),
                "risk": {
                    "volatility": returns.get("historical_volatility"),
                    "max_drawdown": returns.get("max_drawdown"),
                },
                "yearly_breakdown": yearly_breakdown,
                "cumulative_nav_change": nav_details.get("cumulative_nav_change"),
                "cumulative_appreciation": nav_details.get("cumulative_appreciation"),
                "top_holdings": full.get("top_holdings"),
                "distribution": full.get("distribution"),
            }

        else:
            return {"success": False, "error": f"不支持的报告类型: {report_type}，可选: daily/monthly/yearly"}

    def full_report(self, price_timeout: int = 30, snapshot: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """生成完整报告（只读，不记录净值）

        利用实时持仓价格合成"今日"虚拟净值，确保收益统计始终可用，
        即使当天尚未调用 record_nav()。

        Args:
            price_timeout: 价格获取超时时间（秒），默认30秒
        """
        try:
            snapshot = snapshot or self.build_snapshot()
            valuation = snapshot["valuation"]
            holdings_data = snapshot["holdings_data"]
            position_data = snapshot["position_data"]

            # 一次性获取全部净值历史（1 次 API 调用）
            all_navs = self.storage.get_nav_history(self.account, days=9999)

            # --- 合成实时虚拟净值 ---
            # 用统一估值结果 + 最近一次记录的份额，推算当前净值与四个派生指标
            today = date.today()
            live_total = valuation.total_value_cny
            live_cash = valuation.cash_value_cny
            live_stock = valuation.stock_value_cny + valuation.fund_value_cny

            working_navs = [n for n in all_navs if n.date < today]
            synthetic_nav = None
            if all_navs and live_total > 0:
                last_nav = all_navs[-1]
                if last_nav.shares and last_nav.shares > 0:
                    current_year = str(today.year)
                    yesterday_nav = self.portfolio._find_latest_nav_before(all_navs, today)
                    prev_year_end_nav = self.portfolio._find_year_end_nav(all_navs, str(today.year - 1))
                    prev_month_end_nav = self.portfolio._find_prev_month_end_nav(all_navs, today.year, today.month)
                    daily_cash_flow = self.portfolio._get_daily_cash_flow(self.account, today)
                    monthly_cash_flow = self.portfolio._get_monthly_cash_flow(self.account, today.year, today.month)
                    yearly_cash_flow = self.portfolio._get_yearly_cash_flow(self.account, current_year)
                    if last_nav and last_nav.date < today:
                        from datetime import timedelta
                        gap_start = last_nav.date + timedelta(days=1)
                        gap_cash_flow = self.portfolio._get_period_cash_flow(self.account, gap_start, today)
                        base_shares = last_nav.shares or 0
                        base_nav = last_nav.nav
                    else:
                        gap_cash_flow = daily_cash_flow
                        base_shares = last_nav.shares or 0
                        base_nav = last_nav.nav

                    synthetic_share_change = (gap_cash_flow / base_nav) if base_nav else 0.0
                    synthetic_shares = base_shares + synthetic_share_change
                    synthetic_nav_value = live_total / synthetic_shares if synthetic_shares > 0 else 1.0
                    synthetic_mtd_nav_change = self.portfolio._calc_mtd_nav_change(synthetic_nav_value, prev_month_end_nav)
                    synthetic_ytd_nav_change = self.portfolio._calc_ytd_nav_change(synthetic_nav_value, prev_year_end_nav)
                    synthetic_mtd_pnl = self.portfolio._calc_mtd_pnl(live_total, prev_month_end_nav, monthly_cash_flow)
                    synthetic_ytd_pnl = self.portfolio._calc_ytd_pnl(live_total, prev_year_end_nav, yearly_cash_flow)
                    synthetic_daily_pnl = None
                    if yesterday_nav and yesterday_nav.date and (today - yesterday_nav.date).days == 1:
                        synthetic_daily_pnl = live_total - yesterday_nav.total_value - gap_cash_flow

                    synthetic_nav = NAVHistory(
                        date=today,
                        account=self.account,
                        total_value=round(live_total, 2),
                        cash_value=round(live_cash, 2),
                        stock_value=round(live_stock, 2),
                        fund_value=round(valuation.fund_value_cny, 2),
                        cn_stock_value=round(valuation.cn_asset_value, 2),
                        us_stock_value=round(valuation.us_asset_value, 2),
                        hk_stock_value=round(valuation.hk_asset_value, 2),
                        shares=round(synthetic_shares, 2),
                        nav=round(synthetic_nav_value, 6),
                        stock_weight=round(live_stock / live_total, 6) if live_total > 0 else 0,
                        cash_weight=round(live_cash / live_total, 6) if live_total > 0 else 0,
                        cash_flow=round(daily_cash_flow, 2),
                        share_change=round(synthetic_share_change, 2),
                        mtd_nav_change=round(synthetic_mtd_nav_change, 6),
                        ytd_nav_change=round(synthetic_ytd_nav_change, 6),
                        pnl=round(synthetic_daily_pnl, 2) if synthetic_daily_pnl is not None else None,
                        mtd_pnl=round(synthetic_mtd_pnl, 2),
                        ytd_pnl=round(synthetic_ytd_pnl, 2),
                        details={"is_synthetic": True},
                    )
                    working_navs.append(synthetic_nav)

            # 从 working_navs（含虚拟今日）构建 nav_latest
            nav_latest = None
            if working_navs:
                latest = working_navs[-1]
                nav_latest = {
                    "date": latest.date.isoformat(),
                    "nav": latest.nav,
                    "shares": latest.shares,
                    "total_value": latest.total_value,
                    "stock_value": latest.stock_value,
                    "cash_value": latest.cash_value,
                    "stock_weight": latest.stock_weight,
                    "cash_weight": latest.cash_weight,
                }
                # 这些字段仅在已记录的 NAV 中有值（虚拟 NAV 为 None）
                if latest.mtd_nav_change is not None:
                    nav_latest["mtd_nav_change"] = latest.mtd_nav_change
                    nav_latest["ytd_nav_change"] = latest.ytd_nav_change
                    nav_latest["pnl"] = latest.pnl
                    nav_latest["mtd_pnl"] = latest.mtd_pnl
                    nav_latest["ytd_pnl"] = latest.ytd_pnl
                    nav_latest["cash_flow"] = latest.cash_flow
                    nav_latest["share_change"] = latest.share_change
                if latest.details:
                    nav_latest["details"] = latest.details

            # 计算风险指标（复用 all_navs，不含虚拟净值）
            hist_volatility, hist_max_dd = self._calc_risk_metrics(all_navs)

            # 获取资产分布（复用持仓数据）
            distribution_data = self.get_distribution(holdings_data=holdings_data)
            distribution_result = distribution_data.get("by_type", []) if distribution_data.get("success") else []

            # 计算收益率（使用 working_navs，含虚拟今日净值）
            current_year = str(today.year)
            current_month = today.strftime('%Y-%m')

            monthly_return = self._calc_month_return(current_month, _navs=working_navs)
            yearly_return = self._calc_year_return(current_year, _navs=working_navs)
            since_inception = self._calc_since_inception_return(_navs=working_navs)

            # 获取 top10 持仓列表
            top_holdings_list = holdings_data.get("holdings", [])[:10]

            return {
                "success": True,
                "generated_at": datetime.now().isoformat(),
                "overview": {
                    "total_value": holdings_data.get("total_value", 0),
                    "cash_ratio": position_data.get("cash_ratio", 0),
                    "stock_ratio": position_data.get("stock_ratio", 0),
                    "fund_ratio": position_data.get("fund_ratio", 0)
                },
                "nav": nav_latest,
                "returns": {
                    "monthly": monthly_return,
                    "yearly": yearly_return,
                    "since_inception": since_inception,
                    "historical_volatility": hist_volatility,
                    "max_drawdown": hist_max_dd
                },
                "top_holdings": top_holdings_list,
                "distribution": distribution_result
            }
        except Exception as e:
            return {"success": False, "error": str(e)}

    def record_nav(self, price_timeout: int = 30, snapshot: Optional[Dict[str, Any]] = None,
                   overwrite_existing: bool = True, dry_run: bool = False) -> Dict[str, Any]:
        """记录今日净值（独立方法，与报告生成解耦）

        Args:
            price_timeout: 价格获取超时时间（秒）
            snapshot: 可复用的统一估值快照
            overwrite_existing: 是否允许覆盖同日已有净值记录
            dry_run: 仅演练，不实际写入
        """
        try:
            snapshot = snapshot or self.build_snapshot()
            valuation = snapshot["valuation"]
            today = date.today()
            nav_record = self.portfolio.record_nav(
                self.account,
                valuation=valuation,
                nav_date=today,
                persist=True,
                overwrite_existing=overwrite_existing,
                dry_run=dry_run,
            )
            storage_result = None
            result = {
                "success": True,
                "date": today.isoformat(),
                "nav": nav_record.nav,
                "total_value": nav_record.total_value,
                "shares": nav_record.shares,
                "message": (f"已演练 {today} 净值写入: {nav_record.nav:.4f}" if dry_run else f"已记录 {today} 净值: {nav_record.nav:.4f}")
            }
            result["snapshot_time"] = snapshot.get("snapshot_time")
            result["dry_run"] = dry_run
            if storage_result is not None:
                result["storage"] = storage_result
            if valuation.warnings:
                result["warnings"] = valuation.warnings
            return result
        except Exception as e:
            return {"success": False, "error": str(e)}

    def _calc_risk_metrics(self, navs) -> tuple:
        """计算风险指标：波动率和最大回撤"""
        import statistics

        if len(navs) < 2:
            return 0, 0

        # 过滤无效 nav
        valid_navs = [n for n in navs if n.nav and n.nav > 0]
        if len(valid_navs) < 2:
            return 0, 0

        returns = []
        for i in range(1, len(valid_navs)):
            r = (valid_navs[i].nav - valid_navs[i-1].nav) / valid_navs[i-1].nav
            returns.append(r)

        volatility = statistics.stdev(returns) * (252 ** 0.5) * 100 if len(returns) > 1 else 0

        max_dd = 0
        peak = valid_navs[0].nav
        for nav in valid_navs[1:]:
            if nav.nav > peak:
                peak = nav.nav
            dd = (peak - nav.nav) / peak
            if dd > max_dd:
                max_dd = dd

        return volatility, max_dd * 100

    # ---------- 价格查询 ----------

    def get_price(self, code: str) -> Dict[str, Any]:
        """查询资产价格"""
        try:
            asset_type, currency, _ = detect_asset_type(code)
            result = self.price_fetcher.fetch(code)

            if result and 'price' in result:
                return {
                    "success": True,
                    "code": code.upper(),
                    "name": result.get('name', 'N/A'),
                    "price": result['price'],
                    "currency": result.get('currency', currency),
                    "cny_price": result.get('cny_price'),
                    "change_pct": result.get('change_pct'),
                    "source": result.get('source', 'N/A')
                }
            else:
                return {"success": False, "error": f"无法获取 {code} 的价格"}
        except Exception as e:
            return {"success": False, "error": str(e)}


# ========== 数据库初始化 ==========

def init_db(account: str = DEFAULT_ACCOUNT, initial_cash: float = 0) -> Dict[str, Any]:
    """
    初始化投资组合数据库（飞书多维表）

    Args:
        account: 账户标识，默认 "lx"
        initial_cash: 初始现金金额（可选），默认 0

    Returns:
        {"success": bool, "message": str}

    Example:
        # 初始化空数据库
        init_db()

        # 初始化并设置初始现金 10万元
        init_db(initial_cash=100000)
    """
    try:
        storage = FeishuStorage()

        # 检查飞书配置
        if not storage.client.app_token:
            raise ValueError("未配置 FEISHU_APP_TOKEN，无法连接飞书多维表")

        # 创建初始现金持仓（如果需要）
        if initial_cash > 0:
            cash_holding = storage.get_holding('CNY-CASH', account)
            if not cash_holding:
                holding = Holding(
                    asset_id='CNY-CASH',
                    asset_name='人民币现金',
                    asset_type=AssetType.CASH,
                    account=account,
                    quantity=initial_cash,
                    currency='CNY',
                    asset_class=AssetClass.CASH,
                    industry=Industry.CASH
                )
                storage.upsert_holding(holding)

        # 检查数据库状态
        holdings = storage.get_holdings(account=account)
        nav_history = storage.get_nav_history(account, days=1)

        return {
            "success": True,
            "account": account,
            "initial_cash": initial_cash,
            "current_holdings": len(holdings),
            "nav_records": len(nav_history),
            "message": f"已初始化飞书多维表\n" +
                      f"  - 持仓记录: {len(holdings)} 条\n" +
                      f"  - 净值记录: {len(nav_history)} 条\n" +
                      (f"  - 初始现金: ¥{initial_cash:,.2f}" if initial_cash > 0 else "")
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "message": f"初始化失败: {e}"
        }


# ========== 便捷函数（供 Skill 直接调用） ==========

_default_skill = None

def _get_default_skill() -> PortfolioSkill:
    """获取默认 Skill 实例（单例模式）"""
    global _default_skill
    if _default_skill is None:
        _default_skill = PortfolioSkill()
    return _default_skill


# 交易记录
def buy(code: str, name: str, quantity: float, price: float, **kwargs) -> Dict:
    """买入资产"""
    return _get_default_skill().buy(code, name, quantity, price, **kwargs)

def sell(code: str, quantity: float, price: float, **kwargs) -> Dict:
    """卖出资产"""
    return _get_default_skill().sell(code, quantity, price, **kwargs)

def deposit(amount: float, **kwargs) -> Dict:
    """入金"""
    return _get_default_skill().deposit(amount, **kwargs)

def withdraw(amount: float, **kwargs) -> Dict:
    """出金"""
    return _get_default_skill().withdraw(amount, **kwargs)

# 持仓查询
def get_holdings(**kwargs) -> Dict:
    """全部持仓"""
    return _get_default_skill().get_holdings(**kwargs)

def get_position() -> Dict:
    """仓位分析"""
    return _get_default_skill().get_position()

def get_distribution() -> Dict:
    """资产分布"""
    return _get_default_skill().get_distribution()

# 净值收益
def get_nav() -> Dict:
    """账户净值"""
    return _get_default_skill().get_nav()

def get_return(period_type: str, period: str = None) -> Dict:
    """查询收益率"""
    return _get_default_skill().get_return(period_type, period)

# 现金管理
def get_cash() -> Dict:
    """现金资产"""
    return _get_default_skill().get_cash()

def add_cash(amount: float, **kwargs) -> Dict:
    """增加现金"""
    return _get_default_skill().add_cash(amount, **kwargs)

def sub_cash(amount: float, **kwargs) -> Dict:
    """减少现金"""
    return _get_default_skill().sub_cash(amount, **kwargs)

# 报告
def generate_report(report_type: str = "daily", record_nav: bool = False, price_timeout: int = 30) -> Dict:
    """生成日报/月报/年报"""
    return _get_default_skill().generate_report(report_type=report_type, record_nav=record_nav, price_timeout=price_timeout)

def full_report(price_timeout: int = 30) -> Dict:
    """完整报告（只读，不记录净值）

    Args:
        price_timeout: 价格获取超时时间（秒），默认30秒
    """
    return _get_default_skill().full_report(price_timeout=price_timeout)

def record_nav(price_timeout: int = 30) -> Dict:
    """记录今日净值"""
    return _get_default_skill().record_nav(price_timeout=price_timeout)

# 价格
def get_price(code: str) -> Dict:
    """查询价格"""
    return _get_default_skill().get_price(code)


# 数据清理
def clean_data(table: str = None, account: str = None, dry_run: bool = True,
               code: str = None, date_before: str = None,
               empty_only: bool = False) -> Dict:
    """
    清理测试数据

    Args:
        table: 要清理的表 ('holdings', 'transactions', 'cash_flow', 'nav_history', 'all')
        account: 按账户过滤，默认当前账户
        dry_run: 是否只预览不删除（默认 True，设为 False 才实际删除）
        code: 按资产代码过滤（如 'TEST'）
        date_before: 删除指定日期之前的数据 (YYYY-MM-DD)
        empty_only: 只清理空记录（asset_id 为空 或 quantity/price 为 0）

    Returns:
        {"success": bool, "deleted": {...}, "preview": [...]}

    Example:
        # 预览要删除的数据
        clean_data(table='transactions', code='TEST')

        # 实际删除
        clean_data(table='transactions', code='TEST', dry_run=False)

        # 清理空记录
        clean_data(table='all', empty_only=True, dry_run=False, confirm=True)
    """
    try:
        skill = _get_default_skill()
        target_account = account or skill.account
        storage = skill.storage

        # 将 date_before 转换为时间戳（毫秒）用于与飞书字段比较
        date_before_ts = None
        if date_before:
            from datetime import datetime as dt
            d = dt.strptime(date_before, "%Y-%m-%d")
            date_before_ts = int(d.timestamp() * 1000)

        results = {
            'holdings': 0,
            'transactions': 0,
            'cash_flow': 0,
            'nav_history': 0
        }
        preview = []

        tables_to_clean = ['holdings', 'transactions', 'cash_flow', 'nav_history'] if table == 'all' else [table]

        for tbl in tables_to_clean:
            if tbl == 'holdings':
                # 获取所有记录（包括 quantity=0 的）
                records = storage.client.list_records('holdings')
                for record in records:
                    fields = record.get('fields', {})
                    r_id = record.get('record_id')
                    asset_id = fields.get('asset_id', '')
                    quantity = fields.get('quantity', 0)

                    should_delete = False
                    if empty_only:
                        # 空记录：asset_id 为空 或 quantity 为 0 或空
                        if not asset_id or asset_id.strip() == '' or not quantity or quantity == 0 or quantity == '0':
                            should_delete = True
                    else:
                        if code and asset_id == code.upper():
                            should_delete = True

                    if should_delete:
                        preview.append({
                            'table': tbl,
                            'record_id': r_id,
                            'asset_id': asset_id,
                            'quantity': quantity,
                            'reason': 'empty_record' if empty_only else 'code_match'
                        })
                        if not dry_run and r_id:
                            if storage.delete_holding_by_record_id(r_id):
                                results[tbl] += 1

            elif tbl == 'transactions':
                records = storage.client.list_records('transactions')
                for record in records:
                    fields = record.get('fields', {})
                    r_id = record.get('record_id')
                    asset_id = fields.get('asset_id', '')
                    tx_date = fields.get('tx_date', '')
                    quantity = fields.get('quantity', 0)
                    price = fields.get('price', 0)

                    should_delete = False
                    if empty_only:
                        # 空记录：asset_id 为空 或 quantity/price 为 0
                        if (not asset_id or asset_id.strip() == '' or
                            not quantity or quantity == 0 or quantity == '0' or
                            not price or price == 0 or price == '0'):
                            should_delete = True
                    else:
                        if code and asset_id == code.upper():
                            should_delete = True
                        if date_before_ts and tx_date:
                            ts = tx_date if isinstance(tx_date, (int, float)) else 0
                            if ts and ts <= date_before_ts:
                                should_delete = True

                    if should_delete:
                        preview.append({
                            'table': tbl,
                            'record_id': r_id,
                            'date': tx_date,
                            'asset_id': asset_id,
                            'reason': 'empty_record' if empty_only else 'matched'
                        })
                        if not dry_run and r_id:
                            if storage.delete_transaction_by_record_id(r_id):
                                results[tbl] += 1

            elif tbl == 'cash_flow':
                records = storage.client.list_records('cash_flow')
                for record in records:
                    fields = record.get('fields', {})
                    r_id = record.get('record_id')
                    flow_date = fields.get('flow_date', '')
                    amount = fields.get('amount', 0)

                    should_delete = False
                    if empty_only:
                        # 空记录：amount 为 0 或空
                        if not amount or amount == 0 or amount == '0' or amount == '':
                            should_delete = True
                    else:
                        if date_before_ts and flow_date:
                            ts = flow_date if isinstance(flow_date, (int, float)) else 0
                            if ts and ts <= date_before_ts:
                                should_delete = True

                    if should_delete:
                        preview.append({
                            'table': tbl,
                            'record_id': r_id,
                            'date': flow_date,
                            'amount': amount,
                            'reason': 'empty_record' if empty_only else 'matched'
                        })
                        if not dry_run and r_id:
                            if storage.delete_cash_flow_by_record_id(r_id):
                                results[tbl] += 1

            elif tbl == 'nav_history':
                records = storage.client.list_records('nav_history')
                for record in records:
                    fields = record.get('fields', {})
                    r_id = record.get('record_id')
                    nav_date = fields.get('date', '')
                    total_value = fields.get('total_value', 0)
                    nav = fields.get('nav', 0)

                    should_delete = False
                    if empty_only:
                        # 空记录：total_value 或 nav 为 0
                        if (not total_value or total_value == 0 or total_value == '0' or
                            not nav or nav == 0 or nav == '0'):
                            should_delete = True
                    else:
                        if date_before_ts and nav_date:
                            ts = nav_date if isinstance(nav_date, (int, float)) else 0
                            if ts and ts <= date_before_ts:
                                should_delete = True

                    if should_delete:
                        preview.append({
                            'table': tbl,
                            'record_id': r_id,
                            'date': nav_date,
                            'nav': nav,
                            'reason': 'empty_record' if empty_only else 'matched'
                        })
                        if not dry_run and r_id:
                            if storage.delete_nav_by_record_id(r_id):
                                results[tbl] += 1

        return {
            'success': True,
            'dry_run': dry_run,
            'empty_only': empty_only,
            'account': target_account,
            'filters': {'code': code, 'date_before': date_before},
            'deleted_count': results if not dry_run else {k: 0 for k in results},
            'preview': preview[:50],  # 最多显示50条
            'total_preview': len(preview),
            'message': f'{"【预览模式】" if dry_run else "【已删除】"} {"空记录" if empty_only else "匹配记录"}: {len(preview)} 条'
        }
    except Exception as e:
        return {'success': False, 'error': str(e)}
