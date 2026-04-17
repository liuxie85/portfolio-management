"""Cash flow CRUD mixin for FeishuStorage."""
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional

from ..models import CashFlow, make_cf_dedup_key, DATETIME_FORMAT


class CashFlowMixin:
    """Cash flow table operations + aggregation cache."""

    def add_cash_flow(self, cf: CashFlow) -> CashFlow:
        """添加出入金记录（自动防重）"""
        if not cf.dedup_key:
            cf.dedup_key = make_cf_dedup_key(cf)

        if cf.dedup_key:
            existing = self._find_by_dedup_key('cash_flow', cf.dedup_key)
            if existing:
                print(f"[防重保护] 发现相同内容出入金(dedup_key={cf.dedup_key})，跳过创建")
                cf.record_id = existing
                return cf

        fields = self._cash_flow_to_dict(cf)
        feishu_fields = self._to_feishu_fields(fields, 'cash_flow')

        try:
            result = self.client.create_record('cash_flow', feishu_fields)
        except Exception as e:
            if self._is_missing_field_error(e):
                raise ValueError("Feishu cash_flow 表缺少 dedup_key 等防重字段，已拒绝降级写入；请先补齐表字段") from e
            raise
        cf.record_id = result['record_id']

        if cf.dedup_key:
            self._dedup_key_cache[f"cash_flow:{cf.dedup_key}"] = cf.record_id

        # 增量更新本地 cash_flow 聚合缓存
        if cf.account in self._cash_flow_agg_loaded_accounts and cf.flow_date:
            from ..time_utils import bj_now_naive
            cny_amount = cf.cny_amount if cf.cny_amount is not None else cf.amount
            self._local_cash_flow_agg_cache.append_flow(
                cf.account,
                cf.flow_date,
                float(cny_amount or 0.0),
                cf.record_id,
                bj_now_naive().strftime(DATETIME_FORMAT),
            )
            self._cash_flow_agg_mem_cache[cf.account] = self._local_cash_flow_agg_cache.get_account(cf.account)

        return cf

    def get_cash_flow(self, record_id: str) -> Optional[CashFlow]:
        """获取单条出入金记录"""
        try:
            record = self.client.get_record_strict('cash_flow', record_id)
        except Exception:
            return None

        fields = self._from_feishu_fields(record['fields'], 'cash_flow')
        fields['record_id'] = record['record_id']
        return self._dict_to_cash_flow(fields)

    def preload_cash_flow_aggs(self, account: str, force_refresh: bool = False) -> Dict[str, Any]:
        """预加载并缓存 cash_flow 月度/年度聚合。"""
        if (not force_refresh) and (account in self._cash_flow_agg_loaded_accounts):
            cached = self._cash_flow_agg_mem_cache.get(account) or {}
            return {
                'account': account,
                'loaded': int(cached.get('flow_count', 0) or 0),
                'source': 'memory',
                'invalidated': False,
            }

        cached_local = self._local_cash_flow_agg_cache.get_account(account)

        filter_str = f'CurrentValue.[account] = "{self._escape_filter_value(account)}"'
        try:
            records = self.client.list_records(
                'cash_flow',
                filter_str=filter_str,
                field_names=self.CASH_FLOW_PROJECTION_FIELDS,
            )
        except Exception as e:
            if 'FieldNameNotFound' in str(e):
                fallback_fields = [f for f in self.CASH_FLOW_PROJECTION_FIELDS if f != 'updated_at']
                records = self.client.list_records(
                    'cash_flow',
                    filter_str=filter_str,
                    field_names=fallback_fields,
                )
            else:
                raise

        flows: List[Dict[str, Any]] = []
        daily: Dict[str, float] = {}
        monthly: Dict[str, float] = {}
        yearly: Dict[str, float] = {}
        cumulative = Decimal('0')

        for record in records:
            fields = self._from_feishu_fields(record.get('fields') or {}, 'cash_flow')
            cf = self._dict_to_cash_flow({**fields, 'record_id': record['record_id']})
            if not cf.flow_date:
                continue
            amount = cf.cny_amount if cf.cny_amount is not None else cf.amount
            amount_dec = self._to_decimal(amount or 0)
            amount_float = float(amount_dec)

            ds = cf.flow_date.strftime('%Y-%m-%d')
            ym = cf.flow_date.strftime('%Y-%m')
            yy = cf.flow_date.strftime('%Y')
            daily[ds] = float(self._to_decimal(daily.get(ds, 0.0)) + amount_dec)
            monthly[ym] = float(self._to_decimal(monthly.get(ym, 0.0)) + amount_dec)
            yearly[yy] = float(self._to_decimal(yearly.get(yy, 0.0)) + amount_dec)
            cumulative += amount_dec

            flows.append({
                'date': self._safe_date_str(cf.flow_date),
                'record_id': record['record_id'],
                'cny_amount': amount_float,
                'updated_at': self._extract_updated_at_str(record.get('fields') or {}),
            })

        flows.sort(key=lambda x: x.get('date') or '')
        last_record = dict(flows[-1]) if flows else None

        invalidated = False
        if cached_local:
            old_fp = {r.get('date'): (r.get('record_id'), r.get('updated_at')) for r in (cached_local.get('flows') or [])}
            new_fp = {r.get('date'): (r.get('record_id'), r.get('updated_at')) for r in flows}
            if old_fp != new_fp:
                invalidated = True

        payload = {
            'account': account,
            'daily': daily,
            'monthly': monthly,
            'yearly': yearly,
            'cumulative': float(cumulative),
            'flow_count': len(flows),
            'flows': flows,
            'last_record': last_record,
            'latest_updated_at': (last_record or {}).get('updated_at') if last_record else None,
        }

        self._cash_flow_agg_mem_cache[account] = payload
        self._cash_flow_agg_loaded_accounts.add(account)
        self._local_cash_flow_agg_cache.set_account(account, payload)

        return {'account': account, 'loaded': len(flows), 'source': 'feishu', 'invalidated': invalidated}

    def _ensure_cash_flow_aggs_loaded(self, account: str):
        if account in self._cash_flow_agg_loaded_accounts:
            return
        cached = self._local_cash_flow_agg_cache.get_account(account)
        if cached:
            self._cash_flow_agg_mem_cache[account] = cached
            self._cash_flow_agg_loaded_accounts.add(account)
            return
        self.preload_cash_flow_aggs(account)

    def get_cash_flow_aggs(self, account: str) -> Dict[str, Any]:
        self._ensure_cash_flow_aggs_loaded(account)
        return self._cash_flow_agg_mem_cache.get(account) or {}

    def get_cash_flows(self, account: Optional[str] = None,
                      start_date: Optional[date] = None,
                      end_date: Optional[date] = None) -> List[CashFlow]:
        """获取出入金记录列表（投影字段，降低 payload）。"""
        conditions = []

        if account:
            conditions.append(f'CurrentValue.[account] = "{self._escape_filter_value(account)}"')
        filter_str = ' AND '.join(conditions) if conditions else None
        try:
            records = self.client.list_records(
                'cash_flow',
                filter_str=filter_str,
                field_names=self.CASH_FLOW_PROJECTION_FIELDS,
            )
        except Exception as e:
            if 'FieldNameNotFound' in str(e):
                fallback_fields = [f for f in self.CASH_FLOW_PROJECTION_FIELDS if f != 'updated_at']
                records = self.client.list_records(
                    'cash_flow',
                    filter_str=filter_str,
                    field_names=fallback_fields,
                )
            else:
                raise

        cash_flows = []
        for record in records:
            fields = self._from_feishu_fields(record['fields'], 'cash_flow')
            fields['record_id'] = record['record_id']
            cf = self._dict_to_cash_flow(fields)
            if start_date and cf.flow_date and cf.flow_date < start_date:
                continue
            if end_date and cf.flow_date and cf.flow_date > end_date:
                continue
            cash_flows.append(cf)

        cash_flows.sort(key=lambda c: c.flow_date, reverse=True)
        return cash_flows

    def get_total_cash_flow_cny(self, account: str) -> float:
        """获取账户累计出入金总额(人民币)"""
        records = self.client.list_records(
            'cash_flow',
            filter_str=f'CurrentValue.[account] = "{self._escape_filter_value(account)}"'
        )

        total = Decimal('0')
        for record in records:
            fields = record['fields']
            cny_amount = fields.get('cny_amount', fields.get('amount', 0))
            if cny_amount is not None and cny_amount != '':
                total += self._to_decimal(cny_amount)

        return float(total)

    def _cash_flow_to_dict(self, cf: CashFlow) -> Dict:
        """CashFlow 转字典"""
        flow_type = str(cf.flow_type).upper() if cf.flow_type is not None else None
        result = {
            'flow_date': cf.flow_date,
            'account': cf.account,
            'amount': cf.amount,
            'currency': cf.currency,
            'cny_amount': cf.cny_amount,
            'exchange_rate': cf.exchange_rate,
            'flow_type': flow_type,
            'source': cf.source,
            'remark': cf.remark,
        }
        if cf.dedup_key:
            result['dedup_key'] = cf.dedup_key
        return result

    def _dict_to_cash_flow(self, data: Dict) -> CashFlow:
        """字典转 CashFlow"""
        flow_date = data.get('flow_date')
        if isinstance(flow_date, (int, float)):
            flow_date = datetime.fromtimestamp(flow_date / 1000, tz=self.FEISHU_DATE_TZ).date()
        elif isinstance(flow_date, str):
            flow_date = datetime.strptime(flow_date, '%Y-%m-%d').date()

        return CashFlow(
            record_id=data.get('record_id'),
            flow_date=flow_date,
            account=data.get('account', ''),
            amount=float(data.get('amount', 0)),
            currency=data.get('currency', 'CNY'),
            cny_amount=float(data.get('cny_amount')) if data.get('cny_amount') is not None else None,
            exchange_rate=float(data.get('exchange_rate')) if data.get('exchange_rate') is not None else None,
            flow_type=str(data.get('flow_type', 'DEPOSIT')).upper(),
            source=data.get('source'),
            remark=data.get('remark'),
        )

    def delete_cash_flow_by_record_id(self, record_id: str) -> bool:
        """通过记录ID删除出入金"""
        ok = self.client.delete_record('cash_flow', record_id)
        if ok:
            self._cash_flow_agg_loaded_accounts.clear()
            self._cash_flow_agg_mem_cache.clear()
        return ok
