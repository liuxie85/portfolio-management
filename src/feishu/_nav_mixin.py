"""NAV history CRUD mixin for FeishuStorage."""
import json
from datetime import date, datetime
from typing import Any, Dict, List, Optional

from ..models import NAVHistory, DATETIME_FORMAT


class NavMixin:
    """NAV history table operations + nav index cache."""

    NAV_INDEX_PROJECTION_FIELDS: List[str] = [
        'date', 'account', 'total_value', 'shares', 'nav',
        'cash_flow', 'pnl', 'mtd_nav_change', 'ytd_nav_change',
        'mtd_pnl', 'ytd_pnl', 'updated_at',
    ]

    CASH_FLOW_PROJECTION_FIELDS: List[str] = [
        'flow_date', 'account', 'amount', 'currency', 'cny_amount',
        'exchange_rate', 'flow_type', 'updated_at',
    ]

    NAV_DERIVED_PATCH_FIELDS = {
        'cash_flow',
        'share_change',
        'pnl',
        'mtd_nav_change',
        'ytd_nav_change',
        'mtd_pnl',
        'ytd_pnl',
    }

    def _build_nav_index_payload(self, account: str, records: List[Dict[str, any]]) -> Dict[str, any]:
        navs: List[NAVHistory] = []
        nav_records: List[Dict[str, any]] = []

        for record in records:
            raw_fields = record.get('fields') or {}
            fields = self._from_feishu_fields(raw_fields, 'nav_history')
            if account and not fields.get('account'):
                fields['account'] = account
            fields['record_id'] = record.get('record_id')
            nav = self._dict_to_nav(fields)
            if not nav.date:
                continue
            navs.append(nav)
            nav_records.append({
                'date': self._safe_date_str(nav.date),
                'record_id': nav.record_id,
                'total_value': nav.total_value,
                'shares': nav.shares,
                'nav': nav.nav,
                'cash_flow': nav.cash_flow,
                'pnl': nav.pnl,
                'mtd_nav_change': nav.mtd_nav_change,
                'ytd_nav_change': nav.ytd_nav_change,
                'mtd_pnl': nav.mtd_pnl,
                'ytd_pnl': nav.ytd_pnl,
                'updated_at': self._extract_updated_at_str(raw_fields),
            })

        nav_records.sort(key=lambda x: x.get('date') or '')
        navs.sort(key=lambda x: x.date)

        month_end_base: Dict[str, Dict[str, any]] = {}
        year_end_base: Dict[str, Dict[str, any]] = {}
        for row in nav_records:
            ds = row.get('date')
            if not ds:
                continue
            d = datetime.strptime(ds, '%Y-%m-%d').date()
            month_end_base[d.strftime('%Y-%m')] = dict(row)
            year_end_base[str(d.year)] = dict(row)

        inception_base = dict(nav_records[0]) if nav_records else None
        last_record = dict(nav_records[-1]) if nav_records else None

        return {
            'account': account,
            'record_count': len(nav_records),
            'nav_history': nav_records,
            'month_end_base': month_end_base,
            'year_end_base': year_end_base,
            'inception_base': inception_base,
            'last_record': last_record,
            'latest_updated_at': (last_record or {}).get('updated_at') if last_record else None,
            '_nav_objects': navs,
        }

    @staticmethod
    def _nav_index_fingerprint(payload: Dict[str, any]) -> Dict[str, tuple]:
        fp: Dict[str, tuple] = {}
        for row in payload.get('nav_history') or []:
            ds = row.get('date')
            if not ds:
                continue
            fp[ds] = (row.get('record_id'), row.get('updated_at'))
        return fp

    def preload_nav_index(self, account: str, force_refresh: bool = False) -> Dict[str, any]:
        """预加载并缓存 nav_history 索引（含 month/year/inception bases）。"""
        if (not force_refresh) and (account in self._nav_index_loaded_accounts):
            cached = self._nav_index_mem_cache.get(account) or {}
            return {
                'account': account,
                'loaded': int(cached.get('record_count', 0) or 0),
                'source': 'memory',
                'invalidated': False,
            }

        cached_local = self._local_nav_index_cache.get_account(account)

        filter_str = f'CurrentValue.[account] = "{self._escape_filter_value(account)}"'
        try:
            records = self.client.list_records(
                'nav_history',
                filter_str=filter_str,
                field_names=self.NAV_INDEX_PROJECTION_FIELDS,
            )
        except Exception as e:
            if 'FieldNameNotFound' in str(e):
                fallback_fields = [f for f in self.NAV_INDEX_PROJECTION_FIELDS if f != 'updated_at']
                records = self.client.list_records(
                    'nav_history',
                    filter_str=filter_str,
                    field_names=fallback_fields,
                )
            else:
                raise

        payload = self._build_nav_index_payload(account, records)
        invalidated = False

        if cached_local:
            missing_base = not cached_local.get('inception_base') or not cached_local.get('month_end_base') or not cached_local.get('year_end_base')
            if missing_base:
                invalidated = True
            else:
                old_fp = self._nav_index_fingerprint(cached_local)
                new_fp = self._nav_index_fingerprint(payload)
                if old_fp != new_fp:
                    invalidated = True

        self._nav_index_mem_cache[account] = payload
        self._nav_index_loaded_accounts.add(account)

        persist_payload = dict(payload)
        persist_payload.pop('_nav_objects', None)
        self._local_nav_index_cache.set_account(account, persist_payload)

        return {
            'account': account,
            'loaded': len(payload.get('nav_history') or []),
            'source': 'feishu',
            'invalidated': invalidated,
        }

    def _ensure_nav_index_loaded(self, account: str):
        if account in self._nav_index_loaded_accounts:
            return

        cached_local = self._local_nav_index_cache.get_account(account)
        if cached_local:
            navs: List[NAVHistory] = []
            for row in cached_local.get('nav_history') or []:
                ds = row.get('date')
                if not ds:
                    continue
                try:
                    d = datetime.strptime(ds[:10], '%Y-%m-%d').date()
                except Exception:
                    continue
                navs.append(NAVHistory(
                    record_id=row.get('record_id'),
                    date=d,
                    account=account,
                    total_value=float(row.get('total_value') or 0.0),
                    shares=float(row['shares']) if row.get('shares') is not None else None,
                    nav=float(row['nav']) if row.get('nav') is not None else None,
                    cash_flow=float(row['cash_flow']) if row.get('cash_flow') is not None else None,
                    pnl=float(row['pnl']) if row.get('pnl') is not None else None,
                    mtd_nav_change=float(row['mtd_nav_change']) if row.get('mtd_nav_change') is not None else None,
                    ytd_nav_change=float(row['ytd_nav_change']) if row.get('ytd_nav_change') is not None else None,
                    mtd_pnl=float(row['mtd_pnl']) if row.get('mtd_pnl') is not None else None,
                    ytd_pnl=float(row['ytd_pnl']) if row.get('ytd_pnl') is not None else None,
                ))

            payload = dict(cached_local)
            payload['_nav_objects'] = sorted(navs, key=lambda x: x.date)
            self._nav_index_mem_cache[account] = payload
            self._nav_index_loaded_accounts.add(account)
            return

        self.preload_nav_index(account)

    def get_nav_index(self, account: str) -> Dict[str, any]:
        self._ensure_nav_index_loaded(account)
        return self._nav_index_mem_cache.get(account) or {}

    def _get_indexed_navs(self, account: str) -> List[NAVHistory]:
        idx = self.get_nav_index(account)
        navs: List[NAVHistory] = list(idx.get('_nav_objects') or [])
        if navs:
            return navs

        self.preload_nav_index(account, force_refresh=True)
        idx = self.get_nav_index(account)
        return list(idx.get('_nav_objects') or [])

    def _invalidate_nav_index(self, account: str):
        self._nav_index_loaded_accounts.discard(account)
        self._nav_index_mem_cache.pop(account, None)

    def _normalize_nav_date(self, nav_date) -> date:
        if isinstance(nav_date, datetime):
            return nav_date.date()
        if isinstance(nav_date, str):
            return datetime.strptime(nav_date[:10], '%Y-%m-%d').date()
        return nav_date

    def _nav_to_index_row(self, nav: NAVHistory, updated_at: Optional[str] = None) -> Dict[str, any]:
        return {
            'date': self._safe_date_str(nav.date),
            'record_id': nav.record_id,
            'total_value': nav.total_value,
            'shares': nav.shares,
            'nav': nav.nav,
            'cash_flow': nav.cash_flow,
            'pnl': nav.pnl,
            'mtd_nav_change': nav.mtd_nav_change,
            'ytd_nav_change': nav.ytd_nav_change,
            'mtd_pnl': nav.mtd_pnl,
            'ytd_pnl': nav.ytd_pnl,
            'updated_at': updated_at,
        }

    def _apply_nav_rows_to_local_cache(self, account: str, rows: List[Dict[str, any]]):
        """增量更新本地 NAV 索引缓存，并失效内存镜像。"""
        if not rows:
            return
        self._local_nav_index_cache.upsert_nav_records(account, rows, _flush=True)
        self._invalidate_nav_index(account)

    def _validate_nav_write(self, nav: NAVHistory):
        """Validate a full nav record write before persisting."""
        nav.date = self._normalize_nav_date(nav.date)

        if not getattr(nav, 'account', None):
            raise ValueError('nav_history write validation failed: account is required')
        if not getattr(nav, 'date', None):
            raise ValueError('nav_history write validation failed: date is required')

        if nav.total_value is None:
            raise ValueError('nav_history write validation failed: total_value is required')
        try:
            tv = float(nav.total_value)
        except Exception:
            raise ValueError('nav_history write validation failed: total_value must be a number')
        if tv <= 0:
            raise ValueError('nav_history write validation failed: total_value must be > 0')

        details = getattr(nav, 'details', None)
        status = None
        if isinstance(details, dict):
            status = (details.get('status') or '').upper()

        if status == 'CLOSED':
            if nav.shares is None:
                raise ValueError('nav_history write validation failed: shares is required when status=CLOSED')
            try:
                if float(nav.shares) != 0.0:
                    raise ValueError('nav_history write validation failed: shares must be 0 when status=CLOSED')
            except ValueError:
                raise
            except Exception:
                raise ValueError('nav_history write validation failed: shares must be a number when status=CLOSED')
            return

        if nav.shares is None:
            raise ValueError('nav_history write validation failed: shares is required')
        if nav.nav is None:
            raise ValueError('nav_history write validation failed: nav is required')
        try:
            if float(nav.shares) <= 0:
                raise ValueError('nav_history write validation failed: shares must be > 0')
        except ValueError:
            raise
        except Exception:
            raise ValueError('nav_history write validation failed: shares must be a number')
        try:
            if float(nav.nav) <= 0:
                raise ValueError('nav_history write validation failed: nav must be > 0')
        except ValueError:
            raise
        except Exception:
            raise ValueError('nav_history write validation failed: nav must be a number')

    def _execute_single_nav_write(self, nav: NAVHistory, existing_row: Optional[Dict[str, Any]], preserve_none_for_update: bool, dry_run: bool = False) -> Dict[str, Any]:
        """Execute one full nav write with the same semantics as bulk replace/upsert."""
        existing_record_id = (existing_row or {}).get('record_id')
        fields = self._nav_to_dict(nav)
        feishu_fields = self._to_feishu_fields(
            fields,
            'nav_history',
            preserve_none=bool(existing_record_id and preserve_none_for_update),
        )

        if dry_run:
            return {
                'existing': bool(existing_record_id),
                'record_id': existing_record_id,
                'fields': feishu_fields,
                'cache_row': self._nav_to_index_row(nav, updated_at=feishu_fields.get('updated_at')),
            }

        used_fields = feishu_fields
        try:
            if existing_record_id:
                self.client.update_record('nav_history', existing_record_id, feishu_fields)
                nav.record_id = existing_record_id
            else:
                result = self.client.create_record('nav_history', feishu_fields)
                nav.record_id = result['record_id']
        except Exception as e:
            msg = str(e)
            if 'FieldNameNotFound' not in msg:
                raise

            fallback_fields = dict(feishu_fields)
            fallback_fields.pop('details', None)
            used_fields = fallback_fields

            if existing_record_id:
                self.client.update_record('nav_history', existing_record_id, fallback_fields)
                nav.record_id = existing_record_id
            else:
                result = self.client.create_record('nav_history', fallback_fields)
                nav.record_id = result['record_id']

        cache_row = self._nav_to_index_row(nav, updated_at=used_fields.get('updated_at'))
        if existing_record_id and (not preserve_none_for_update):
            existing_cache_row = dict(existing_row or {})
            merged_row = dict(existing_cache_row)
            merged_row.update(cache_row)
            for k, v in list(merged_row.items()):
                if v is None and k in existing_cache_row:
                    merged_row[k] = existing_cache_row.get(k)
            cache_row = merged_row
        return {
            'existing': bool(existing_record_id),
            'record_id': nav.record_id,
            'fields': used_fields,
            'cache_row': cache_row,
        }

    def _write_nav_full_records(
        self,
        nav_list: List[NAVHistory],
        *,
        mode: str = 'replace',
        allow_partial: bool = False,
        dry_run: bool = False,
        use_batch_api: bool = True,
    ) -> Dict[str, Any]:
        """Unified full-record nav writer used by both single and bulk APIs."""
        if mode not in ('replace', 'upsert'):
            raise ValueError("mode must be 'replace' or 'upsert'")

        if not nav_list:
            return {
                'mode': mode, 'total': 0, 'updated': 0, 'created': 0,
                'preloaded_accounts': [], 'accounts': {}, 'errors': [], 'dry_run': dry_run,
            }

        grouped: Dict[str, List[NAVHistory]] = {}
        for nav in nav_list:
            if not nav:
                continue
            self._validate_nav_write(nav)
            grouped.setdefault(nav.account, []).append(nav)

        total_updated = 0
        total_created = 0
        preloaded_accounts: List[str] = []
        errors: List[Dict[str, Any]] = []
        account_results: Dict[str, Dict[str, Any]] = {}
        previews: List[Dict[str, Any]] = []

        for account in sorted(grouped.keys()):
            navs_raw = grouped.get(account) or []
            by_date_nav: Dict[str, NAVHistory] = {}
            for n in navs_raw:
                by_date_nav[self._safe_date_str(n.date)] = n
            navs = [by_date_nav[d] for d in sorted(by_date_nav.keys())]

            try:
                self.preload_nav_index(account, force_refresh=True)
                preloaded_accounts.append(account)
                idx = self.get_nav_index(account)
                existing_row_by_date: Dict[str, Dict[str, Any]] = {}
                for row in idx.get('nav_history') or []:
                    ds = str((row or {}).get('date') or '')
                    if ds:
                        existing_row_by_date[ds] = dict(row or {})

                preserve_none_for_update = (mode == 'replace')

                if use_batch_api:
                    update_payloads: List[Dict[str, Any]] = []
                    update_rows_for_cache: List[Dict[str, Any]] = []
                    create_payloads: List[Dict[str, Any]] = []
                    create_rows_for_cache: List[Dict[str, Any]] = []
                    created_navs: List[NAVHistory] = []

                    for nav in sorted(navs, key=lambda x: x.date):
                        ds = self._safe_date_str(nav.date)
                        existing_row = existing_row_by_date.get(ds)
                        rid = (existing_row or {}).get('record_id')
                        fields = self._nav_to_dict(nav)
                        if rid:
                            feishu_fields = self._to_feishu_fields(fields, 'nav_history', preserve_none=preserve_none_for_update)
                            update_payloads.append({'record_id': rid, 'fields': feishu_fields})
                            nav.record_id = rid

                            merged_row = dict(existing_row or {})
                            merged_row.update(self._nav_to_index_row(nav, updated_at=feishu_fields.get('updated_at')))
                            if not preserve_none_for_update:
                                for k, v in list(merged_row.items()):
                                    if v is None and k in (existing_row or {}):
                                        merged_row[k] = existing_row.get(k)
                            update_rows_for_cache.append(merged_row)
                            if dry_run:
                                previews.append({'account': account, 'date': ds, 'existing': True, 'fields': feishu_fields})
                        else:
                            feishu_fields = self._to_feishu_fields(fields, 'nav_history', preserve_none=False)
                            create_payloads.append({'fields': feishu_fields})
                            create_rows_for_cache.append(self._nav_to_index_row(nav, updated_at=feishu_fields.get('updated_at')))
                            created_navs.append(nav)
                            if dry_run:
                                previews.append({'account': account, 'date': ds, 'existing': False, 'fields': feishu_fields})

                    if not dry_run:
                        if update_payloads:
                            try:
                                self.client.batch_update_records('nav_history', update_payloads)
                            except Exception as e:
                                msg = str(e)
                                if 'FieldNameNotFound' not in msg:
                                    raise
                                fallback_updates = []
                                fallback_rows = []
                                for p, row in zip(update_payloads, update_rows_for_cache):
                                    f = dict(p.get('fields') or {})
                                    f.pop('details', None)
                                    fallback_updates.append({'record_id': p['record_id'], 'fields': f})
                                    r = dict(row)
                                    r['updated_at'] = f.get('updated_at')
                                    fallback_rows.append(r)
                                self.client.batch_update_records('nav_history', fallback_updates)
                                update_rows_for_cache = fallback_rows

                        if create_payloads:
                            try:
                                created = self.client.batch_create_records('nav_history', create_payloads)
                            except Exception as e:
                                msg = str(e)
                                if 'FieldNameNotFound' not in msg:
                                    raise
                                fallback_creates = []
                                for p in create_payloads:
                                    f = dict((p.get('fields') or {}))
                                    f.pop('details', None)
                                    fallback_creates.append({'fields': f})
                                created = self.client.batch_create_records('nav_history', fallback_creates)
                                for i, p in enumerate(fallback_creates):
                                    if i < len(create_rows_for_cache):
                                        create_rows_for_cache[i]['updated_at'] = (p.get('fields') or {}).get('updated_at')

                            for i, nav in enumerate(created_navs):
                                rec = created[i] if i < len(created) else {}
                                rid = rec.get('record_id') or ((rec.get('record') or {}).get('record_id') if isinstance(rec, dict) else None)
                                nav.record_id = rid
                                if i < len(create_rows_for_cache):
                                    create_rows_for_cache[i]['record_id'] = rid

                        all_rows = []
                        all_rows.extend(update_rows_for_cache)
                        all_rows.extend(create_rows_for_cache)
                        if all_rows:
                            self._apply_nav_rows_to_local_cache(account, all_rows)

                    updated_n = len(update_payloads)
                    created_n = len(create_payloads)
                else:
                    account_rows_for_cache: List[Dict[str, Any]] = []
                    updated_n = 0
                    created_n = 0
                    for nav in sorted(navs, key=lambda x: x.date):
                        ds = self._safe_date_str(nav.date)
                        existing_row = existing_row_by_date.get(ds)
                        outcome = self._execute_single_nav_write(nav, existing_row, preserve_none_for_update, dry_run=dry_run)
                        previews.append({
                            'account': account,
                            'date': ds,
                            'existing': outcome['existing'],
                            'fields': outcome['fields'],
                            'existing_row': dict(existing_row or {}),
                        })
                        if outcome['existing']:
                            updated_n += 1
                        else:
                            created_n += 1
                        if not dry_run:
                            account_rows_for_cache.append(outcome['cache_row'])
                    if account_rows_for_cache:
                        self._apply_nav_rows_to_local_cache(account, account_rows_for_cache)

                total_updated += updated_n
                total_created += created_n
                account_results[account] = {
                    'updated': updated_n,
                    'created': created_n,
                    'total': len(navs),
                }
            except Exception as e:
                err = {'account': account, 'error': str(e), 'count': len(navs)}
                errors.append(err)
                if not allow_partial:
                    raise
                account_results[account] = {
                    'updated': 0, 'created': 0, 'total': len(navs), 'error': str(e),
                }

        return {
            'mode': mode,
            'total': len(nav_list),
            'updated': total_updated,
            'created': total_created,
            'preloaded_accounts': preloaded_accounts,
            'accounts': account_results,
            'errors': errors,
            'dry_run': dry_run,
            'previews': previews,
        }

    def _write_one_nav_record(self, nav: NAVHistory, overwrite_existing: bool = True, dry_run: bool = False):
        preview_result = self._write_nav_full_records(
            [nav],
            mode='replace',
            allow_partial=False,
            dry_run=True,
            use_batch_api=False,
        )
        preview = (preview_result.get('previews') or [{}])[0]
        if preview.get('existing') and not overwrite_existing:
            raise ValueError(f"nav_history 已存在同日记录，拒绝覆盖: account={nav.account}, date={nav.date}")
        if dry_run:
            return {"existing": bool(preview.get('existing')), "fields": preview.get('fields')}
        outcome = self._execute_single_nav_write(
            nav,
            preview.get('existing_row') or None,
            preserve_none_for_update=True,
            dry_run=False,
        )
        self._apply_nav_rows_to_local_cache(nav.account, [outcome['cache_row']])
        return

    def write_nav_record(self, nav: NAVHistory, overwrite_existing: bool = True, dry_run: bool = False):
        """Public semantic alias for writing one full nav record."""
        return self._write_one_nav_record(nav, overwrite_existing=overwrite_existing, dry_run=dry_run)

    def write_nav_records(
        self,
        nav_list: List[NAVHistory],
        mode: str = 'replace',
        allow_partial: bool = False,
        dry_run: bool = False,
    ) -> Dict[str, any]:
        """Public semantic alias for writing full nav records in bulk."""
        result = self._write_nav_full_records(
            nav_list,
            mode=mode,
            allow_partial=allow_partial,
            dry_run=dry_run,
            use_batch_api=not dry_run,
        )
        if not dry_run:
            result.pop('previews', None)
            result.pop('dry_run', None)
        return result

    def get_nav_history(self, account: str, days: int = 365) -> List[NAVHistory]:
        """获取净值历史（优先本地预加载索引）。"""
        from datetime import timedelta
        from ..time_utils import bj_today
        start_date = bj_today() - timedelta(days=days)

        self.preload_nav_index(account)
        idx = self.get_nav_index(account)
        navs: List[NAVHistory] = list(idx.get('_nav_objects') or [])
        if not navs:
            self.preload_nav_index(account, force_refresh=True)
            idx = self.get_nav_index(account)
            navs = list(idx.get('_nav_objects') or [])

        filtered = [n for n in navs if n.date and n.date >= start_date]
        filtered.sort(key=lambda n: n.date)
        return filtered

    def get_latest_nav(self, account: str) -> Optional[NAVHistory]:
        """获取最新净值记录（优先索引）。"""
        navs = self._get_indexed_navs(account)
        return navs[-1] if navs else None

    def get_nav_on_date(self, account: str, nav_date: date) -> Optional[NAVHistory]:
        """获取指定日期的净值记录。"""
        if isinstance(nav_date, datetime):
            nav_date = nav_date.date()
        elif isinstance(nav_date, str):
            nav_date = datetime.strptime(nav_date[:10], '%Y-%m-%d').date()

        navs = self._get_indexed_navs(account)
        matches = [n for n in navs if n.date == nav_date]

        if len(matches) > 1:
            print(f"[警告] nav_history 存在重复日期记录: account={account}, date={nav_date}, count={len(matches)}")

        return matches[0] if matches else None

    def _patch_nav_fields(
        self,
        record_id: str,
        fields: Dict[str, any],
        dry_run: bool = False,
        allowed_fields: Optional[set] = None,
    ):
        if allowed_fields is not None:
            illegal = [k for k in fields.keys() if k not in allowed_fields]
            if illegal:
                raise ValueError(f"update_nav_fields: illegal field(s): {illegal}. allowed={sorted(list(allowed_fields))}")

        normalized = {}
        for k, v in fields.items():
            if k in ('mtd_nav_change', 'ytd_nav_change') and v is not None:
                normalized[k] = self._quantize_nav(v)
            elif k in ('mtd_pnl', 'ytd_pnl', 'pnl', 'cash_flow', 'share_change') and v is not None:
                normalized[k] = self._quantize_money(v)
            else:
                normalized[k] = v

        feishu_fields = self._to_feishu_fields(normalized, 'nav_history', preserve_none=True)
        if dry_run:
            return {"record_id": record_id, "fields": feishu_fields}
        self.client.update_record('nav_history', record_id, feishu_fields)
        self._nav_index_loaded_accounts.clear()
        self._nav_index_mem_cache.clear()
        return {"record_id": record_id, "fields": feishu_fields}

    def patch_nav_derived_fields(self, record_id: str, fields: Dict[str, any], dry_run: bool = False):
        """Patch only derived nav fields with an explicit allowlist."""
        return self._patch_nav_fields(
            record_id,
            fields,
            dry_run=dry_run,
            allowed_fields=self.NAV_DERIVED_PATCH_FIELDS,
        )

    def get_latest_nav_before(self, account: str, before_date: date) -> Optional[NAVHistory]:
        """获取指定日期之前的最新净值记录（优先索引）。"""
        navs = self._get_indexed_navs(account)
        candidates = [n for n in navs if n.date and n.date < before_date]
        candidates.sort(key=lambda n: n.date, reverse=True)
        return candidates[0] if candidates else None

    def get_total_shares(self, account: str) -> float:
        """获取账户总份额"""
        latest = self.get_latest_nav(account)
        return latest.shares if latest else 0.0

    def _nav_to_dict(self, nav: NAVHistory) -> Dict:
        """NAVHistory 转字典"""
        return {
            'date': nav.date,
            'account': nav.account,
            'total_value': nav.total_value,
            'cash_value': nav.cash_value,
            'stock_value': nav.stock_value,
            'fund_value': nav.fund_value,
            'cn_stock_value': nav.cn_stock_value,
            'us_stock_value': nav.us_stock_value,
            'hk_stock_value': nav.hk_stock_value,
            'stock_weight': nav.stock_weight,
            'cash_weight': nav.cash_weight,
            'shares': nav.shares,
            'nav': nav.nav,
            'cash_flow': nav.cash_flow,
            'share_change': nav.share_change,
            'mtd_nav_change': nav.mtd_nav_change,
            'ytd_nav_change': nav.ytd_nav_change,
            'pnl': nav.pnl,
            'mtd_pnl': nav.mtd_pnl,
            'ytd_pnl': nav.ytd_pnl,
            'details': nav.details,
        }

    def _dict_to_nav(self, data: Dict) -> NAVHistory:
        """字典转 NAVHistory"""
        nav_date = data.get('date')
        if isinstance(nav_date, (int, float)):
            nav_date = datetime.fromtimestamp(nav_date / 1000, tz=self.FEISHU_DATE_TZ).date()
        elif isinstance(nav_date, str):
            nav_date = datetime.strptime(nav_date[:10], '%Y-%m-%d').date()

        def _opt_float(key):
            v = data.get(key)
            if v is None:
                return None
            return self._parse_float(v)

        return NAVHistory(
            date=nav_date,
            record_id=data.get('record_id'),
            account=data.get('account', ''),
            total_value=self._parse_float(data.get('total_value')) or 0.0,
            cash_value=_opt_float('cash_value'),
            stock_value=_opt_float('stock_value'),
            fund_value=_opt_float('fund_value'),
            cn_stock_value=_opt_float('cn_stock_value'),
            us_stock_value=_opt_float('us_stock_value'),
            hk_stock_value=_opt_float('hk_stock_value'),
            stock_weight=_opt_float('stock_weight'),
            cash_weight=_opt_float('cash_weight'),
            shares=_opt_float('shares'),
            nav=_opt_float('nav'),
            cash_flow=_opt_float('cash_flow'),
            share_change=_opt_float('share_change'),
            mtd_nav_change=_opt_float('mtd_nav_change'),
            ytd_nav_change=_opt_float('ytd_nav_change'),
            pnl=_opt_float('pnl'),
            mtd_pnl=_opt_float('mtd_pnl'),
            ytd_pnl=_opt_float('ytd_pnl'),
            details=data.get('details')
        )

    def delete_nav_by_record_id(self, record_id: str) -> bool:
        """通过记录ID删除净值记录"""
        ok = self.client.delete_record('nav_history', record_id)
        if ok:
            self._nav_index_loaded_accounts.clear()
            self._nav_index_mem_cache.clear()
        return ok
