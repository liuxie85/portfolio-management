"""Holdings CRUD mixin for FeishuStorage."""
import json
from datetime import datetime
from typing import Any, Dict, List, Optional

from ..models import (
    Holding, AssetType, AssetClass, Industry, DATETIME_FORMAT,
)


class HoldingsMixin:
    """Holdings table operations + in-memory / persistent cache."""

    def _get_holding_cache_key(self, asset_id: str, account: str, market: Optional[str]) -> str:
        """生成持仓缓存 key"""
        return f"{asset_id}:{account}:{market or ''}"

    HOLDING_PROJECTION_FIELDS: List[str] = [
        'asset_id', 'asset_name', 'asset_type', 'account', 'market',
        'quantity', 'avg_cost', 'currency', 'asset_class', 'industry', 'tag',
        'created_at', 'updated_at'
    ]

    def _snapshot_for_persistent_cache(self, holding: Holding) -> Dict[str, any]:
        return {
            'record_id': holding.record_id,
            'asset_id': holding.asset_id,
            'asset_name': holding.asset_name,
            'asset_type': holding.asset_type.value if holding.asset_type else None,
            'market': holding.market or '',
            'account': holding.account,
            'quantity': holding.quantity,
            'avg_cost': holding.avg_cost,
            'currency': holding.currency,
            'asset_class': holding.asset_class.value if holding.asset_class else None,
            'industry': holding.industry.value if holding.industry else None,
            'tag': holding.tag,
            'created_at': holding.created_at.strftime(DATETIME_FORMAT) if holding.created_at else None,
            'updated_at': holding.updated_at.strftime(DATETIME_FORMAT) if holding.updated_at else None,
        }

    def _load_persistent_holdings_index(self):
        """启动时从本地缓存恢复持仓索引到内存。"""
        entries = self._local_holdings_index_cache.load_all()
        if not entries:
            return
        for bk, fields in entries.items():
            if not fields or not fields.get('record_id'):
                continue
            asset_id = fields.get('asset_id', '')
            account = fields.get('account', '')
            market = fields.get('market') or ''
            cache_key = self._get_holding_cache_key(asset_id, account, market or None)
            self._holding_id_cache[cache_key] = fields['record_id']

            mem_fields = dict(fields)
            mem_fields.setdefault('asset_name', '')
            mem_fields.setdefault('asset_type', None)
            mem_fields.setdefault('quantity', 0)
            mem_fields.setdefault('avg_cost', None)
            mem_fields.setdefault('currency', 'CNY')
            mem_fields.setdefault('asset_class', None)
            mem_fields.setdefault('industry', None)
            mem_fields.setdefault('tag', [])
            mem_fields.setdefault('created_at', None)
            mem_fields.setdefault('updated_at', None)
            self._holding_fields_cache[cache_key] = mem_fields

    def _flush_persistent_holdings_index(self):
        """将内存持仓索引刷写到本地缓存。"""
        self._local_holdings_index_cache.flush()

    def _invalidate_holding_cache_by_record_id(self, record_id: str, *, flush_persistent: bool = False):
        """通过 record_id 失效持仓缓存。"""
        keys_to_delete = [k for k, rid in self._holding_id_cache.items() if rid == record_id]
        for k in keys_to_delete:
            self._holding_id_cache.pop(k, None)
            self._holding_fields_cache.pop(k, None)
            self._local_holdings_index_cache.delete(k)
        if flush_persistent:
            self._flush_persistent_holdings_index()

    def _invalidate_holding_cache(self, asset_id: str, account: str, market: Optional[str], *, flush_persistent: bool = False):
        cache_key = self._get_holding_cache_key(asset_id, account, market)
        self._holding_id_cache.pop(cache_key, None)
        self._holding_fields_cache.pop(cache_key, None)
        self._local_holdings_index_cache.delete(cache_key)
        if flush_persistent:
            self._flush_persistent_holdings_index()

    def _put_holding_cache(self, holding: Holding, *, flush_persistent: bool = False):
        """Store holding into all cache layers (memory + persistent)."""
        if not holding.record_id:
            return

        cache_key = self._get_holding_cache_key(holding.asset_id, holding.account, holding.market)
        self._holding_id_cache[cache_key] = holding.record_id
        self._holding_fields_cache[cache_key] = {
            'record_id': holding.record_id,
            'asset_id': holding.asset_id,
            'asset_name': holding.asset_name,
            'asset_type': holding.asset_type.value if holding.asset_type else None,
            'market': holding.market or '',
            'account': holding.account,
            'quantity': holding.quantity,
            'avg_cost': holding.avg_cost,
            'currency': holding.currency,
            'asset_class': holding.asset_class.value if holding.asset_class else None,
            'industry': holding.industry.value if holding.industry else None,
            'tag': holding.tag,
            'created_at': holding.created_at.strftime(DATETIME_FORMAT) if holding.created_at else None,
            'updated_at': holding.updated_at.strftime(DATETIME_FORMAT) if holding.updated_at else None,
        }

        self._local_holdings_index_cache.upsert(
            cache_key,
            self._snapshot_for_persistent_cache(holding),
            _flush=flush_persistent,
        )

    def _get_holding_from_cache(self, asset_id: str, account: str, market: Optional[str]) -> Optional[Holding]:
        cache_key = self._get_holding_cache_key(asset_id, account, market)
        fields = self._holding_fields_cache.get(cache_key)
        if not fields:
            return None
        return self._dict_to_holding(fields)

    def _get_holding_from_cache_any_market(self, asset_id: str, account: str) -> Optional[Holding]:
        prefix = f"{asset_id}:{account}:"
        best = None
        for k, fields in self._holding_fields_cache.items():
            if k.startswith(prefix):
                h = self._dict_to_holding(fields)
                if best is None:
                    best = h
                elif not (h.market or ''):
                    best = h
                    break
        return best

    def preload_holdings_index(self, account: Optional[str] = None) -> Dict[str, any]:
        """预加载持仓索引到内存和本地缓存。"""
        conditions = []
        if account:
            conditions.append(f'CurrentValue.[account] = "{self._escape_filter_value(account)}"')
        filter_str = ' AND '.join(conditions) if conditions else None
        records = self.client.list_records(
            'holdings',
            filter_str=filter_str,
            field_names=self.HOLDING_PROJECTION_FIELDS,
        )

        count = 0
        for record in records:
            fields = self._from_feishu_fields(record.get('fields') or {}, 'holdings')
            fields['record_id'] = record['record_id']
            holding = self._dict_to_holding(fields)
            self._put_holding_cache(holding)
            count += 1

        if account:
            self._holdings_index_loaded_accounts.add(account)
        else:
            self._holdings_index_loaded_all = True

        self._flush_persistent_holdings_index()

        return {
            'account': account or 'all',
            'loaded': count,
            'source': 'feishu',
        }

    # ========== holdings CRUD ==========

    def get_holding(self, asset_id: str, account: str, market: Optional[str] = None) -> Optional[Holding]:
        """获取单个持仓（优先使用内存索引与快照）"""
        cached_holding = self._get_holding_from_cache(asset_id, account, market)
        if not cached_holding and market is None:
            cached_holding = self._get_holding_from_cache_any_market(asset_id, account)
        if cached_holding:
            return cached_holding

        if account and (not self._holdings_index_loaded_all) and (account not in self._holdings_index_loaded_accounts):
            self.preload_holdings_index(account=account)
            cached_holding = self._get_holding_from_cache(asset_id, account, market)
            if not cached_holding and market is None:
                cached_holding = self._get_holding_from_cache_any_market(asset_id, account)
            if cached_holding:
                return cached_holding

        if self._holdings_index_loaded_all or (account in self._holdings_index_loaded_accounts):
            return None

        if market:
            filter_str = (
                f'CurrentValue.[asset_id] = "{self._escape_filter_value(asset_id)}" '
                f'AND CurrentValue.[account] = "{self._escape_filter_value(account)}" '
                f'AND CurrentValue.[market] = "{self._escape_filter_value(market)}"'
            )
        else:
            filter_str = (
                f'CurrentValue.[asset_id] = "{self._escape_filter_value(asset_id)}" '
                f'AND CurrentValue.[account] = "{self._escape_filter_value(account)}"'
            )

        records = self.client.list_records(
            'holdings',
            filter_str=filter_str,
            field_names=self.HOLDING_PROJECTION_FIELDS,
        )
        if not records:
            return None

        selected = records[0]
        if not market:
            for record in records:
                if not (record.get('fields') or {}).get('market'):
                    selected = record
                    break

        fields = self._from_feishu_fields(selected.get('fields') or {}, 'holdings')
        fields['record_id'] = selected['record_id']
        holding = self._dict_to_holding(fields)
        self._put_holding_cache(holding)

        if market is None and holding.market:
            default_key = self._get_holding_cache_key(asset_id, account, None)
            self._holding_id_cache[default_key] = holding.record_id
            self._holding_fields_cache[default_key] = dict(self._holding_fields_cache[self._get_holding_cache_key(asset_id, account, holding.market)])

        return holding

    def get_holdings(self, account: Optional[str] = None, asset_type: Optional[str] = None, include_empty: bool = False) -> List[Holding]:
        """获取持仓列表（优先使用内存缓存索引）"""
        # 当缓存已加载且无 asset_type 过滤时，直接从缓存返回
        cache_hit = (
            not asset_type
            and (
                self._holdings_index_loaded_all
                or (account and account in self._holdings_index_loaded_accounts)
            )
        )
        if cache_hit:
            holdings = []
            for cache_key, fields in self._holding_fields_cache.items():
                if account and fields.get('account') != account:
                    continue
                holding = self._dict_to_holding(fields)
                if not include_empty and holding.quantity <= 0:
                    continue
                holdings.append(holding)
            holdings.sort(key=lambda h: (h.asset_type.value if h.asset_type else '', h.asset_id))
            return holdings

        conditions = []
        if account:
            conditions.append(f'CurrentValue.[account] = "{self._escape_filter_value(account)}"')
        if asset_type:
            conditions.append(f'CurrentValue.[asset_type] = "{self._escape_filter_value(asset_type)}"')
        filter_str = ' AND '.join(conditions) if conditions else None
        records = self.client.list_records(
            'holdings',
            filter_str=filter_str,
            field_names=self.HOLDING_PROJECTION_FIELDS,
        )

        holdings = []
        for record in records:
            fields = self._from_feishu_fields(record.get('fields') or {}, 'holdings')
            fields['record_id'] = record['record_id']
            holding = self._dict_to_holding(fields)
            self._put_holding_cache(holding)
            if not include_empty and holding.quantity <= 0:
                continue
            holdings.append(holding)

        if not asset_type:
            if account:
                self._holdings_index_loaded_accounts.add(account)
            else:
                self._holdings_index_loaded_all = True

        holdings.sort(key=lambda h: (h.asset_type.value if h.asset_type else '', h.asset_id))
        return holdings

    def upsert_holding(self, holding: Holding) -> Holding:
        """插入或更新持仓（优先使用预加载索引与内存快照）"""
        from ..time_utils import bj_now_naive

        now = bj_now_naive()
        existing = self.get_holding(holding.asset_id, holding.account, holding.market)

        if existing and existing.record_id:
            is_cash_like = (existing.asset_type and existing.asset_type.value in ('cash', 'mmf'))
            new_quantity = (
                self._quantize_money(existing.quantity + holding.quantity)
                if is_cash_like else (existing.quantity + holding.quantity)
            )
            update_fields = {
                'quantity': new_quantity,
                'updated_at': now.strftime(DATETIME_FORMAT)
            }

            new_name = holding.asset_name or existing.asset_name
            if new_name and len(new_name) > len(existing.asset_name or ''):
                update_fields['asset_name'] = new_name
                print(f"[持仓名称更新] {existing.asset_name} -> {new_name}")

            try:
                self.client.update_record('holdings', existing.record_id, update_fields)
            except Exception:
                self._invalidate_holding_cache(holding.asset_id, holding.account, holding.market, flush_persistent=True)
                raise

            existing.quantity = new_quantity
            existing.updated_at = now
            if 'asset_name' in update_fields:
                existing.asset_name = update_fields['asset_name']

            holding.record_id = existing.record_id
            holding.updated_at = now
            self._put_holding_cache(existing)
            return holding

        holding.created_at = now
        holding.updated_at = now
        fields = self._holding_to_dict(holding)
        feishu_fields = self._to_feishu_fields(fields, 'holdings')
        result = self.client.create_record('holdings', feishu_fields)
        holding.record_id = result['record_id']
        self._put_holding_cache(holding)
        return holding

    def upsert_holdings_bulk(self, holdings: List[Holding], mode: str = 'additive') -> Dict[str, any]:
        """批量 upsert 持仓，减少 HTTP 调用。"""
        from ..time_utils import bj_now_naive

        if mode not in ('additive', 'replace'):
            raise ValueError(f"unsupported mode={mode}, expected 'additive' or 'replace'")

        if not holdings:
            return {'mode': mode, 'updated': 0, 'created': 0, 'preloaded_accounts': []}

        preloaded_accounts: List[str] = []
        if mode == 'additive':
            accounts_to_preload = set()
            for h in holdings:
                cache_key = self._get_holding_cache_key(h.asset_id, h.account, h.market)
                has_cache = cache_key in self._holding_fields_cache
                if (not has_cache) and h.account and (not self._holdings_index_loaded_all) and (h.account not in self._holdings_index_loaded_accounts):
                    accounts_to_preload.add(h.account)
            for account in sorted(accounts_to_preload):
                self.preload_holdings_index(account=account)
                preloaded_accounts.append(account)

        now = bj_now_naive()
        now_str = now.strftime(DATETIME_FORMAT)

        update_payloads: List[Dict[str, any]] = []
        update_targets: List[Holding] = []
        create_payloads: List[Dict[str, any]] = []
        create_targets: List[Holding] = []

        working_existing: Dict[str, Holding] = {}

        for incoming in holdings:
            cache_key = self._get_holding_cache_key(incoming.asset_id, incoming.account, incoming.market)
            existing = working_existing.get(cache_key)
            if existing is None:
                existing = self.get_holding(incoming.asset_id, incoming.account, incoming.market)
                if existing:
                    working_existing[cache_key] = Holding(**existing.model_dump())
                    existing = working_existing[cache_key]

            if existing and existing.record_id:
                if mode == 'replace':
                    new_quantity = incoming.quantity
                else:
                    is_cash_like = (existing.asset_type and existing.asset_type.value in ('cash', 'mmf'))
                    new_quantity = (
                        self._quantize_money(existing.quantity + incoming.quantity)
                        if is_cash_like else (existing.quantity + incoming.quantity)
                    )

                update_fields = {
                    'quantity': new_quantity,
                    'updated_at': now_str,
                }
                new_name = incoming.asset_name or existing.asset_name
                if new_name and len(new_name) > len(existing.asset_name or ''):
                    update_fields['asset_name'] = new_name

                update_payloads.append({'record_id': existing.record_id, 'fields': update_fields})

                existing.quantity = new_quantity
                existing.updated_at = now
                if 'asset_name' in update_fields:
                    existing.asset_name = update_fields['asset_name']
                update_targets.append(Holding(**existing.model_dump()))
            else:
                new_holding = Holding(**incoming.model_dump())
                new_holding.created_at = now
                new_holding.updated_at = now
                fields = self._holding_to_dict(new_holding)
                feishu_fields = self._to_feishu_fields(fields, 'holdings')
                create_payloads.append({'fields': feishu_fields})
                create_targets.append(new_holding)

        if update_payloads:
            try:
                self.client.batch_update_records('holdings', update_payloads)
            except Exception:
                for h in update_targets:
                    self._invalidate_holding_cache(h.asset_id, h.account, h.market)
                self._flush_persistent_holdings_index()
                raise
            for h in update_targets:
                self._put_holding_cache(h)

        if create_payloads:
            created_records = self.client.batch_create_records('holdings', create_payloads)
            for idx, h in enumerate(create_targets):
                rec = created_records[idx] if idx < len(created_records) else {}
                h.record_id = rec.get('record_id') or (rec.get('record') or {}).get('record_id')
                if h.record_id:
                    self._put_holding_cache(h)

        if update_payloads or create_payloads:
            self._flush_persistent_holdings_index()

        return {
            'mode': mode,
            'updated': len(update_payloads),
            'created': len(create_payloads),
            'preloaded_accounts': preloaded_accounts,
        }

    def update_holding_quantity(self, asset_id: str, account: str, quantity_change: float, market: Optional[str] = None):
        """更新持仓数量（优先使用预加载索引与内存快照）"""
        from ..time_utils import bj_now_naive

        holding = self.get_holding(asset_id, account, market)
        if not holding or not holding.record_id:
            return

        is_cash_like = (holding.asset_type and holding.asset_type.value in ('cash', 'mmf'))
        new_quantity = self._quantize_money(holding.quantity + quantity_change) if is_cash_like else (holding.quantity + quantity_change)
        now_str = bj_now_naive().strftime('%Y-%m-%d %H:%M:%S')
        update_fields = {
            'quantity': new_quantity,
            'updated_at': now_str
        }
        try:
            self.client.update_record('holdings', holding.record_id, update_fields)
        except Exception:
            self._invalidate_holding_cache(asset_id, account, market, flush_persistent=True)
            raise

        holding.quantity = new_quantity
        holding.updated_at = datetime.strptime(now_str, DATETIME_FORMAT)
        self._put_holding_cache(holding)

    def delete_holding_if_zero(self, asset_id: str, account: str, market: Optional[str] = None):
        """如果持仓为0则删除（容忍极小浮点残值）"""
        holding = self.get_holding(asset_id, account, market)
        if holding and holding.record_id and abs(holding.quantity) <= 1e-8:
            self.client.delete_record('holdings', holding.record_id)
            self._invalidate_holding_cache(asset_id, account, market, flush_persistent=True)

    def delete_holding_by_record_id(self, record_id: str) -> bool:
        """通过记录ID删除持仓"""
        ok = self.client.delete_record('holdings', record_id)
        if ok:
            self._invalidate_holding_cache_by_record_id(record_id, flush_persistent=True)
        return ok

    def _holding_to_dict(self, holding: Holding) -> Dict:
        """Holding 转字典"""
        from ..time_utils import bj_now_naive

        result = {
            'asset_id': holding.asset_id,
            'asset_name': holding.asset_name,
            'asset_type': holding.asset_type,
            'market': holding.market or '',
            'account': holding.account,
            'quantity': holding.quantity,
            'avg_cost': holding.avg_cost,
            'currency': holding.currency,
            'asset_class': holding.asset_class,
            'industry': holding.industry,
            'tag': holding.tag,
        }

        if holding.created_at:
            result['created_at'] = holding.created_at.strftime(DATETIME_FORMAT)
        if holding.updated_at:
            result['updated_at'] = holding.updated_at.strftime(DATETIME_FORMAT)

        return result

    def _dict_to_holding(self, data: Dict) -> Holding:
        """字典转 Holding"""
        created_at = None
        updated_at = None

        if data.get('created_at'):
            try:
                created_at = datetime.strptime(data['created_at'], DATETIME_FORMAT)
            except (ValueError, TypeError):
                pass

        if data.get('updated_at'):
            try:
                updated_at = datetime.strptime(data['updated_at'], DATETIME_FORMAT)
            except (ValueError, TypeError):
                pass

        return Holding(
            record_id=data.get('record_id'),
            asset_id=data.get('asset_id', ''),
            asset_name=data.get('asset_name', ''),
            asset_type=AssetType(data.get('asset_type')) if data.get('asset_type') else AssetType.OTHER,
            market=data.get('market') or None,
            account=data.get('account', ''),
            quantity=float(data.get('quantity', 0)),
            avg_cost=float(data.get('avg_cost')) if data.get('avg_cost') is not None else None,
            currency=data.get('currency', 'CNY'),
            asset_class=AssetClass(data.get('asset_class')) if data.get('asset_class') else None,
            industry=Industry(data.get('industry')) if data.get('industry') else None,
            tag=data.get('tag', []),
            created_at=created_at,
            updated_at=updated_at
        )
