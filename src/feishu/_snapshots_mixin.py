"""Holdings snapshot mixin for FeishuStorage."""
from typing import Any, Dict, List

from ..snapshot_models import HoldingSnapshot


class SnapshotsMixin:
    """Holdings snapshot batch upsert."""

    def batch_upsert_holding_snapshots(self, snapshots: List[HoldingSnapshot], dry_run: bool = False) -> Dict[str, any]:
        """Write holdings_snapshot rows in a best-effort idempotent way."""
        if not snapshots:
            return {"created": 0, "updated": 0, "dry_run": dry_run}

        by_key: Dict[str, HoldingSnapshot] = {}
        for s in snapshots:
            by_key[s.dedup_key] = s

        any_s = snapshots[0]
        filter_str = (
            f'CurrentValue.[as_of] = "{self._escape_filter_value(any_s.as_of)}" && '
            f'CurrentValue.[account] = "{self._escape_filter_value(any_s.account)}"'
        )
        existing_records = self.client.list_records('holdings_snapshot', filter_str=filter_str)

        existing_by_key: Dict[str, str] = {}
        for r in existing_records:
            k = (r.get('fields') or {}).get('dedup_key')
            if k:
                existing_by_key[str(k)] = r['record_id']

        creates = []
        updates = []

        for k, s in by_key.items():
            fields = {
                'as_of': s.as_of,
                'account': s.account,
                'asset_id': s.asset_id,
                'market': s.market,
                'quantity': s.quantity,
                'currency': s.currency,
                'price': s.price,
                'cny_price': s.cny_price,
                'market_value_cny': s.market_value_cny,
                'dedup_key': s.dedup_key,
                'asset_name': s.asset_name,
                'avg_cost': s.avg_cost,
                'source': s.source,
                'remark': s.remark,
            }
            feishu_fields = self._to_feishu_fields(fields, 'holdings_snapshot')

            record_id = existing_by_key.get(k)
            if record_id:
                updates.append({'record_id': record_id, 'fields': feishu_fields})
            else:
                creates.append({'fields': feishu_fields})

        if dry_run:
            return {
                'dry_run': True,
                'filter': filter_str,
                'existing_count': len(existing_records),
                'to_create': len(creates),
                'to_update': len(updates),
                'create_sample': creates[:3],
                'update_sample': updates[:3],
            }

        created = 0
        updated = 0
        if creates:
            self.client.batch_create_records('holdings_snapshot', creates)
            created = len(creates)
        if updates:
            self.client.batch_update_records('holdings_snapshot', updates)
            updated = len(updates)

        return {'dry_run': False, 'created': created, 'updated': updated, 'existing_count': len(existing_records)}
