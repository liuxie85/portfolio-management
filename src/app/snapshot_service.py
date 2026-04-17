"""Holdings snapshot persistence service."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Iterable, Optional

from src.snapshot_models import HoldingSnapshot


def snapshot_digest(snapshots: Iterable[HoldingSnapshot]) -> str:
    """Compute a stable digest for holdings snapshot content."""
    items = []
    for snapshot in snapshots:
        items.append(
            {
                "account": snapshot.account,
                "as_of": snapshot.as_of,
                "asset_id": snapshot.asset_id,
                "market": snapshot.market,
                "currency": snapshot.currency,
                "quantity": snapshot.quantity,
                "market_value_cny": snapshot.market_value_cny,
            }
        )
    items.sort(key=lambda item: (item["account"], item["as_of"], item["market"], item["asset_id"]))
    raw = json.dumps(items, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


class SnapshotService:
    """Persist NAV-time holdings snapshots for auditability and replay."""

    def __init__(self, storage: Any, data_dir: Optional[Path] = None):
        self.storage = storage
        self.data_dir = data_dir or (Path(__file__).resolve().parents[2] / ".data")

    def build_holdings_snapshots(self, *, account: str, as_of: str, valuation: Any) -> list[HoldingSnapshot]:
        snapshots = []
        for holding in valuation.holdings:
            market = holding.market or ""
            snapshots.append(
                HoldingSnapshot(
                    as_of=as_of,
                    account=account,
                    asset_id=holding.asset_id,
                    market=market,
                    quantity=holding.quantity,
                    currency=holding.currency,
                    price=holding.current_price,
                    cny_price=holding.cny_price,
                    market_value_cny=holding.market_value_cny,
                    dedup_key=f"{account}:{as_of}:{market}:{holding.asset_id}",
                    asset_name=holding.asset_name,
                    avg_cost=holding.avg_cost,
                    source="record_nav",
                )
            )
        return snapshots

    def persist_holdings_snapshot(self, *, account: str, today, valuation: Any, dry_run: bool = False) -> list[HoldingSnapshot]:
        """Persist holdings_snapshot rows and write a best-effort local copy.

        Feishu write failures are allowed to bubble up because snapshots are part
        of the NAV auditability contract. Local file write failures remain
        best-effort and should not block NAV recording.
        """
        as_of = today.strftime("%Y-%m-%d")
        snapshots = self.build_holdings_snapshots(account=account, as_of=as_of, valuation=valuation)

        dry_preview = self.storage.batch_upsert_holding_snapshots(snapshots, dry_run=True)
        should_write_snapshot = bool(dry_preview.get("to_create") or dry_preview.get("to_update"))
        if should_write_snapshot:
            self.storage.batch_upsert_holding_snapshots(snapshots, dry_run=dry_run)

        self._write_local_snapshot(account=account, as_of=as_of, snapshots=snapshots)
        return snapshots

    def _write_local_snapshot(self, *, account: str, as_of: str, snapshots: list[HoldingSnapshot]) -> None:
        try:
            out_dir = self.data_dir / "holdings_snapshot" / account
            out_dir.mkdir(parents=True, exist_ok=True)
            out_file = out_dir / f"{as_of}.json"
            payload = {
                "as_of": as_of,
                "account": account,
                "count": len(snapshots),
                "digest": snapshot_digest(snapshots),
                "snapshots": [snapshot.model_dump() for snapshot in snapshots],
            }
            out_file.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception:
            pass
