import json
from datetime import date
from unittest.mock import Mock

import pytest

from src.app.snapshot_service import SnapshotService, snapshot_digest
from src.models import AssetClass, AssetType, Holding, PortfolioValuation


def _valuation():
    holding = Holding(
        asset_id="000001",
        asset_name="平安银行",
        asset_type=AssetType.A_STOCK,
        account="a",
        market="CN",
        quantity=12.345,
        avg_cost=9.876,
        currency="CNY",
        asset_class=AssetClass.CN_ASSET,
        current_price=10.123,
        cny_price=10.123,
        market_value_cny=124.963,
    )
    return PortfolioValuation(
        account="a",
        total_value_cny=124.96,
        cash_value_cny=0.0,
        stock_value_cny=124.96,
        fund_value_cny=0.0,
        shares=100.0,
        nav=1.2496,
        holdings=[holding],
        warnings=[],
    )


def test_snapshot_service_writes_when_preview_has_changes(tmp_path):
    storage = Mock()
    storage.batch_upsert_holding_snapshots.side_effect = [
        {"to_create": [{"asset_id": "000001"}], "to_update": []},
        {"created": 1, "updated": 0},
    ]
    service = SnapshotService(storage=storage, data_dir=tmp_path)

    snapshots = service.persist_holdings_snapshot(
        account="a",
        today=date(2026, 3, 19),
        valuation=_valuation(),
        dry_run=False,
    )

    assert len(snapshots) == 1
    assert snapshots[0].dedup_key == "a:2026-03-19:CN:000001"
    assert storage.batch_upsert_holding_snapshots.call_count == 2
    assert storage.batch_upsert_holding_snapshots.call_args_list[0].kwargs["dry_run"] is True
    assert storage.batch_upsert_holding_snapshots.call_args_list[1].kwargs["dry_run"] is False

    out_file = tmp_path / "holdings_snapshot" / "a" / "2026-03-19.json"
    payload = json.loads(out_file.read_text(encoding="utf-8"))
    assert payload["count"] == 1
    assert payload["digest"] == snapshot_digest(snapshots)
    assert payload["snapshots"][0]["asset_id"] == "000001"


def test_snapshot_service_skips_feishu_write_when_preview_has_no_changes(tmp_path):
    storage = Mock()
    storage.batch_upsert_holding_snapshots.return_value = {"to_create": [], "to_update": []}
    service = SnapshotService(storage=storage, data_dir=tmp_path)

    service.persist_holdings_snapshot(
        account="a",
        today=date(2026, 3, 19),
        valuation=_valuation(),
        dry_run=False,
    )

    storage.batch_upsert_holding_snapshots.assert_called_once()
    assert (tmp_path / "holdings_snapshot" / "a" / "2026-03-19.json").exists()


def test_snapshot_service_passes_dry_run_to_actual_write(tmp_path):
    storage = Mock()
    storage.batch_upsert_holding_snapshots.side_effect = [
        {"to_create": [], "to_update": [{"asset_id": "000001"}]},
        {"created": 0, "updated": 0},
    ]
    service = SnapshotService(storage=storage, data_dir=tmp_path)

    service.persist_holdings_snapshot(
        account="a",
        today=date(2026, 3, 19),
        valuation=_valuation(),
        dry_run=True,
    )

    assert storage.batch_upsert_holding_snapshots.call_count == 2
    assert storage.batch_upsert_holding_snapshots.call_args_list[1].kwargs["dry_run"] is True


def test_snapshot_service_raises_when_feishu_write_fails(tmp_path):
    storage = Mock()
    storage.batch_upsert_holding_snapshots.side_effect = RuntimeError("boom")
    service = SnapshotService(storage=storage, data_dir=tmp_path)

    with pytest.raises(RuntimeError, match="boom"):
        service.persist_holdings_snapshot(
            account="a",
            today=date(2026, 3, 19),
            valuation=_valuation(),
            dry_run=False,
        )


def test_snapshot_service_ignores_local_snapshot_write_failure(tmp_path):
    storage = Mock()
    storage.batch_upsert_holding_snapshots.return_value = {"to_create": [], "to_update": []}
    data_dir = tmp_path / "not_a_directory"
    data_dir.write_text("block mkdir", encoding="utf-8")
    service = SnapshotService(storage=storage, data_dir=data_dir)

    snapshots = service.persist_holdings_snapshot(
        account="a",
        today=date(2026, 3, 19),
        valuation=_valuation(),
        dry_run=False,
    )

    assert len(snapshots) == 1
