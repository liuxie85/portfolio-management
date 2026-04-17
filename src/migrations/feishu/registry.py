"""Migration registry for the Feishu-backed schema."""
from __future__ import annotations

from src.migrations.runner import Migration


def get_migrations() -> list[Migration]:
    return [
        Migration(
            id="0001_baseline",
            description="Baseline required production tables and idempotency fields.",
            required_tables={
                "holdings": ["asset_id", "asset_name", "asset_type", "account", "market", "quantity", "currency"],
                "transactions": ["tx_date", "tx_type", "asset_id", "account", "quantity", "price", "currency", "request_id", "dedup_key"],
                "cash_flow": ["flow_date", "account", "amount", "currency", "cny_amount", "flow_type", "dedup_key"],
                "nav_history": ["date", "account", "total_value", "shares", "nav"],
                "holdings_snapshot": ["as_of", "account", "asset_id", "quantity", "market_value_cny", "dedup_key"],
            },
        ),
        Migration(
            id="0002_compensation_tasks",
            description="Introduce compensation_tasks for repairable partial multi-table writes.",
            required_tables={
                "compensation_tasks": [
                    "task_id",
                    "operation_type",
                    "account",
                    "status",
                    "payload",
                    "error",
                    "related_record_id",
                    "retry_count",
                    "created_at",
                    "updated_at",
                ],
            },
        ),
        Migration(
            id="0003_schema_version",
            description="Introduce schema_version tracking for future Feishu migrations.",
            required_tables={
                "schema_version": ["migration_id", "description", "applied_at", "status"],
            },
        ),
    ]
