"""Compensation task recording for non-transactional multi-table writes."""
from __future__ import annotations

import json
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional

from src import config
from src.time_utils import bj_now_naive


@dataclass
class CompensationTask:
    task_id: str
    operation_type: str
    account: str
    status: str
    payload: Dict[str, Any]
    error: str
    related_record_id: Optional[str] = None
    retry_count: int = 0
    created_at: str = field(default_factory=lambda: bj_now_naive().isoformat())
    updated_at: str = field(default_factory=lambda: bj_now_naive().isoformat())


class CompensationService:
    """Record repair tasks when a multi-step write partially succeeds.

    Storage is best-effort: if a storage backend exposes
    ``add_compensation_task`` it is used; otherwise tasks are appended to
    ``.data/compensation_tasks.jsonl``.
    """

    def __init__(self, storage=None, queue_file: Optional[Path] = None):
        self.storage = storage
        self.queue_file = queue_file or (config.get_data_dir() / "compensation_tasks.jsonl")

    def record(
        self,
        *,
        operation_type: str,
        account: str,
        payload: Dict[str, Any],
        error: Exception | str,
        related_record_id: Optional[str] = None,
    ) -> CompensationTask:
        task = CompensationTask(
            task_id=f"repair_{uuid.uuid4().hex}",
            operation_type=operation_type,
            account=account,
            status="PENDING",
            payload=payload,
            error=str(error),
            related_record_id=related_record_id,
        )

        if self.storage is not None and hasattr(self.storage, "add_compensation_task"):
            try:
                self.storage.add_compensation_task(task)
                return task
            except Exception:
                # Fall back to local queue; never mask the original write path.
                pass

        self.queue_file.parent.mkdir(parents=True, exist_ok=True)
        with open(self.queue_file, "a", encoding="utf-8") as f:
            f.write(json.dumps(asdict(task), ensure_ascii=False, sort_keys=True) + "\n")
        return task
