"""Local schema migration state."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List

from src import config
from src.time_utils import bj_now_naive


class SchemaStateStore:
    def __init__(self, state_file: Path | None = None):
        self.state_file = state_file or (config.get_data_dir() / "schema_migrations.json")

    def load(self) -> Dict:
        try:
            with open(self.state_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except FileNotFoundError:
            return {"applied": []}
        except (json.JSONDecodeError, OSError):
            return {"applied": []}

    def applied_ids(self) -> set[str]:
        return {item.get("id") for item in self.load().get("applied", []) if item.get("id")}

    def mark_applied(self, migration_id: str, description: str) -> None:
        state = self.load()
        applied: List[Dict] = state.setdefault("applied", [])
        if migration_id not in {item.get("id") for item in applied}:
            applied.append(
                {
                    "id": migration_id,
                    "description": description,
                    "applied_at": bj_now_naive().isoformat(),
                }
            )
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        with open(self.state_file, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2, sort_keys=True)
