"""Migration runner for Feishu schema evolution."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Dict, Iterable, List, Optional

from .schema_state import SchemaStateStore


ApplyFn = Callable[[object], Dict]


@dataclass(frozen=True)
class Migration:
    id: str
    description: str
    required_tables: Dict[str, List[str]] = field(default_factory=dict)
    apply_fn: Optional[ApplyFn] = None

    def plan(self) -> Dict:
        return {
            "id": self.id,
            "description": self.description,
            "required_tables": self.required_tables,
            "has_apply": self.apply_fn is not None,
        }

    def apply(self, storage=None) -> Dict:
        if self.apply_fn is None:
            return {"success": True, "changed": False, "message": "documentation/check migration only"}
        return self.apply_fn(storage)


class MigrationRunner:
    def __init__(self, migrations: Iterable[Migration], state_store: Optional[SchemaStateStore] = None):
        self.migrations = list(migrations)
        self.state_store = state_store or SchemaStateStore()

    def pending(self) -> List[Migration]:
        applied = self.state_store.applied_ids()
        return [m for m in self.migrations if m.id not in applied]

    def plan(self) -> Dict:
        return {
            "pending_count": len(self.pending()),
            "pending": [m.plan() for m in self.pending()],
        }

    def apply(self, storage=None) -> Dict:
        results = []
        for migration in self.pending():
            result = migration.apply(storage)
            if not result.get("success", False):
                results.append({"id": migration.id, "success": False, "result": result})
                break
            self.state_store.mark_applied(migration.id, migration.description)
            results.append({"id": migration.id, "success": True, "result": result})
        return {"success": all(r["success"] for r in results), "results": results}
