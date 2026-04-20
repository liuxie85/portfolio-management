"""Shared pricing types."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Optional


@dataclass(frozen=True)
class PriceRequest:
    """Realtime quote request passed to pricing providers."""

    code: str
    asset_name: str = ""
    asset_type: Optional[Any] = None
    normalized_code: Optional[str] = None
    hints: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ProviderResult:
    """Result returned by a pricing provider."""

    payload: Optional[Dict[str, Any]]
    provider: str
    error: Optional[str] = None
    latency_ms: Optional[int] = None

    @property
    def ok(self) -> bool:
        return self.payload is not None and self.error is None
