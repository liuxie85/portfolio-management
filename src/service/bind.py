"""Bind-host safety checks for the local HTTP service."""
from __future__ import annotations

import ipaddress


LOOPBACK_NAMES = {"localhost"}


def is_loopback_host(host: str) -> bool:
    normalized = (host or "").strip().lower()
    if normalized in LOOPBACK_NAMES:
        return True
    try:
        return ipaddress.ip_address(normalized).is_loopback
    except ValueError:
        return False


def validate_bind_host(host: str, *, allow_remote: bool = False) -> None:
    if allow_remote or is_loopback_host(host):
        return
    raise ValueError(
        f"refusing to bind unauthenticated portfolio service to non-loopback host {host!r}; "
        "use --allow-remote only behind an authenticated local network boundary"
    )
