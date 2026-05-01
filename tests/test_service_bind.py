from __future__ import annotations

import pytest

from src.service.bind import is_loopback_host, validate_bind_host


def test_validate_bind_host_allows_loopback_hosts():
    for host in ("127.0.0.1", "localhost", "::1"):
        assert is_loopback_host(host) is True
        validate_bind_host(host)


def test_validate_bind_host_rejects_non_loopback_by_default():
    with pytest.raises(ValueError, match="refusing to bind"):
        validate_bind_host("0.0.0.0")


def test_validate_bind_host_allows_non_loopback_with_explicit_override():
    validate_bind_host("0.0.0.0", allow_remote=True)
