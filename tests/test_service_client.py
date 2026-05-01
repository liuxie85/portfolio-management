from __future__ import annotations

import io
import json
from urllib.error import HTTPError, URLError

import pytest
import src.service.client as client_module
from src.service.client import PortfolioServiceClient, PortfolioServiceResponseError, PortfolioServiceUnavailable


class FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def read(self):
        return json.dumps(self.payload).encode("utf-8")


class RawResponse(FakeResponse):
    def read(self):
        return self.payload


def test_service_client_builds_local_request_urls():
    calls = []

    def fake_urlopen(request, timeout):
        calls.append((request.full_url, timeout))
        return FakeResponse({"success": True, "accounts": ["alice"]})

    old_urlopen = client_module.urlopen
    try:
        client_module.urlopen = fake_urlopen
        client = PortfolioServiceClient(base_url="http://127.0.0.1:8765/", timeout=1.25)
        result = client.list_accounts(include_default=False)
    finally:
        client_module.urlopen = old_urlopen

    assert result["accounts"] == ["alice"]
    assert calls == [("http://127.0.0.1:8765/accounts?include_default=False", 1.25)]


def test_service_client_uses_query_routes_for_account_values():
    calls = []

    def fake_urlopen(request, timeout):
        calls.append((request.full_url, timeout))
        return FakeResponse({"success": True})

    old_urlopen = client_module.urlopen
    try:
        client_module.urlopen = fake_urlopen
        client = PortfolioServiceClient(base_url="http://127.0.0.1:8765", timeout=1.0)
        client.get_holdings(account="alice/bob & co", include_cash=False, group_by_market=True, include_price=True)
        client.get_cash(account="alice/bob & co")
        client.get_nav(account="alice/bob & co", days=14)
        client.full_report(account="alice/bob & co", price_timeout=9)
        client.generate_report(account="alice/bob & co", report_type="monthly/special", price_timeout=11)
    finally:
        client_module.urlopen = old_urlopen

    encoded_account = "alice%2Fbob+%26+co"
    assert calls == [
        (f"http://127.0.0.1:8765/holdings?account={encoded_account}&include_cash=False&group_by_market=True&include_price=True", 1.0),
        (f"http://127.0.0.1:8765/cash?account={encoded_account}", 1.0),
        (f"http://127.0.0.1:8765/nav?account={encoded_account}&days=14", 1.0),
        (f"http://127.0.0.1:8765/report/full?account={encoded_account}&price_timeout=9", 1.0),
        (f"http://127.0.0.1:8765/report/monthly%2Fspecial?account={encoded_account}&price_timeout=11", 1.0),
    ]


def test_service_client_normalizes_accounts_list_query_value():
    calls = []

    def fake_urlopen(request, timeout):
        calls.append((request.full_url, timeout))
        return FakeResponse({"success": True})

    old_urlopen = client_module.urlopen
    try:
        client_module.urlopen = fake_urlopen
        client = PortfolioServiceClient(base_url="http://127.0.0.1:8765", timeout=1.0)
        client.multi_account_overview(accounts=["alice", "bob"], price_timeout=7, include_details=True)
    finally:
        client_module.urlopen = old_urlopen

    assert calls == [
        ("http://127.0.0.1:8765/accounts/overview?accounts=alice%2Cbob&price_timeout=7&include_details=True", 1.0)
    ]


def test_service_client_marks_unavailable_on_connection_error():
    def fake_urlopen(_request, **_kwargs):
        raise URLError("down")

    old_urlopen = client_module.urlopen
    try:
        client_module.urlopen = fake_urlopen
        client = PortfolioServiceClient(base_url="http://127.0.0.1:8765", timeout=0.1)
        try:
            client.health()
        except PortfolioServiceUnavailable as exc:
            assert "down" in str(exc)
        else:
            raise AssertionError("expected PortfolioServiceUnavailable")
    finally:
        client_module.urlopen = old_urlopen


def test_service_client_raises_response_error_on_http_error():
    def fake_urlopen(_request, **_kwargs):
        raise HTTPError(
            url="http://127.0.0.1:8765/accounts",
            code=500,
            msg="error",
            hdrs={},
            fp=io.BytesIO(b'{"detail":"boom"}'),
        )

    old_urlopen = client_module.urlopen
    try:
        client_module.urlopen = fake_urlopen
        client = PortfolioServiceClient(base_url="http://127.0.0.1:8765", timeout=0.1)
        with pytest.raises(PortfolioServiceResponseError, match="HTTP 500"):
            client.list_accounts()
    finally:
        client_module.urlopen = old_urlopen


def test_service_client_raises_response_error_on_invalid_payload():
    responses = [RawResponse(b"<html>not json</html>"), RawResponse(b"[]")]

    def fake_urlopen(_request, **_kwargs):
        return responses.pop(0)

    old_urlopen = client_module.urlopen
    try:
        client_module.urlopen = fake_urlopen
        client = PortfolioServiceClient(base_url="http://127.0.0.1:8765", timeout=0.1)
        with pytest.raises(PortfolioServiceResponseError, match="non-JSON"):
            client.health()
        with pytest.raises(PortfolioServiceResponseError, match="non-object JSON"):
            client.health()
    finally:
        client_module.urlopen = old_urlopen
