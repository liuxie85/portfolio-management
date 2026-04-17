from src.pricing import PriceRequest, PriceService
from src.pricing.types import ProviderResult


class Provider:
    name = "test-provider"

    def supports(self, request):
        return True

    def fetch_one(self, request):
        return ProviderResult(
            payload={"code": request.normalized_code or request.code, "price": 1.23, "currency": "CNY", "cny_price": 1.23},
            provider=self.name,
            latency_ms=1,
        )


def test_price_service_returns_first_successful_provider_result():
    service = PriceService([Provider()])

    result = service.fetch_realtime(PriceRequest(code="000001", normalized_code="SZ000001"))

    assert result["code"] == "SZ000001"
    assert result["provider"] == "test-provider"
    assert result["source_chain"] == ["test-provider"]
    assert service.last_diagnostics[0]["ok"] is True
