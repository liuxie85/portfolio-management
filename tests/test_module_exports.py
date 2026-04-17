import src.app as app
import src.domain as domain


def test_app_exports_public_services():
    expected = {
        "AssetNameService",
        "CashFlowSummaryService",
        "CashService",
        "CompensationService",
        "NavBaselineService",
        "NavRecordService",
        "NavSummaryPrinter",
        "ReportingService",
        "ShareService",
        "SnapshotService",
        "TradeService",
        "ValuationService",
        "snapshot_digest",
    }

    assert set(app.__all__) == expected
    for name in expected:
        assert hasattr(app, name)


def test_domain_exports_public_helpers():
    expected = {
        "NavCalculator",
        "NavHistoryIndex",
        "PayloadNormalizer",
    }

    assert set(domain.__all__) == expected
    for name in expected:
        assert hasattr(domain, name)
