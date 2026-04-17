from __future__ import annotations

import json
import sys
import types

import pytest

from scripts import pm


def test_pm_report_requires_preview_flag():
    with pytest.raises(SystemExit) as exc:
        pm.main(["report", "daily", "--json"])

    assert "preview-only" in str(exc.value)


def test_pm_report_preview_marks_noncanonical_output(monkeypatch, capsys):
    fake_skill_api = types.SimpleNamespace(
        generate_report=lambda **kwargs: {
            "success": True,
            "report_type": kwargs["report_type"],
        }
    )
    monkeypatch.setitem(sys.modules, "skill_api", fake_skill_api)

    assert pm.main(["report", "daily", "--preview", "--json"]) == 0

    out = json.loads(capsys.readouterr().out)
    assert out["success"] is True
    assert out["report_type"] == "daily"
    assert out["preview_only"] is True
    assert out["canonical_entrypoint"] == "scripts/publish_daily_report.py"
