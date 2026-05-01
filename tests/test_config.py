from __future__ import annotations

import json
from pathlib import Path
from tempfile import TemporaryDirectory

from pytest import MonkeyPatch

from src import config


def test_config_typed_getters_use_file_then_env_overrides():
    with TemporaryDirectory() as tmp:
        config_file = Path(tmp) / "config.json"
        config_file.write_text(
            json.dumps({
                "report": {"sync_futu_cash_mmf": False},
                "futu": {"opend": {"port": 1234}},
                "nav": {"disable_runtime_validation": True},
            }),
            encoding="utf-8",
        )

        patch = MonkeyPatch()
        try:
            patch.setattr(config, "_CONFIG_FILE", config_file)
            patch.delenv("PM_SYNC_FUTU_CASH_MMF", raising=False)
            patch.delenv("FUTU_OPEND_PORT", raising=False)
            patch.delenv("PORTFOLIO_NAV_DISABLE_RUNTIME_VALIDATION", raising=False)
            config.reload_config()

            assert config.get_bool("report.sync_futu_cash_mmf", True) is False
            assert config.get_int("futu.opend.port") == 1234
            assert config.get_bool("nav.disable_runtime_validation", False) is True

            patch.setenv("PM_SYNC_FUTU_CASH_MMF", "1")
            patch.setenv("FUTU_OPEND_PORT", "2222")
            patch.setenv("PORTFOLIO_NAV_DISABLE_RUNTIME_VALIDATION", "0")

            assert config.get_bool("report.sync_futu_cash_mmf", False) is True
            assert config.get_int("futu.opend.port") == 2222
            assert config.get_bool("nav.disable_runtime_validation", True) is False

            patch.setenv("FUTU_OPEND_PORT", "not-an-int")
            assert config.get_int("futu.opend.port", 99) == 99
        finally:
            patch.undo()
            config.reload_config()
