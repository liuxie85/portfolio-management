from __future__ import annotations

import ast
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def _module_ast(path: str) -> ast.Module:
    return ast.parse((REPO_ROOT / path).read_text(encoding="utf-8"))


def test_generate_daily_report_html_is_renderer_only():
    tree = _module_ast("scripts/generate_daily_report_html.py")

    imported_names = set()
    forbidden_calls = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            imported_names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.Import):
            imported_names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.Call):
            func = node.func
            if isinstance(func, ast.Name):
                forbidden_calls.add(func.id)
            elif isinstance(func, ast.Attribute):
                forbidden_calls.add(func.attr)

    assert "PortfolioSkill" not in imported_names
    assert "build_snapshot" not in forbidden_calls
    assert "generate_report" not in forbidden_calls
    assert "full_report" not in forbidden_calls
    assert "get_nav_history" not in forbidden_calls


def test_publish_daily_report_returns_renderer_bundle_shape():
    tree = _module_ast("scripts/publish_daily_report.py")
    build_report = next(
        node for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == "build_report_data"
    )
    return_dicts = [node.value for node in ast.walk(build_report) if isinstance(node, ast.Return) and isinstance(node.value, ast.Dict)]
    keys = {
        key.value
        for ret in return_dicts
        for key in ret.keys
        if isinstance(key, ast.Constant)
    }

    assert {"snapshot", "report", "nav_result", "nav_snapshot"}.issubset(keys)
