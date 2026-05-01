from __future__ import annotations

import json

from scripts import service as service_script


def test_metadata_matches_process_requires_expected_service_command(monkeypatch):
    monkeypatch.setattr(service_script, "_pid_running", lambda pid: True)

    serve_script = service_script.REPO_ROOT / "scripts" / "serve.py"
    command = (
        f"/usr/bin/python3 {serve_script} "
        "--host 127.0.0.1 --port 8765"
    )
    monkeypatch.setattr(service_script, "_process_command", lambda pid: command)

    assert service_script._metadata_matches_process({
        "pid": 123,
        "host": "127.0.0.1",
        "port": 8765,
    }) is True
    assert service_script._metadata_matches_process({
        "pid": 123,
        "host": "0.0.0.0",
        "port": 8765,
    }) is False
    assert service_script._metadata_matches_process({
        "pid": 123,
        "host": "127.0.0.1",
        "port": 9999,
    }) is False


def test_metadata_matches_process_rejects_non_service_command(monkeypatch):
    monkeypatch.setattr(service_script, "_pid_running", lambda pid: True)
    monkeypatch.setattr(service_script, "_process_command", lambda pid: "/bin/sleep 999")

    assert service_script._metadata_matches_process({
        "pid": 123,
        "host": "127.0.0.1",
        "port": 8765,
    }) is False


def test_cmd_stop_removes_mismatched_pid_without_killing_process(tmp_path, monkeypatch):
    pid_file = tmp_path / "portfolio-service.pid"
    pid_file.write_text(json.dumps({
        "pid": 123,
        "host": "127.0.0.1",
        "port": 8765,
    }), encoding="utf-8")

    killed = []
    monkeypatch.setattr(service_script, "PID_FILE", pid_file)
    monkeypatch.setattr(service_script, "_pid_running", lambda pid: True)
    monkeypatch.setattr(service_script, "_metadata_matches_process", lambda metadata: False)
    monkeypatch.setattr(service_script.os, "kill", lambda pid, sig: killed.append((pid, sig)))

    assert service_script.cmd_stop(object()) == 1
    assert killed == []
    assert not pid_file.exists()
