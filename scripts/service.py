#!/usr/bin/env python3
"""Manage the local portfolio-management service daemon."""
from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src import config
from src.service.bind import validate_bind_host
from src.service.client import PortfolioServiceClient


DATA_DIR = config.get_data_dir()
PID_FILE = DATA_DIR / "portfolio-service.pid"
LOG_FILE = DATA_DIR / "portfolio-service.log"


def _pid_running(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def _process_command(pid: int) -> str:
    try:
        result = subprocess.run(
            ["ps", "-p", str(pid), "-o", "command="],
            check=False,
            capture_output=True,
            text=True,
        )
    except OSError:
        return ""
    return (result.stdout or "").strip()


def _read_pid_metadata() -> dict | None:
    try:
        raw = PID_FILE.read_text(encoding="utf-8").strip()
    except OSError:
        return None
    if not raw:
        return None
    try:
        payload = json.loads(raw)
        if isinstance(payload, dict) and payload.get("pid"):
            return payload
    except json.JSONDecodeError:
        pass
    try:
        return {"pid": int(raw)}
    except ValueError:
        return None


def _read_pid() -> int | None:
    metadata = _read_pid_metadata()
    if not metadata:
        return None
    try:
        return int(metadata["pid"])
    except (TypeError, ValueError):
        return None


def _write_pid_metadata(*, pid: int, host: str, port: int, url: str, command: list[str]) -> None:
    payload = {
        "pid": pid,
        "host": host,
        "port": port,
        "url": url,
        "cwd": str(REPO_ROOT),
        "command": command,
        "started_at": time.time(),
    }
    PID_FILE.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _metadata_matches_process(metadata: dict | None) -> bool:
    if not metadata:
        return False
    try:
        pid = int(metadata["pid"])
    except (KeyError, TypeError, ValueError):
        return False
    if not _pid_running(pid):
        return False
    command = _process_command(pid)
    expected_script = str(REPO_ROOT / "scripts" / "serve.py")
    if expected_script not in command:
        return False
    host = metadata.get("host")
    port = metadata.get("port")
    if host and f"--host {host}" not in command:
        return False
    if port and f"--port {port}" not in command:
        return False
    return True


def _service_url(args) -> str:
    if args.url:
        return args.url.rstrip("/")
    host = args.host or config.get_service_host()
    port = args.port or config.get_service_port()
    return f"http://{host}:{port}"


def cmd_status(args) -> int:
    url = _service_url(args)
    metadata = _read_pid_metadata()
    pid = _read_pid()
    client = PortfolioServiceClient(base_url=url, timeout=0.5)
    healthy = client.is_available()

    print(f"url: {url}")
    print(f"pid_file: {PID_FILE}")
    print(f"log_file: {LOG_FILE}")
    print(f"pid: {pid or '-'}")
    print(f"process: {'running' if _metadata_matches_process(metadata) else 'not-running'}")
    print(f"health: {'ok' if healthy else 'unavailable'}")
    return 0 if healthy else 1


def cmd_start(args) -> int:
    url = _service_url(args)
    metadata = _read_pid_metadata()
    pid = _read_pid()
    if pid and _metadata_matches_process(metadata):
        client = PortfolioServiceClient(base_url=url, timeout=0.5)
        if client.is_available():
            print(f"service already running: {url} pid={pid}")
            return 0
        raise SystemExit(f"pid file points to a running non-healthy process: pid={pid}")

    if pid and _pid_running(pid) and not _metadata_matches_process(metadata):
        PID_FILE.unlink(missing_ok=True)
    elif pid and not _pid_running(pid):
        PID_FILE.unlink(missing_ok=True)

    host = args.host or config.get_service_host()
    port = args.port or config.get_service_port()
    validate_bind_host(host, allow_remote=bool(args.allow_remote))
    command = [
        sys.executable,
        str(REPO_ROOT / "scripts" / "serve.py"),
        "--host",
        host,
        "--port",
        str(port),
    ]
    if args.allow_remote:
        command.append("--allow-remote")

    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    log = LOG_FILE.open("ab")
    process = subprocess.Popen(
        command,
        cwd=str(REPO_ROOT),
        stdout=log,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    log.close()
    _write_pid_metadata(pid=process.pid, host=host, port=port, url=url, command=command)

    client = PortfolioServiceClient(base_url=url, timeout=0.5)
    deadline = time.time() + args.wait
    while time.time() < deadline:
        if client.is_available():
            print(f"service started: {url} pid={process.pid}")
            return 0
        if process.poll() is not None:
            raise SystemExit(f"service exited during startup; see {LOG_FILE}")
        time.sleep(0.2)

    raise SystemExit(f"service did not become healthy within {args.wait:g}s; see {LOG_FILE}")


def cmd_stop(_args) -> int:
    metadata = _read_pid_metadata()
    pid = _read_pid()
    if not pid:
        print("service pid file not found")
        return 0
    if not _pid_running(pid):
        PID_FILE.unlink(missing_ok=True)
        print(f"removed stale pid file: {pid}")
        return 0
    if not _metadata_matches_process(metadata):
        PID_FILE.unlink(missing_ok=True)
        print(f"removed mismatched pid file without stopping process: pid={pid}")
        return 1

    os.kill(pid, signal.SIGTERM)
    deadline = time.time() + 5
    while time.time() < deadline:
        if not _pid_running(pid):
            PID_FILE.unlink(missing_ok=True)
            print(f"service stopped: pid={pid}")
            return 0
        time.sleep(0.2)

    raise SystemExit(f"service did not stop after SIGTERM: pid={pid}")


def cmd_restart(args) -> int:
    cmd_stop(args)
    return cmd_start(args)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="portfolio-service", description="Manage local portfolio HTTP service")
    parser.add_argument("--url", default=None, help="service URL override")
    parser.add_argument("--host", default=None, help="bind/status host override")
    parser.add_argument("--port", type=int, default=None, help="bind/status port override")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_start = sub.add_parser("start", help="start local service daemon")
    p_start.add_argument("--wait", type=float, default=5.0, help="seconds to wait for health check")
    p_start.add_argument("--allow-remote", action="store_true", help="allow binding to non-loopback hosts; unauthenticated, use with care")
    p_start.set_defaults(func=cmd_start)

    sub.add_parser("status", help="show service status").set_defaults(func=cmd_status)
    sub.add_parser("stop", help="stop service started by this script").set_defaults(func=cmd_stop)

    p_restart = sub.add_parser("restart", help="restart local service daemon")
    p_restart.add_argument("--wait", type=float, default=5.0, help="seconds to wait for health check")
    p_restart.add_argument("--allow-remote", action="store_true", help="allow binding to non-loopback hosts; unauthenticated, use with care")
    p_restart.set_defaults(func=cmd_restart)
    return parser


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
