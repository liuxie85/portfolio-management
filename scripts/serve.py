#!/usr/bin/env python3
"""Run the portfolio-management HTTP service."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def build_parser() -> argparse.ArgumentParser:
    from src import config

    parser = argparse.ArgumentParser(prog="portfolio-service", description="Run portfolio-management HTTP service")
    parser.add_argument("--host", default=config.get_service_host(), help="bind host (default from config, 127.0.0.1)")
    parser.add_argument("--port", type=int, default=config.get_service_port(), help="bind port (default from config, 8765)")
    parser.add_argument("--allow-remote", action="store_true", help="allow binding to non-loopback hosts; unauthenticated, use with care")
    parser.add_argument("--reload", action="store_true", help="enable uvicorn reload")
    return parser


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)

    from src.service.bind import validate_bind_host

    validate_bind_host(args.host, allow_remote=bool(args.allow_remote))

    import uvicorn

    uvicorn.run(
        "src.service.http:app",
        host=args.host,
        port=args.port,
        reload=bool(args.reload),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
