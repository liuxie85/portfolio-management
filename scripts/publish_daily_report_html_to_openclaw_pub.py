#!/usr/bin/env python3
"""Generate HTML daily report and publish it under OpenClaw publish root.

OpenClaw instances commonly run a publish-server bound to :3000 with root:
  /home/node/.openclaw/workspace/published

This script:
1) generates portfolio-management/public/index.html
2) copies it to published/portfolio-management/index.html

So it becomes available at:
  https://openclaw-pub-<instance>.imlgz.com/portfolio-management/

Default is to overwrite index.html.
"""

from __future__ import annotations

import shutil
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
PUBLIC_HTML = REPO_ROOT / "public" / "index.html"
PUBLISHED_ROOT = Path("/home/node/.openclaw/workspace/published")
TARGET_DIR = PUBLISHED_ROOT / "portfolio-management"
TARGET_HTML = TARGET_DIR / "index.html"


def main() -> None:
    # 1) Generate HTML
    gen = REPO_ROOT / "scripts" / "generate_daily_report_html.py"
    cmd = [sys.executable, str(gen)]
    rc = __import__("subprocess").run(cmd, cwd=str(REPO_ROOT)).returncode
    if rc != 0:
        raise SystemExit(rc)
    if not PUBLIC_HTML.exists():
        raise RuntimeError(f"Expected generated HTML at {PUBLIC_HTML}")

    # 2) Copy into publish root
    TARGET_DIR.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(PUBLIC_HTML, TARGET_HTML)
    print(str(TARGET_HTML))


if __name__ == "__main__":
    main()
