#!/usr/bin/env python3
"""Natural language → structured intent (NO side effects).

This is the LLM-facing adapter layer. It must NOT write to storage.
Execution must be done by calling skill_api / scripts/pm.py with explicit params.

For now this is a stub: it just wraps input into a placeholder structure.
"""

from __future__ import annotations

import argparse
import json


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description='NL → intent (stub)')
    ap.add_argument('text', nargs='*', help='natural language input')
    args = ap.parse_args(argv)

    text = ' '.join(args.text).strip()
    intent = {
        'version': 1,
        'text': text,
        'intent': None,
        'confidence': 0.0,
        'slots': {},
        'notes': 'stub: plug LLM here; execution must be code-driven',
    }
    print(json.dumps(intent, ensure_ascii=False, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
