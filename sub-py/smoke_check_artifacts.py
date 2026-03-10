"""
Minimal smoke checks for this repo outputs.

Goal: fail fast after running bat/5_run_all_for_stock.bat, so you don't push a broken Pages build.
"""

from __future__ import annotations

import json
import os
import sys
from typing import List, Tuple


PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_PATH = os.path.join(PROJECT_ROOT, "data")


def _exists(path: str) -> Tuple[bool, str]:
    return (os.path.exists(path), path)


def _check_json(path: str) -> Tuple[bool, str]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            json.load(f)
        return (True, path)
    except Exception as e:
        return (False, f"{path} ({repr(e)})")


def _check_manifest() -> List[Tuple[bool, str]]:
    p = os.path.join(DATA_PATH, "manifest.json")
    ok, msg = _check_json(p)
    if not ok:
        return [(False, msg)]

    with open(p, "r", encoding="utf-8") as f:
        m = json.load(f) or {}
    ids = m.get("stock_ids") or []
    if not isinstance(ids, list) or len(ids) == 0:
        return [(False, f"{p} stock_ids empty")]

    sample = str(ids[0])
    whale_json = os.path.join(DATA_PATH, f"{sample}_whale_track.json")
    return [(True, p), _check_json(whale_json)]


def main() -> int:
    checks: List[Tuple[bool, str]] = []

    # Static dashboard entry
    checks.append(_exists(os.path.join(PROJECT_ROOT, "index.html")))
    checks.append(_exists(os.path.join(PROJECT_ROOT, "Version.txt")))

    # Dashboard data
    checks.extend(_check_manifest())

    # Phase 1
    checks.append(_exists(os.path.join(DATA_PATH, "signal_vs_returns_report.html")))

    # Phase 3
    for h in (5, 10, 20):
        checks.append(_exists(os.path.join(DATA_PATH, f"ml_winrate_report_ret{h}d.html")))

    failed = [msg for ok, msg in checks if not ok]
    if failed:
        print("[FAIL] Smoke check FAILED:")
        for msg in failed:
            print("  -", msg)
        return 1

    print("[OK] Smoke check OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

