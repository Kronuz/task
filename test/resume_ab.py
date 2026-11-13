#!/usr/bin/env python3
# Copyright © 2026 Germán Méndez Bravo (Kronuz)
# SPDX-License-Identifier: MIT
"""Paired A/B of the resume path (test/resume_bench.cc) with light_slices 0: task at BASE (A)
against this checkout (B).

`python3 test/resume_ab.py BASE [ROUNDS]` builds the bench against BASE's and this checkout's
headers with one compiler and one Asio, runs ROUNDS ABBA blocks (8 by default), and prints, per
measurement, the B/A ratio of each block, their median, min and max, and each arm's median ns.
"""

from __future__ import annotations

import json
import statistics
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def run(cmd: list[str]) -> str:
    return subprocess.run(cmd, check=True, capture_output=True, text=True).stdout


def main() -> int:
    if len(sys.argv) not in (2, 3):
        print(__doc__, file=sys.stderr)
        return 2
    base = Path(sys.argv[1]).resolve()
    rounds = int(sys.argv[2]) if len(sys.argv) == 3 else 8
    work = ROOT / "build-resume-ab"
    deps = work / "deps"
    # Configuring this checkout fetches the Asio and wheel that CMakeLists.txt pins.
    subprocess.run(["cmake", "-S", str(ROOT), "-B", str(deps), "-DCMAKE_BUILD_TYPE=Release"], check=True, capture_output=True)
    asio_inc = deps / "_deps" / "asio-src" / "asio" / "include"
    wheel_inc = next(p.parent for p in (deps / "_deps" / "wheel-src").rglob("wheel.hh"))
    arms: dict[str, Path] = {}
    for arm, headers in (("A", base), ("B", ROOT)):
        inc = work / f"inc-{arm}"
        inc.mkdir(parents=True, exist_ok=True)
        for f in ("task.hh", "task_asio.hh"):
            (inc / f).write_bytes((headers / f).read_bytes())
        exe = work / f"resume_bench_{arm}"
        subprocess.run(
            ["c++", "-std=c++20", "-O2", "-DNDEBUG", "-DASIO_STANDALONE", f"-I{inc}", f"-I{wheel_inc}", f"-I{asio_inc}",
             str(ROOT / "test" / "resume_bench.cc"), "-o", str(exe), "-pthread"],
            check=True,
        )
        arms[arm] = exe
    samples: dict[str, list[dict[str, float]]] = {"A": [], "B": []}
    ratios: dict[str, list[float]] = {}
    for _ in range(rounds):
        block: dict[str, list[dict[str, float]]] = {"A": [], "B": []}
        for arm in ("A", "B", "B", "A"):
            block[arm].append(json.loads(run([str(arms[arm])])))
        for arm in ("A", "B"):
            samples[arm].extend(block[arm])
        for key in block["A"][0]:
            a = statistics.mean(r[key] for r in block["A"])
            b = statistics.mean(r[key] for r in block["B"])
            ratios.setdefault(key, []).append(b / a)
    report = {
        key: {
            "ratio_median": round(statistics.median(rs), 4),
            "ratio_min": round(min(rs), 4),
            "ratio_max": round(max(rs), 4),
            "ratios": [round(r, 4) for r in rs],
            "A_ns_median": round(statistics.median(s[key] for s in samples["A"]), 3),
            "B_ns_median": round(statistics.median(s[key] for s in samples["B"]), 3),
        }
        for key, rs in ratios.items()
    }
    out = json.dumps({"base": str(base), "rounds": rounds, "results": report}, indent=1)
    (work / "ab.json").write_text(out)
    print(out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
