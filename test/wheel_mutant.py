#!/usr/bin/env python3
# Copyright © 2026 Germán Méndez Bravo (Kronuz)
# SPDX-License-Identifier: MIT
"""Builds test/deadline_test.cc against a mutant copy of Kronuz/wheel's wheel.hh, at the pin
CMakeLists.txt fetches, and runs the deadline_test cases it targets.

`python3 test/wheel_mutant.py NAME` exits 0 when every targeted case fails on a CHECK (exit 1),
1 when one passes or dies otherwise, and 2 when the mutant does not apply or the build fails.
Everything lives in build-wheel-mutant-NAME/, so the checkout and the pin are untouched.
"""

from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

ARM = "\t[[gnu::always_inline]] void arm(timer_entry& e, std::uint64_t deadline) noexcept {\n\t\towner_check();\n"

# name: (original text in wheel.hh, mutant text, deadline_test cases it must fail)
MUTANTS: dict[str, tuple[str, str, tuple[str, ...]]] = {
    # An entry a fire arms or re-arms joins the due list, so it fires in the same expire, early.
    "rearm-fires-early": (
        ARM,
        ARM + "\t\tif (expiring_) {\n\t\t\tif (e.armed()) { unlink(e); }\n\t\t\te.deadline = deadline;\n"
        "\t\t\tlink(e, due_);\n\t\t\treturn;\n\t\t}\n",
        ("reentry", "model"),
    ),
}


def run(cmd: list[str]) -> bool:
    return subprocess.run(cmd).returncode == 0


def main() -> int:
    if len(sys.argv) != 2 or sys.argv[1] not in MUTANTS:
        print(f"usage: wheel_mutant.py {{{','.join(MUTANTS)}}}", file=sys.stderr)
        return 2
    name = sys.argv[1]
    original, mutant, cases = MUTANTS[name]
    work = ROOT / f"build-wheel-mutant-{name}"
    shutil.rmtree(work, ignore_errors=True)
    fetch = work / "fetch"
    if not run(["cmake", "-S", str(ROOT), "-B", str(fetch), "-DTASK_ASIO=OFF"]):
        print(f"mutant {name}: fetching the wheel failed", file=sys.stderr)
        return 2
    wheel = work / "wheel"
    shutil.copytree(fetch / "_deps" / "wheel-src", wheel, ignore=shutil.ignore_patterns(".git"))
    header = wheel / "wheel.hh"
    text = header.read_text()
    if text.count(original) != 1:
        print(f"mutant {name}: the original text occurs {text.count(original)} times in wheel.hh", file=sys.stderr)
        return 2
    header.write_text(text.replace(original, mutant))
    build = work / "build"
    for cmd in (
        ["cmake", "-S", str(ROOT), "-B", str(build), "-DTASK_ASIO=OFF", "-DCMAKE_BUILD_TYPE=Debug", f"-DFETCHCONTENT_SOURCE_DIR_WHEEL={wheel}"],
        ["cmake", "--build", str(build), "--target", "task_deadline_test", "-j4"],
    ):
        if not run(cmd):
            print(f"mutant {name}: build failed", file=sys.stderr)
            return 2
    killed = True
    for case in cases:
        rc = subprocess.run([str(build / "task_deadline_test"), case]).returncode
        print(f"mutant {name}: deadline_test {case} exited {rc}")
        killed = killed and rc == 1
    return 0 if killed else 1


if __name__ == "__main__":
    sys.exit(main())
