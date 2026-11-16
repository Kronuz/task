#!/usr/bin/env python3
# Copyright © 2026 Germán Méndez Bravo (Kronuz)
# SPDX-License-Identifier: MIT
"""Builds one timer test against a mutant copy of Kronuz/wheel's wheel.hh (at the pin
CMakeLists.txt fetches) or of task's headers, and runs the cases the mutant targets.

`python3 test/wheel_mutant.py NAME` exits 0 when every targeted case is killed (exits with the
mutant's `killed` code: 1 for a CHECK or a sanitizer report, 0 for a control that finds no
assertion), 1 when one is not, and 2 when the mutant does not apply or the build fails.
Everything lives in build-wheel-mutant-NAME/, so the checkout and the pin are untouched.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
ASAN = "-fsanitize=address -fno-omit-frame-pointer"


@dataclass(frozen=True)
class Mutant:
    header: str  # wheel.hh, task.hh or task_asio.hh
    original: str
    mutant: str
    target: str  # the CMake test executable
    cases: tuple[str, ...]
    killed: int = 1
    flags: str = ""  # CMAKE_CXX_FLAGS and linker flags
    also: tuple[
        tuple[str, str], ...
    ] = ()  # further (original, mutant) edits to the same header


ARM = "\t[[gnu::always_inline]] void arm(timer_entry& e, std::uint64_t deadline) noexcept {\n\t\towner_check();\n"

MUTANTS: dict[str, Mutant] = {
    # An entry a fire arms or re-arms joins the due list, so it fires in the same expire, early.
    "rearm-fires-early": Mutant(
        "wheel.hh",
        ARM,
        ARM
        + "\t\tif (expiring_) {\n\t\t\tif (e.armed()) { unlink(e); }\n\t\t\te.deadline = deadline;\n"
        "\t\t\tlink(e, due_);\n\t\t\treturn;\n\t\t}\n",
        "task_deadline_test",
        ("reentry", "model"),
    ),
    # X1: with nothing armed, drive() returns at once though may_wait() holds.
    "unarmed-returns": Mutant(
        "task.hh",
        "\t\t\t\twake_.wait(lock, queued);\n\t\t\t\tcontinue;\n",
        "\t\t\t\treturn false;\n",
        "task_blocking_test",
        ("wakes-unarmed",),
    ),
    # X8: drive() drains the queue before expiring, so a self-reposting call holds off timers.
    "expire-when-idle": Mutant(
        "task.hh",
        "for (std::size_t n = ready_.size(); n > 0; --n) {",
        "while (!ready_.empty()) {",
        "task_blocking_test",
        ("starve",),
    ),
    # F1: a deadline's fire that loses to an in-flight cross-thread cancel does not wait for its hop.
    "fire-skips-settle": Mutant(
        "task.hh",
        "if (expired && !s.request(cancel_reason::deadline, disarmed_bit)) { s.settle(); }",
        "if (expired) { s.request(cancel_reason::deadline, disarmed_bit); }",
        "task_blocking_test",
        ("race-forced",),
    ),
    # F2: neither the recheck after the fires nor the wait's predicate sees a post made meanwhile.
    "blocking-lost-wake": Mutant(
        "task.hh",
        "\t\t\tif (!ready_.empty()) { continue; }  // a fire, or another thread, queued work\n",
        "",
        "task_blocking_test",
        ("window-forced",),
        also=(
            (
                "wake_.wait_until(lock, until, queued);",
                "wake_.wait_until(lock, until);",
            ),
        ),
    ),
    # X4: a dying deadline calls into its destroyed scheduler (ASan reports the read).
    "drop-calls-dead-scheduler": Mutant(
        "task.hh",
        "\tif (!s->armed()) { return; }\n\tself->cancel(*s);\n\ts->release();\n",
        "\tif (self->cancel(*s)) { s->release(); }\n",
        "task_blocking_test",
        ("late",),
        flags=ASAN,
    ),
    # X7: blocking_scheduler::cancel off the driving thread goes unnamed.
    "blocking-cancel-unchecked": Mutant(
        "task.hh",
        '\t\tassert(runs_here() && "blocking_scheduler::cancel off the driving thread");\n',
        "",
        "task_blocking_test",
        ("offthread-cancel",),
        killed=0,
    ),
    # X5: asio_scheduler::arm is allowed off the loop once closed.
    "closed-arm-allowed": Mutant(
        "task_asio.hh",
        '\t\tassert((!io_ || runs_here()) && "asio_scheduler::arm off the loop");\n',
        '\t\tassert((!io_ || runs_here() || closed()) && "asio_scheduler::arm off the loop");\n',
        "task_deadline_asio_test",
        ("closed-arm",),
        killed=0,
    ),
}


def run(cmd: list[str]) -> bool:
    return subprocess.run(cmd, check=False).returncode == 0


def apply(path: Path, name: str, m: Mutant) -> bool:
    text = path.read_text()
    for original, mutant in ((m.original, m.mutant), *m.also):
        if text.count(original) != 1:
            print(
                f"mutant {name}: the original text occurs {text.count(original)} times in {m.header}",
                file=sys.stderr,
            )
            return False
        text = text.replace(original, mutant)
    path.write_text(text)
    return True


def main() -> int:
    if len(sys.argv) != 2 or sys.argv[1] not in MUTANTS:
        print(f"usage: wheel_mutant.py {{{','.join(MUTANTS)}}}", file=sys.stderr)
        return 2
    name = sys.argv[1]
    m = MUTANTS[name]
    work = ROOT / f"build-wheel-mutant-{name}"
    shutil.rmtree(work, ignore_errors=True)
    src = work / "src"
    src.mkdir(parents=True)
    for f in ("task.hh", "task_asio.hh", "CMakeLists.txt"):
        shutil.copy2(ROOT / f, src / f)
    for d in ("test", "bench"):
        shutil.copytree(ROOT / d, src / d)
    asio = "ON" if m.target == "task_deadline_asio_test" else "OFF"
    configure = [
        "cmake",
        "-S",
        str(src),
        "-B",
        str(work / "build"),
        f"-DTASK_ASIO={asio}",
        "-DCMAKE_BUILD_TYPE=Debug",
    ]
    if m.flags:
        configure += [
            f"-DCMAKE_CXX_FLAGS={m.flags}",
            f"-DCMAKE_EXE_LINKER_FLAGS={m.flags}",
        ]
    if m.header == "wheel.hh":
        fetch = work / "fetch"
        if not run(["cmake", "-S", str(src), "-B", str(fetch), "-DTASK_ASIO=OFF"]):
            print(f"mutant {name}: fetching the wheel failed", file=sys.stderr)
            return 2
        wheel = work / "wheel"
        shutil.copytree(
            fetch / "_deps" / "wheel-src", wheel, ignore=shutil.ignore_patterns(".git")
        )
        if not apply(wheel / "wheel.hh", name, m):
            return 2
        configure.append(f"-DFETCHCONTENT_SOURCE_DIR_WHEEL={wheel}")
    elif not apply(src / m.header, name, m):
        return 2
    for cmd in (
        configure,
        ["cmake", "--build", str(work / "build"), "--target", m.target, "-j4"],
    ):
        if not run(cmd):
            print(f"mutant {name}: build failed", file=sys.stderr)
            return 2
    killed = True
    for case in m.cases:
        env = {
            **os.environ,
            "ASAN_OPTIONS": "abort_on_error=0",
        }  # a report exits 1 on macOS too
        rc = subprocess.run(
            [str(work / "build" / m.target), case], env=env, check=False
        ).returncode
        print(f"mutant {name}: {m.target} {case} exited {rc} (killed at {m.killed})")
        killed = killed and rc == m.killed
    return 0 if killed else 1


if __name__ == "__main__":
    sys.exit(main())
