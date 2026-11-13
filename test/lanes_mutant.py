#!/usr/bin/env python3
# Copyright © 2026 Germán Méndez Bravo (Kronuz)
# SPDX-License-Identifier: MIT
"""Builds test/lanes_test.cc against a mutant copy of the headers and runs the test it targets.

`python3 test/lanes_mutant.py NAME` exits 0 when that test fails on a CHECK (exit 1), 1 when it
passes or dies otherwise, and 2 when the mutant does not apply or the build fails. The copy lives
in build-mutant-NAME/, so the checkout is untouched.
"""

from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# name: (header, original text, mutant text, test it must fail)
MUTANTS: dict[str, tuple[str, str, str, str]] = {
    "light-posts-demoted": (
        "task.hh",
        "\t\t\t\t} else {\n\t\t\t\t\ts.post(h);\n",
        "\t\t\t\t} else {\n\t\t\t\t\ts.post_demoted(h);\n",
        "light_overtakes_heavy",
    ),
    "drain-runs-all": (
        "task_asio.hh",
        "\t\tif (demoted_.empty()) {\n\t\t\tdraining_ = false;\n\t\t} else {\n\t\t\tasio::post(executor_, [this] { drain(); });\n",
        "\t\tif (demoted_.empty()) {\n\t\t\tdraining_ = false;\n\t\t} else {\n\t\t\tdrain();\n",
        "timer_within_two_heavy_turns",
    ),
    "strict-priority": (
        "task_asio.hh",
        "\t\tconst demoted_entry e = demoted_.front();\n",
        "\t\tif (io_) {\n\t\t\twhile (io_->context().poll_one()) {}\n\t\t}\n\t\tconst demoted_entry e = demoted_.front();\n",
        "heavy_progresses_under_flood",
    ),
    "post-ignores-lane": (
        "task.hh",
        "void post(std::coroutine_handle<> h) const { (heavy() ? vt()->post_heavy : vt()->post)(self_, h); }",
        "void post(std::coroutine_handle<> h) const { vt()->post(self_, h); }",
        "heavy_sticky_across_chore",
    ),
    "no-hop": (
        "task_asio.hh",
        "\t\tif (io_ && !io_->running_in_this_thread()) {  // from another thread: queue it from the loop\n"
        "\t\t\tasio::post(executor_, [this, e] { demote(e); });\n\t\t\treturn;\n\t\t}\n",
        "",
        "cross_thread_hop",
    ),
    "default-heavy": (
        "task.hh",
        "\t\t\t\tif (b.light_ == 0) {\n\t\t\t\t\ts.post_demoted(h);\n",
        "\t\t\t\tif (b.light_ == 0) {\n\t\t\t\t\ts.with_heavy(true).post(h);\n",
        "default_unchanged",
    ),
    "eq-compares-lane": (
        "task.hh",
        "{ return a.self_ == b.self_; }",
        "{ return a.self_ == b.self_ && a.vt_ == b.vt_; }",
        "equality_ignores_lane",
    ),
    "rearm-stays-heavy": (
        "task.hh",
        "\t\tspent_ = allowance < light_ ? light_ - allowance : 0;\n\t\tenter_light();\n",
        "\t\tspent_ = allowance < light_ ? light_ - allowance : 0;\n",
        "rearm_light_resumes_light",
    ),
    "rearm-keeps-count": (
        "task.hh",
        "\t\tspent_ = allowance < light_ ? light_ - allowance : 0;\n\t\tenter_light();\n",
        "\t\t(void)allowance;\n\t\tenter_light();\n",
        "when_all_join_lane",
    ),
    "heavy-late": (
        "task.hh",
        "b.spent_ >= b.light_ || s.heavy()",
        "b.spent_ > b.light_ || s.heavy()",
        "turns_heavy_at_light_slices",
    ),
    "inline-heavy": (
        "task_asio.hh",
        "if (!s.heavy() && inline_resume_depth < inline_resume_bound",
        "if (inline_resume_depth < inline_resume_bound",
        "await_asio_heavy_posts",
    ),
}


def main() -> int:
    if len(sys.argv) != 2 or sys.argv[1] not in MUTANTS:
        print(f"usage: lanes_mutant.py {{{','.join(MUTANTS)}}}", file=sys.stderr)
        return 2
    name = sys.argv[1]
    header, original, mutant, test = MUTANTS[name]
    work = ROOT / f"build-mutant-{name}"
    src = work / "src"
    shutil.rmtree(work, ignore_errors=True)
    src.mkdir(parents=True)
    for f in ("task.hh", "task_asio.hh", "CMakeLists.txt"):
        shutil.copy2(ROOT / f, src / f)
    for d in ("test", "bench"):
        shutil.copytree(ROOT / d, src / d)
    text = (src / header).read_text()
    if text.count(original) != 1:
        print(f"mutant {name}: the original text occurs {text.count(original)} times in {header}", file=sys.stderr)
        return 2
    (src / header).write_text(text.replace(original, mutant))
    build = work / "build"
    for cmd in (
        ["cmake", "-S", str(src), "-B", str(build), "-DCMAKE_BUILD_TYPE=RelWithDebInfo"],
        ["cmake", "--build", str(build), "--target", "task_lanes_test", "-j4"],
    ):
        if subprocess.run(cmd).returncode != 0:
            print(f"mutant {name}: build failed", file=sys.stderr)
            return 2
    rc = subprocess.run([str(build / "task_lanes_test"), test]).returncode
    print(f"mutant {name}: {test} exited {rc}")
    return 0 if rc == 1 else 1


if __name__ == "__main__":
    sys.exit(main())
