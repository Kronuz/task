"""Summarize wheel_bench.sh logs by docs/design/wheel-evidence.md's rule, as A1 applies it.

Per row, size, pad and arm: the median over rounds of each metric with its min-max; the A/A
band (a against a2 for single, f against f2 for cross); and t's ratio to the reference (a for
single, f for cross) against 1.10.
"""

from __future__ import annotations

import statistics
import sys
from collections import defaultdict

THRESHOLD = 1.10
METRICS: dict[str, list[str]] = {
    "single": ["arm_ns", "rearm_ns", "cancel_ns", "expire_ns", "score"],
    "cross": ["producer_ns", "span_ms", "max_late_ms", "last_after_deadline_ms"],
}
SIZE = {"single": "n", "cross": "p"}
PAIR = {"single": ("a", "a2", "score"), "cross": ("f", "f2", "producer_ns")}

Key = tuple[str, int, int]


def parse(paths: list[str]) -> dict[tuple[Key, str], list[dict[str, str]]]:
    runs: dict[tuple[Key, str], list[dict[str, str]]] = defaultdict(list)
    for path in paths:
        with open(path) as f:
            for line in f:
                if not line.startswith("row="):
                    continue
                fields = dict(kv.split("=", 1) for kv in line.split())
                row = fields["row"]
                key = (row, int(fields[SIZE[row]]), int(fields.get("pad", "0")))
                runs[(key, fields["arm"])].append(fields)
    return runs


def spread(values: list[float]) -> float:
    m = statistics.median(values)
    return (max(values) - min(values)) / m if m else 0.0


def main(paths: list[str]) -> None:
    by_key: dict[Key, dict[str, list[dict[str, str]]]] = defaultdict(dict)
    for (key, arm), rs in parse(paths).items():
        by_key[key][arm] = rs
    for (row, size, pad), arms in sorted(by_key.items()):
        print(f"\n{row} {SIZE[row]}={size}" + (f" pad={pad}" if pad else ""))
        medians: dict[str, float] = {}
        values: dict[str, list[float]] = {}
        first, second, key = PAIR[row]
        for arm in sorted(arms):
            cells = []
            for metric in METRICS[row]:
                xs = [float(r[metric]) for r in arms[arm]]
                cells.append(f"{metric}={statistics.median(xs):.2f} [{min(xs):.2f}-{max(xs):.2f}]")
            if row == "cross":
                cells.append(f"valid={all(r['valid'] == '1' for r in arms[arm])}")
            print(f"  {arm:3} runs={len(arms[arm])} " + " ".join(cells))
            values[arm] = [float(r[key]) for r in arms[arm]]
            medians[arm] = statistics.median(values[arm])
        band = 0.0
        if first in medians and second in medians:
            band = max(
                abs(medians[first] - medians[second]) / medians[first],
                spread(values[first]),
                spread(values[second]),
            )
            print(f"  A/A band on {key}: {band:.1%} ({first} {medians[first]:.2f}, {second} {medians[second]:.2f})")
        if first not in medians:
            continue
        for arm in sorted(medians):
            if arm in (first, second):
                continue
            ratio = medians[arm] / medians[first]
            if abs(ratio - THRESHOLD) < band:
                verdict = "inconclusive"
            elif ratio <= THRESHOLD:
                verdict = "pass"
            else:
                verdict = "fail"
            print(f"  {arm}: {key} {medians[arm]:.2f} = {ratio:.3f}x {first}: {verdict}")


if __name__ == "__main__":
    main(sys.argv[1:])
