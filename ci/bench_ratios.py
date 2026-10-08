#!/usr/bin/env python3
"""Enforce the SHAPE of the benchmark results, not their absolute times.

Why shape and not thresholds
----------------------------
`docs/measures/complete-and-measured.md` D6 deliberately refuses to gate on absolute
latency: a time bound on shared CI hardware is flaky, and a flaky measure gets deleted.
A ratio between two cases measured in the SAME run on the SAME runner cancels machine
speed out, so it is stable where a threshold is not.

Why it must not fail on an improvement
--------------------------------------
A guard asserting "batch-1 is at least 50x slower than batch-1024" fails the day someone
makes small batches fast. That is a guard that blocks progress, so it is the wrong guard.
What must never regress is the ORDERING: a larger batch must never be slower than a
smaller one. That fails on a real regression and stays silent on any improvement.

Why the denominator is checked first
------------------------------------
A parser that finds nothing asserts nothing and exits 0. Three separate checks in this
program's history "passed" against an empty extraction. So this script states the
population it measured and fails when a benchmark it expects to exist is missing --
including when criterion's output layout changes under it.
"""

import json
import os
import sys

CRITERION = os.path.join("target", "criterion")

# Benchmarks that MUST be present. A missing one means either the bench was deleted or
# the run did not reach it -- both of which make every ratio below unfounded.
REQUIRED = [
    ("feed_poll_batch", ["1", "16", "256", "1024", "4000"]),
    ("feed_poll_per_poll_cost", ["200", "2000"]),
    ("feed_poll_vs_backlog", ["100", "1000", "10000"]),
    ("group_drain", ["poll_assign_then_ack", "poll_assign_only"]),
    ("subject_group_drain", ["drain_all_subjects"]),
]

# Monotonicity: within a group, time must not INCREASE as the parameter grows.
# Only these groups have a parameter where that is a meaningful claim.
#
# `tolerance` is multiplicative slack for run-to-run noise on a shared runner. 1.25 means
# a larger batch may measure up to 25% slower than a smaller one before it is called a
# regression -- wide enough that noise does not page, narrow enough that the 529x effect
# being inverted would.
MONOTONE_NON_INCREASING = [
    ("feed_poll_batch", ["1", "16", "256", "1024"], 1.25),
]


def mean_ns(group, bid):
    p = os.path.join(CRITERION, group, bid, "new", "estimates.json")
    if not os.path.exists(p):
        return None
    with open(p) as fh:
        return json.load(fh)["mean"]["point_estimate"]


def main():
    found, missing = {}, []
    for group, ids in REQUIRED:
        for bid in ids:
            v = mean_ns(group, bid)
            if v is None:
                missing.append(f"{group}/{bid}")
            else:
                found[f"{group}/{bid}"] = v

    expected = sum(len(ids) for _, ids in REQUIRED)
    print(f"benchmarks expected={expected} parsed={len(found)} missing={len(missing)}")
    if missing:
        print("MISSING (every ratio below would be computed from absent data):")
        for m in missing:
            print(f"  - {m}")
        print(
            "\nIf criterion's output layout changed, fix the parser. If a benchmark was "
            "renamed or removed, update REQUIRED in the same change set -- do not let "
            "this script silently measure a smaller population than it claims."
        )
        return 1

    print("\nrecorded means (ns) -- this run's baseline:")
    for k in sorted(found):
        print(f"  {k:<48} {found[k]:>18,.0f}")

    failures = []
    print("\nmonotonicity (a larger batch must never be slower):")
    for group, ids, tol in MONOTONE_NON_INCREASING:
        for a, b in zip(ids, ids[1:]):
            ta, tb = found[f"{group}/{a}"], found[f"{group}/{b}"]
            ok = tb <= ta * tol
            print(
                f"  {group}: {a} -> {b}  {ta:,.0f} -> {tb:,.0f} ns  "
                f"({'ok' if ok else 'REGRESSION'}, tolerance {tol}x)"
            )
            if not ok:
                failures.append(
                    f"{group}: batch {b} ({tb:,.0f} ns) is slower than batch {a} "
                    f"({ta:,.0f} ns) beyond the {tol}x noise tolerance"
                )

    # Reported, never gated: these are the headline findings, and a guard on them
    # would fail the day the drain path is improved.
    print("\nreported (NOT gated -- gating these would fail on an improvement):")
    b1, b1024 = found["feed_poll_batch/1"], found["feed_poll_batch/1024"]
    print(f"  batch 1 vs 1024 total-drain ratio       {b1 / b1024:>10.1f}x")
    b4000 = found["feed_poll_batch/4000"]
    print(f"  batch 1024 vs 4000 (poll-count control) {b1024 / b4000:>10.3f}x")
    pp200 = found["feed_poll_per_poll_cost/200"] / 200.0
    pp2000 = found["feed_poll_per_poll_cost/2000"] / 2000.0
    print(f"  per-poll cost 200 -> 2000 backlog       {pp2000 / pp200:>10.1f}x")
    ack = found["group_drain/poll_assign_then_ack"]
    noack = found["group_drain/poll_assign_only"]
    print(f"  lazy-ack penalty (no-ack / ack)         {noack / ack:>10.2f}x")

    if failures:
        print("\nFAILED:")
        for f in failures:
            print(f"  - {f}")
        return 1
    print("\nshape guards passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
