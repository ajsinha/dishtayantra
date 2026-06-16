#!/usr/bin/env python3
"""Trivial worker used only by dispatcher tests.

Accepts the same CLI shape the dispatcher emits, writes a summary JSON, and exits
0 immediately — so JobDispatchCalculator's launch / idempotency / concurrency /
completion-recording logic can be tested fast and deterministically without
spawning a full ComputeGraph child.
"""
import argparse
import json
import os
import sys


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--config")
    p.add_argument("--replay")
    p.add_argument("--expect", type=int, default=0)
    p.add_argument("--summary")
    p.add_argument("--job-id")
    p.add_argument("--quiet", action="store_true")
    a, _ = p.parse_known_args()
    if a.summary:
        os.makedirs(os.path.dirname(os.path.abspath(a.summary)), exist_ok=True)
        with open(a.summary, "w") as fh:
            json.dump({"job_id": a.job_id, "status": "ok",
                       "published": a.expect, "completed_by": "count"}, fh)
    return 0


if __name__ == "__main__":
    sys.exit(main())
