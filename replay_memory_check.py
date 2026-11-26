"""Streaming memory acceptance scenario for replay_window.

Run in a fresh process:

    python3 replay_memory_check.py --records 70000 --state-bytes 1200

The record generator never materializes the archive.  The consumer reconstructs
each expected value transiently and keeps only counters.
"""
import argparse
import json
import os
from pathlib import Path
import resource
import sys
import tempfile

sys.path.insert(0, str(Path(__file__).resolve().parent))

from contracts import Field, Registry, replay_window


LIMIT_KIB = 64 * 1024  # 64 MiB, reported by Linux getrusage in KiB


def make_state(offset, state_bytes):
    prefix = f"s{offset:08d}:"
    return prefix + "A" * (state_bytes - len(prefix))


def make_event_id(offset):
    return f"e{offset:08d}"


def streaming_records(count, state_bytes, revision, codec):
    # One record in a huge window; offset 1 is its first gap.
    yield ("p1", 0, revision,
           codec.encode({"eid": "first-of-billion-window", "body": "only"}))

    # Deliberately reverse coordinate order and do not retain prior items.
    for offset in range(count - 1, -1, -1):
        event_id = make_event_id(offset)
        state = make_state(offset, state_bytes)
        yield ("p0", offset, revision,
               codec.encode({"eid": event_id, "body": state}))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--records", type=int, default=70_000)
    parser.add_argument("--state-bytes", type=int, default=1200)
    args = parser.parse_args()

    registry = Registry()
    revision = registry.register(
        {"eid": Field(str), "body": Field(str, "")},
        aliases={"eid": "event_id", "body": "state"},
    )
    codec = registry.codec(revision)

    windows = {
        "p0": (0, args.records),
        "p1": (0, 1_000_000_000),
    }

    p0_delivered = 0
    p1_delivered = 0
    last_p0_offset = -1

    def deliver(partition, offset, event_id, state):
        nonlocal p0_delivered, p1_delivered, last_p0_offset
        if partition == "p0":
            # Representatives must arrive in ascending coordinate order even
            # though the generator supplied descending offsets.
            assert offset == last_p0_offset + 1
            assert offset == p0_delivered
            assert event_id == make_event_id(offset)
            assert state == make_state(offset, args.state_bytes)
            last_p0_offset = offset
            p0_delivered += 1
        else:
            assert partition == "p1"
            assert offset == 0
            assert event_id == "first-of-billion-window"
            assert state == "only"
            p1_delivered += 1

    with tempfile.TemporaryDirectory(prefix="replay-memory-scratch-") as scratch:
        next_offsets, blocked = replay_window(
            registry,
            streaming_records(args.records, args.state_bytes, revision, codec),
            windows,
            deliver,
            scratch_dir=scratch,
        )
        leftover = os.listdir(scratch)

    assert p0_delivered == args.records, p0_delivered
    assert p1_delivered == 1, p1_delivered
    assert next_offsets == {"p0": args.records, "p1": 1}, next_offsets
    assert blocked == {"p1": "gap"}, blocked
    assert leftover == [], leftover

    peak_rss_kib = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    result = {
        "records": args.records,
        "state_bytes": args.state_bytes,
        "delivered": p0_delivered + p1_delivered,
        "next_offsets": next_offsets,
        "blocked": blocked,
        "peak_rss_kib": peak_rss_kib,
        "limit_kib": LIMIT_KIB,
        "within_64_mib": peak_rss_kib <= LIMIT_KIB,
    }
    print(json.dumps(result, sort_keys=True))
    if peak_rss_kib > LIMIT_KIB:
        raise SystemExit(f"peak RSS {peak_rss_kib} KiB exceeds {LIMIT_KIB} KiB")


if __name__ == "__main__":
    main()
