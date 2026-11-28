"""Streaming memory/quota acceptance for compact_changelog (fresh process).

    python3 compaction_memory_check.py --scenario reread
    python3 compaction_memory_check.py --scenario scratch
    python3 compaction_memory_check.py --scenario reread  --failure sink-tail
    python3 compaction_memory_check.py --scenario scratch --failure read-last

Two partitions hold 36,000 changes each: 12,000 key writes, 12,000 overwrites
of the same keys, then 12,000 writes to different keys.  Both records at one
offset share a transaction.  Values are 2048 ASCII bytes derived from a
per-run private seed by (revision, partition, offset), so the content is not
compressible and the sink can regenerate and verify every byte it receives.
Neither the source nor the sink retains the full data in the process.

Scenarios:
  reread  -- no scratch; cold-source quota 120,000 reads (one full scan plus
             exactly the checkpoint winners and tail records again).
  scratch -- cold-source quota 72,000 reads with each index supplied at most
             once; a 160 MiB disk staging area is provided.

Failure modes:
  sink-tail -- the sink raises the caller's exception object on the first
               legal tail record; the batch must be discarded, never sealed.
  read-last -- the first read of the 72,000th distinct index raises; the
               whole run must deliver nothing.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import random
import resource
import sys
import tempfile

sys.path.insert(0, str(Path(__file__).resolve().parent))

from contracts import Field, Registry, compact_changelog


LIMIT_KIB = 64 * 1024  # 64 MiB, reported by Linux getrusage in KiB
PARTITIONS = ("a", "b")
OFFSETS = 36_000
BATCH = 12_000
CUT = 24_000
VALUE_BYTES = 2048
RECORDS = 2 * OFFSETS  # 72,000
SCRATCH_CAPACITY = 160 * 1024 * 1024  # 160 MiB
_ALPHABET = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
_TRANSLATE = bytes(_ALPHABET[b % len(_ALPHABET)] for b in range(256))


class Boom(RuntimeError):
    pass


class QuotaExceeded(RuntimeError):
    pass


class ScratchFull(RuntimeError):
    pass


def value_bytes(seed, revision, partition, offset):
    material = f"{seed}:{revision}:{partition}:{offset}".encode("ascii")
    return hashlib.shake_256(material).digest(VALUE_BYTES).translate(_TRANSLATE)


def key_for(offset):
    if offset < BATCH:
        return f"k{offset:05d}"
    if offset < 2 * BATCH:
        return f"k{offset - BATCH:05d}"
    return f"n{offset - 2 * BATCH:05d}"


def payload_for(seed, revision, partition, offset):
    return (b'{"k":"' + key_for(offset).encode("ascii")
            + b'","d":false,"v":"'
            + value_bytes(seed, revision, partition, offset) + b'"}')


def locate(index):
    """Record index -> (partition, offset); the input is generated in reverse."""
    forward = RECORDS - 1 - index
    offset, is_b = divmod(forward, 2)
    return ("b" if is_b else "a"), offset


class ColdSource:
    """Generates records deterministically; enforces the read quota."""

    def __init__(self, seed, revision, quota, once, fail_at_distinct=None):
        self.seed = seed
        self.revision = revision
        self.quota = quota
        self.once = once
        self.fail_at_distinct = fail_at_distinct
        self.reads = 0
        self.distinct = 0
        self.supplied = bytearray(RECORDS)

    def __call__(self, index):
        if not 0 <= index < RECORDS:
            raise ValueError("record index out of range")
        if (self.fail_at_distinct is not None and not self.supplied[index]
                and self.distinct + 1 >= self.fail_at_distinct):
            raise Boom("cold source read failure")
        if self.once and self.supplied[index]:
            raise QuotaExceeded("record index supplied twice")
        if self.reads >= self.quota:
            raise QuotaExceeded("cold read quota exhausted")
        self.reads += 1
        if not self.supplied[index]:
            self.supplied[index] = 1
            self.distinct += 1
        partition, offset = locate(index)
        return (partition, offset, offset + 1, self.revision,
                payload_for(self.seed, self.revision, partition, offset))


class VerifyingSink:
    """Streams verification of every delivered byte; keeps only counters."""

    def __init__(self, seed, revision, boom=None):
        self.seed = seed
        self.revision = revision
        self.boom = boom
        self.checkpoint_counts = {partition: 0 for partition in PARTITIONS}
        self.seen_keys = {partition: bytearray(BATCH) for partition in PARTITIONS}
        self.tail_next = {partition: CUT for partition in PARTITIONS}
        self.tail_count = 0
        self.sealed_cuts = None
        self.discarded = 0

    def checkpoint(self, partition, key, value):
        assert partition in PARTITIONS, partition
        number = int(key[1:]) if key.startswith("k") else -1
        assert 0 <= number < BATCH and key == f"k{number:05d}", key
        assert not self.seen_keys[partition][number], key
        self.seen_keys[partition][number] = 1
        # Every live key must come from the overwrite record of that key.
        expected = value_bytes(
            self.seed, self.revision, partition, BATCH + number).decode("ascii")
        assert value == expected, (partition, key)
        self.checkpoint_counts[partition] += 1

    def tail(self, record):
        if self.boom is not None and self.tail_count == 0:
            raise self.boom
        partition, offset, txn_id, revision, payload = record
        assert partition in PARTITIONS, partition
        assert offset == self.tail_next[partition], (partition, offset)
        self.tail_next[partition] += 1
        assert txn_id == offset + 1, txn_id
        assert revision == self.revision, revision
        assert payload == payload_for(self.seed, self.revision, partition, offset)
        self.tail_count += 1

    def seal(self, cuts):
        self.sealed_cuts = cuts

    def discard(self):
        self.discarded += 1


class DiskScratch:
    """Caller-provided one-shot byte staging on a temporary directory."""

    def __init__(self, directory, capacity):
        self.directory = directory
        self.capacity = capacity
        self.used = 0
        self.peak = 0
        self.present = bytearray(RECORDS)

    def _path(self, key):
        return os.path.join(self.directory, str(key))

    def put(self, key, data):
        old = os.path.getsize(self._path(key)) if self.present[key] else 0
        if self.used - old + len(data) > self.capacity:
            raise ScratchFull("scratch capacity exceeded")
        with open(self._path(key), "wb") as handle:
            handle.write(data)
        self.used += len(data) - old
        self.present[key] = 1
        self.peak = max(self.peak, self.used)

    def get(self, key):
        with open(self._path(key), "rb") as handle:
            return handle.read()

    def drop(self, key):
        size = os.path.getsize(self._path(key))
        os.unlink(self._path(key))
        self.present[key] = 0
        self.used -= size


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--scenario", choices=("reread", "scratch"), required=True)
    parser.add_argument("--failure", choices=("none", "sink-tail", "read-last"),
                        default="none")
    args = parser.parse_args()

    seed = random.SystemRandom().getrandbits(63)
    registry = Registry()
    revision = registry.register(
        {"k": Field(str, ""), "d": Field(bool, False), "v": Field(str, "")},
        aliases={"k": "key", "d": "deleted", "v": "value"})

    marks = {partition: {0, CUT, OFFSETS} for partition in PARTITIONS}
    fences = {partition: CUT for partition in PARTITIONS}
    marks_snapshot = {partition: set(marks[partition]) for partition in PARTITIONS}
    fences_snapshot = dict(fences)

    # Budgets exactly fit the live keys and the retained original tail.
    checkpoint_budget = 0
    for partition in PARTITIONS:
        for number in range(BATCH):
            value = value_bytes(
                seed, revision, partition, BATCH + number).decode("ascii")
            checkpoint_budget += len(json.dumps(
                [partition, f"k{number:05d}", value],
                ensure_ascii=True, separators=(",", ":")).encode("ascii"))
    tail_budget = sum(
        len(payload_for(seed, revision, partition, offset))
        for partition in PARTITIONS for offset in range(CUT, OFFSETS))

    quota = 120_000 if args.scenario == "reread" else 72_000
    source = ColdSource(
        seed, revision, quota, once=args.scenario == "scratch",
        fail_at_distinct=RECORDS if args.failure == "read-last" else None)
    boom = Boom("caller boom") if args.failure == "sink-tail" else None
    sink = VerifyingSink(seed, revision, boom=boom)

    result = {"scenario": args.scenario, "failure": args.failure, "seed": seed}
    scratch = None
    with tempfile.TemporaryDirectory(prefix="compaction-scratch-") as scratch_dir:
        if args.scenario == "scratch":
            scratch = DiskScratch(scratch_dir, SCRATCH_CAPACITY)
        try:
            cuts = compact_changelog(
                registry, RECORDS, source, marks, fences, sink,
                max_checkpoint_bytes=checkpoint_budget,
                max_tail_bytes=tail_budget, scratch=scratch)
        except Boom as error:
            assert args.failure != "none", "unexpected cold/sink failure"
            if args.failure == "sink-tail":
                assert error is boom, "the caller's exception object must propagate"
            result["error"] = type(error).__name__
        else:
            assert args.failure == "none", "failure injection did not happen"
            result["cuts"] = cuts

        result.update({
            "reads": source.reads,
            "distinct_reads": source.distinct,
            "checkpoints": sum(sink.checkpoint_counts.values()),
            "checkpoint_counts": sink.checkpoint_counts,
            "tails": sink.tail_count,
            "discarded": sink.discarded,
            "sealed": sink.sealed_cuts,
            "scratch_peak": scratch.peak if scratch else 0,
            "scratch_used": scratch.used if scratch else 0,
            "scratch_leftover": len(os.listdir(scratch_dir)),
        })

    # ---- scenario assertions ----
    assert marks == marks_snapshot and fences == fences_snapshot
    assert len(registry._versions) == 1
    if args.failure == "none":
        assert result["cuts"] == {"a": CUT, "b": CUT}, result["cuts"]
        assert result["sealed"] == {"a": CUT, "b": CUT}
        assert result["checkpoint_counts"] == {"a": BATCH, "b": BATCH}
        assert result["tails"] == 2 * BATCH
        assert result["discarded"] == 0
        expected_reads = 120_000 if args.scenario == "reread" else 72_000
        assert result["reads"] == expected_reads, result["reads"]
        assert result["distinct_reads"] == RECORDS
    elif args.failure == "sink-tail":
        assert result["sealed"] is None
        assert result["discarded"] == 1
        assert result["tails"] == 0
        assert result["checkpoint_counts"] == {"a": BATCH, "b": BATCH}
    else:  # read-last
        assert result["sealed"] is None
        assert result["discarded"] == 0
        assert result["checkpoints"] == 0
        assert result["tails"] == 0
    assert result["scratch_peak"] <= SCRATCH_CAPACITY
    assert result["scratch_used"] == 0
    assert result["scratch_leftover"] == 0

    peak_rss_kib = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    result["peak_rss_kib"] = peak_rss_kib
    result["limit_kib"] = LIMIT_KIB
    result["within_64_mib"] = peak_rss_kib <= LIMIT_KIB
    print(json.dumps(result, sort_keys=True))
    if peak_rss_kib > LIMIT_KIB:
        raise SystemExit(f"peak RSS {peak_rss_kib} KiB exceeds {LIMIT_KIB} KiB")


if __name__ == "__main__":
    main()
