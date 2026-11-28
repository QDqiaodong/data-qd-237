"""Acceptance and behaviour tests for contracts.compact_changelog."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from unittest import TestCase, skipUnless

from contracts import (
    CompactionError,
    Field,
    PolicyViolation,
    Registry,
    _changelog_logical_values,
    compact_changelog,
)


class Boom(RuntimeError):
    pass


def acceptance_registry():
    """Two changelog revisions with disjoint physical names.

    ``rev_b`` reads an omitted value as ``"ready"`` through its own default,
    while its current production limit forbids emitting that very value; the
    changelog reader must still use the receiving rules, not the production
    limit.
    """
    registry = Registry()
    rev_a = registry.register(
        {"ak": Field(str, ""), "ad": Field(bool, False), "av": Field(str, "")},
        aliases={"ak": "key", "ad": "deleted", "av": "value"})
    rev_b = registry.register(
        {"bk": Field(str, ""), "bd": Field(bool, False),
         "bv": Field(str, "ready")},
        aliases={"bk": "key", "bd": "deleted", "bv": "value"})
    registry.set_emit_policy(rev_b, [[("ne", "bv", "ready")]])
    return registry, rev_a, rev_b


LONG_VALUE = "l" * 200
PAYLOAD_A0 = json.dumps({"ak": "x", "ad": False, "av": LONG_VALUE},
                        sort_keys=True, separators=(",", ":")).encode("ascii")
PAYLOAD_A1 = b'{"ak":"x","ad":true}'
# Deliberately non-canonical whitespace; the tail must keep these exact bytes.
PAYLOAD_A2 = b'{  "ak": "y", "ad": false, "av": "" }'
PAYLOAD_B0 = b'{"bk":"b","bd":false}'
PAYLOAD_B1 = b'{"bk":"b","bd":true}'
assert len(PAYLOAD_A2) >= len(PAYLOAD_B0)


def acceptance_records(rev_a, rev_b):
    return [
        ("a", 0, 1, rev_a, PAYLOAD_A0),   # txn 1: write x = long value
        ("a", 1, 2, rev_a, PAYLOAD_A1),   # txn 2: delete x
        ("a", 2, 3, rev_a, PAYLOAD_A2),   # txn 3: write y = ""
        ("b", 0, 2, rev_b, PAYLOAD_B0),   # txn 2: write b (value defaults)
        ("b", 1, 4, rev_b, PAYLOAD_B1),   # txn 4: delete b
    ]


CHECKPOINT_BUDGET = len(json.dumps(["b", "b", "ready"], ensure_ascii=True,
                                   separators=(",", ":")).encode("ascii"))
TAIL_BUDGET = len(PAYLOAD_A2) + len(PAYLOAD_B1)


class RecordingSink:
    def __init__(self):
        self.checkpoints = []
        self.tails = []
        self.sealed = None
        self.discarded = 0

    def checkpoint(self, partition, key, value):
        self.checkpoints.append((partition, key, value))

    def tail(self, record):
        self.tails.append(record)

    def seal(self, cuts):
        self.sealed = cuts

    def discard(self):
        self.discarded += 1


class CountingRead:
    def __init__(self, records):
        self.records = records
        self.calls = []

    def __call__(self, index):
        self.calls.append(index)
        return self.records[index]


def recover(checkpoints, tails, registry):
    """Apply a sealed batch from empty state, transactions in id order."""
    state = {}
    for partition, key, value in checkpoints:
        state[(partition, key)] = value
    for partition, _offset, _txn, revision, payload in sorted(
            tails, key=lambda record: record[2]):
        key, deleted, value = _changelog_logical_values(
            registry, revision, payload)
        if deleted:
            state.pop((partition, key), None)
        else:
            state[(partition, key)] = value
    return state


class AcceptanceScenarioTest(TestCase):
    """Acceptance 1: only the replica protection line moves."""

    def setUp(self):
        self.registry, self.rev_a, self.rev_b = acceptance_registry()
        self.records = acceptance_records(self.rev_a, self.rev_b)
        self.marks = {"a": {0, 1, 2, 3}, "b": {0, 1, 2}}
        self.fence = {"a": 3, "b": 1}

    def compact(self, read, marks=None, fence=None, sink=None,
                checkpoint_budget=CHECKPOINT_BUDGET, tail_budget=TAIL_BUDGET,
                scratch=None):
        return compact_changelog(
            self.registry, len(self.records), read,
            marks if marks is not None else self.marks,
            fence if fence is not None else self.fence,
            sink if sink is not None else RecordingSink(),
            max_checkpoint_bytes=checkpoint_budget,
            max_tail_bytes=tail_budget, scratch=scratch)

    def test_seals_the_fence_limited_cuts(self):
        sink = RecordingSink()
        read = CountingRead(self.records)
        cuts = self.compact(read, sink=sink)
        self.assertEqual(cuts, {"a": 2, "b": 1})
        self.assertEqual(sink.sealed, {"a": 2, "b": 1})
        self.assertEqual(sink.discarded, 0)
        # The checkpoint holds only b's "ready" (read through the default of
        # a revision currently forbidden from producing that value).
        self.assertEqual(sink.checkpoints, [("b", "b", "ready")])
        # The tail keeps transactions 3 and 4 with their original bytes.
        self.assertEqual(sink.tails, [
            ("a", 2, 3, self.rev_a, PAYLOAD_A2),
            ("b", 1, 4, self.rev_b, PAYLOAD_B1),
        ])
        self.assertIn(b" ", sink.tails[0][4])  # non-canonical whitespace kept
        # Recovery from the checkpoint plus the tail leaves only a's y:"".
        self.assertEqual(recover(sink.checkpoints, sink.tails, self.registry),
                         {("a", "y"): ""})
        # Every record was read exactly once in one pass, then only the
        # delivered records were read again.
        self.assertEqual(read.calls[:5], [0, 1, 2, 3, 4])
        self.assertEqual(sorted(read.calls[5:]), sorted(
            [self.records.index(("b", 0, 2, self.rev_b, PAYLOAD_B0)),
             self.records.index(("a", 2, 3, self.rev_a, PAYLOAD_A2)),
             self.records.index(("b", 1, 4, self.rev_b, PAYLOAD_B1))]))

    def test_shuffled_input_and_empty_partition(self):
        order = [3, 0, 4, 1, 2]
        shuffled = [self.records[i] for i in order]
        sink = RecordingSink()
        marks = {"a": {0, 1, 2, 3}, "b": {0, 1, 2}, "e": {0}}
        fence = {"a": 3, "b": 1, "e": 0}
        cuts = compact_changelog(
            self.registry, len(shuffled), CountingRead(shuffled),
            marks, fence, sink,
            max_checkpoint_bytes=CHECKPOINT_BUDGET, max_tail_bytes=TAIL_BUDGET)
        self.assertEqual(cuts, {"a": 2, "b": 1, "e": 0})
        self.assertEqual(sink.sealed, {"a": 2, "b": 1, "e": 0})
        self.assertEqual(sink.checkpoints, [("b", "b", "ready")])
        self.assertEqual([record[:3] for record in sink.tails],
                         [("a", 2, 3), ("b", 1, 4)])

    def test_fence_blocking_replica_forces_no_fit_without_delivery(self):
        # a:3, b:0 fits both byte budgets on paper but splits transaction 2.
        sink = RecordingSink()
        with self.assertRaises(CompactionError) as ctx:
            self.compact(CountingRead(self.records),
                         fence={"a": 3, "b": 0}, sink=sink)
        self.assertEqual(ctx.exception.reason, "no_fit")
        self.assertIsInstance(ctx.exception, ValueError)
        self.assertEqual(sink.checkpoints, [])
        self.assertEqual(sink.tails, [])
        self.assertIsNone(sink.sealed)
        self.assertEqual(sink.discarded, 0)

    def test_removing_the_needed_restart_mark_forces_no_fit(self):
        sink = RecordingSink()
        with self.assertRaises(CompactionError) as ctx:
            self.compact(CountingRead(self.records),
                         marks={"a": {0, 1, 3}, "b": {0, 1, 2}}, sink=sink)
        self.assertEqual(ctx.exception.reason, "no_fit")
        self.assertEqual(sink.checkpoints, [])
        self.assertEqual(sink.tails, [])
        self.assertIsNone(sink.sealed)

    def test_zero_budgets_still_check_every_record(self):
        # With both budgets at 0 the only cut candidate that fits the tail is
        # impossible here, but a bad final record must be a bad record, not a
        # no_fit verdict.
        bad_key = list(self.records)
        bad_key[-1] = ("b", 1, 4, self.rev_b, b'{"bd":true}')  # key defaulted
        duplicate = list(self.records)
        duplicate[-1] = ("b", 1, 4, self.rev_b,
                         b'{"bk":"b","bk":"b","bd":true}')
        registry, rev_a, rev_b = self.registry, self.rev_a, self.rev_b
        rev_c = registry.register(
            {"ck": Field(str, ""), "cd": Field(bool, False),
             "cv": Field(str, "")},
            aliases={"ck": "key", "cd": "deleted", "cv": "value"},
            policy=[[("absent", "ck", None), ("eq", "ck", "good")]])
        violating = list(self.records)
        violating[-1] = ("b", 1, 4, rev_c, b'{"ck":"b","cd":true}')
        for records in (bad_key, duplicate, violating):
            sink = RecordingSink()
            with self.assertRaises(ValueError, msg=records[-1]) as ctx:
                compact_changelog(
                    self.registry, len(records), CountingRead(records),
                    self.marks, self.fence, sink,
                    max_checkpoint_bytes=0, max_tail_bytes=0)
            self.assertNotIsInstance(ctx.exception, CompactionError)
            self.assertEqual(sink.checkpoints, [])
            self.assertEqual(sink.tails, [])
            self.assertIsNone(sink.sealed)
        # The policy violation surfaces as a read-policy PolicyViolation.
        sink = RecordingSink()
        with self.assertRaises(PolicyViolation) as ctx:
            compact_changelog(
                self.registry, len(violating), CountingRead(violating),
                self.marks, self.fence, sink,
                max_checkpoint_bytes=0, max_tail_bytes=0)
        self.assertEqual(ctx.exception.policy_kind, "read")

    def test_duplicate_coordinates_are_rejected(self):
        records = list(self.records)
        records[2] = ("a", 1, 3, self.rev_a, PAYLOAD_A2)  # repeats ("a", 1)
        with self.assertRaises(ValueError):
            self.compact(CountingRead(records))
        records = list(self.records)
        records[4] = ("a", 2, 4, self.rev_b, PAYLOAD_B1)  # repeats ("a", 2)
        with self.assertRaises(ValueError):
            self.compact(CountingRead(records))

    def test_unknown_revision_is_key_error(self):
        records = list(self.records)
        records[-1] = ("b", 1, 4, 99, PAYLOAD_B1)
        with self.assertRaises(KeyError):
            self.compact(CountingRead(records))
        sink = RecordingSink()
        cuts = self.compact(CountingRead(self.records), sink=sink)
        self.assertEqual(cuts, {"a": 2, "b": 1})  # fixture itself is valid

    def test_inputs_and_registry_are_not_mutated(self):
        marks = {"a": {0, 1, 2, 3}, "b": {0, 1, 2}}
        fence = {"a": 3, "b": 0}
        snapshot_marks = {name: set(marks[name]) for name in marks}
        snapshot_fence = dict(fence)
        records_snapshot = list(self.records)
        with self.assertRaises(CompactionError):
            self.compact(CountingRead(self.records), marks=marks, fence=fence)
        self.assertEqual({name: set(marks[name]) for name in marks},
                         snapshot_marks)
        self.assertEqual(fence, snapshot_fence)
        self.assertEqual(self.records, records_snapshot)
        self.assertEqual(len(self.registry._versions), 2)


class ValidationTest(TestCase):
    def setUp(self):
        self.registry, self.rev_a, self.rev_b = acceptance_registry()
        self.records = acceptance_records(self.rev_a, self.rev_b)
        self.kwargs = dict(
            registry=self.registry, count=len(self.records),
            read=CountingRead(self.records),
            restart_marks={"a": {0, 1, 2, 3}, "b": {0, 1, 2}},
            replica_fence={"a": 3, "b": 1}, sink=RecordingSink(),
            max_checkpoint_bytes=CHECKPOINT_BUDGET, max_tail_bytes=TAIL_BUDGET)

    def compact(self, **overrides):
        call = dict(self.kwargs)
        call.update(overrides)
        registry = call.pop("registry")
        count = call.pop("count")
        read = call.pop("read")
        marks = call.pop("restart_marks")
        fence = call.pop("replica_fence")
        sink = call.pop("sink")
        return compact_changelog(registry, count, read, marks, fence, sink,
                                 **call)

    def assert_rejected(self, **overrides):
        with self.assertRaises(ValueError, msg=repr(overrides)):
            self.compact(**overrides)

    def test_argument_validation(self):
        for count in (-1, 80_001, True, 2.5, "5"):
            self.assert_rejected(count=count)
        for budget in (-1, True, 1.5, "0"):
            self.assert_rejected(max_checkpoint_bytes=budget)
            self.assert_rejected(max_tail_bytes=budget)
        self.assert_rejected(read=None)
        self.assert_rejected(read=42)
        self.assert_rejected(restart_marks=[("a", {0})])
        self.assert_rejected(replica_fence=[("a", 0)])
        self.assert_rejected(restart_marks={"a": {0}}, replica_fence={"b": 0})
        self.assert_rejected(restart_marks={})
        self.assert_rejected(restart_marks={f"p{i}": {0} for i in range(13)},
                             replica_fence={f"p{i}": 0 for i in range(13)})
        self.assert_rejected(restart_marks={"a!": {0}}, replica_fence={"a!": 0})
        self.assert_rejected(restart_marks={"a": [0, 1]},
                             replica_fence={"a": 1})
        self.assert_rejected(restart_marks={"a": {1}}, replica_fence={"a": 1})
        self.assert_rejected(restart_marks={"a": set(range(17))},
                             replica_fence={"a": 16})
        self.assert_rejected(restart_marks={"a": {0, True}},
                             replica_fence={"a": 1})
        self.assert_rejected(restart_marks={"a": {0, -1}},
                             replica_fence={"a": 1})
        self.assert_rejected(replica_fence={"a": True, "b": 1})
        self.assert_rejected(replica_fence={"a": -1, "b": 1})
        # Fence or marks beyond the partition end.
        self.assert_rejected(replica_fence={"a": 4, "b": 1})
        self.assert_rejected(restart_marks={"a": {0, 4}, "b": {0, 1, 2}})
        self.assert_rejected(sink=object())
        self.assert_rejected(scratch=object())

    def test_record_structure_validation(self):
        bad_records = [
            [("a", 0, 1, self.rev_a)],                       # not five items
            [("a", 0, 1, self.rev_a, PAYLOAD_A0, "extra")],
            [["a", 0, 1, self.rev_a, PAYLOAD_A0]],           # not a tuple
            [("a!", 0, 1, self.rev_a, PAYLOAD_A0)],          # bad partition
            [("c", 0, 1, self.rev_a, PAYLOAD_A0)],           # uncovered
            [("a", True, 1, self.rev_a, PAYLOAD_A0)],        # bool offset
            [("a", -1, 1, self.rev_a, PAYLOAD_A0)],
            [("a", 2**63, 1, self.rev_a, PAYLOAD_A0)],
            [("a", 0, True, self.rev_a, PAYLOAD_A0)],        # bool txn
            [("a", 0, 0, self.rev_a, PAYLOAD_A0)],           # txn not positive
            [("a", 0, 2**63, self.rev_a, PAYLOAD_A0)],
            [("a", 0, 1, True, PAYLOAD_A0)],                 # bool revision
            [("a", 0, 1, 0, PAYLOAD_A0)],                    # revision not +
            [("a", 0, 1, self.rev_a, "not-bytes")],
            [("a", 0, 1, self.rev_a, bytearray(PAYLOAD_A0))],
            [("a", 0, 1, self.rev_a, b"x" * 4097)],
        ]
        for bad in bad_records:
            with self.assertRaises(ValueError, msg=repr(bad)):
                self.compact(count=len(bad), read=CountingRead(bad))

    def test_logical_field_validation(self):
        registry = Registry()
        rev = registry.register(
            {"k": Field(str, ""), "d": Field(bool, False),
             "v": Field(str, "")},
            aliases={"k": "key", "d": "deleted", "v": "value"})

        def one_record(payload, revision=rev, registry=registry):
            return compact_changelog(
                registry, 1, CountingRead([("p", 0, 1, revision, payload)]),
                {"p": {0, 1}}, {"p": 1}, RecordingSink(),
                max_checkpoint_bytes=10**9, max_tail_bytes=10**9)

        for payload in (
            b'{"d":false,"v":"x"}',                    # key physically absent
            b'{"k":"x","v":"x"}',                      # deleted physically absent
            b'{"k":"","d":false,"v":"x"}',             # empty key
            b'{"k":"' + b"x" * 129 + b'","d":false}',  # key too long
            b'{"k":"x","d":false,"v":"' + b"x" * 3073 + b'"}',  # value too long
            b'{"k":"x","d":false,"v":1}',              # wrong value type
            b'{"k":1,"d":false}',                      # wrong key type
            b'{"k":"x","d":0}',                        # wrong deleted type
            b'{"k":"x","d":false,"k":"y"}',            # duplicate JSON key
            b'not json',
            b'[1, 2]',
        ):
            with self.assertRaises(ValueError, msg=payload[:40]):
                one_record(payload)
        # A delete may omit value and may target a missing key.
        cuts = one_record(b'{"k":"ghost","d":true}')
        self.assertEqual(cuts, {"p": 1})
        # A write may read the revision's value default; empty values count.
        sink = RecordingSink()
        compact_changelog(
            registry, 1, CountingRead([("p", 0, 1, rev, b'{"k":"x","d":false}')]),
            {"p": {0, 1}}, {"p": 1}, sink,
            max_checkpoint_bytes=len(b'["p","x",""]'), max_tail_bytes=0)
        self.assertEqual(sink.checkpoints, [("p", "x", "")])

    def test_revision_shape_validation(self):
        # A revision that does not alias key/deleted/value at all.
        registry = Registry()
        plain = registry.register({"k": Field(str, ""), "d": Field(bool, False),
                                   "v": Field(str, "")})
        # A revision whose logical "key" has the wrong physical kind.
        wrong_kind = registry.register(
            {"n": Field(int, 0), "d": Field(bool, False), "v": Field(str, "")},
            aliases={"n": "key", "d": "deleted", "v": "value"})
        # A revision with more than 24 declared physical fields.
        wide_fields = {"k": Field(str, ""), "d": Field(bool, False),
                       "v": Field(str, "")}
        wide_fields.update({f"f{i}": Field(int, 0) for i in range(22)})
        wide = registry.register(
            wide_fields,
            aliases={"k": "key", "d": "deleted", "v": "value",
                     **{f"f{i}": f"f{i}" for i in range(22)}})
        payload = b'{"k":"x","d":false}'
        for revision in (plain, wrong_kind, wide):
            with self.assertRaises(ValueError, msg=f"revision {revision}"):
                compact_changelog(
                    registry, 1, CountingRead([("p", 0, 1, revision, payload)]),
                    {"p": {0, 1}}, {"p": 1}, RecordingSink(),
                    max_checkpoint_bytes=10**9, max_tail_bytes=10**9)

    def test_too_many_referenced_revisions(self):
        registry = Registry()
        revisions = [
            registry.register({"k": Field(str, ""), "d": Field(bool, False),
                               "v": Field(str, "")},
                              aliases={"k": "key", "d": "deleted",
                                       "v": "value"})
            for _ in range(33)]
        records = [("p", i, i + 1, revisions[i], b'{"k":"x","d":false}')
                   for i in range(33)]
        with self.assertRaises(ValueError):
            compact_changelog(
                registry, len(records), CountingRead(records),
                {"p": {0}}, {"p": 0}, RecordingSink(),
                max_checkpoint_bytes=0, max_tail_bytes=10**9)

    def test_transaction_rules(self):
        # Two changes of one transaction in a single partition.
        records = [("a", 0, 5, self.rev_a, PAYLOAD_A0),
                   ("a", 1, 5, self.rev_a, PAYLOAD_A1)]
        with self.assertRaises(ValueError):
            self.compact(count=2, read=CountingRead(records),
                         restart_marks={"a": {0, 2}}, replica_fence={"a": 2})
        # Transaction ids must strictly increase with offset per partition.
        records = [("a", 0, 5, self.rev_a, PAYLOAD_A0),
                   ("a", 1, 4, self.rev_a, PAYLOAD_A1)]
        with self.assertRaises(ValueError):
            self.compact(count=2, read=CountingRead(records),
                         restart_marks={"a": {0, 2}}, replica_fence={"a": 2})
        # Offsets must cover [0, end) exactly (gap at 1).
        records = [("a", 0, 1, self.rev_a, PAYLOAD_A0),
                   ("a", 2, 2, self.rev_a, PAYLOAD_A2)]
        with self.assertRaises(ValueError):
            self.compact(count=2, read=CountingRead(records),
                         restart_marks={"a": {0, 3}}, replica_fence={"a": 3})

    def test_read_failure_propagates_unchanged(self):
        def read(index):
            if index == 2:
                raise Boom("cold read failure")
            return self.records[index]

        sink = RecordingSink()
        with self.assertRaises(Boom) as ctx:
            self.compact(read=read, sink=sink)
        self.assertEqual(str(ctx.exception), "cold read failure")
        self.assertEqual(sink.checkpoints, [])
        self.assertEqual(sink.tails, [])
        self.assertEqual(sink.discarded, 0)

    def test_empty_log_seals_zero_cuts(self):
        sink = RecordingSink()
        cuts = compact_changelog(
            self.registry, 0, CountingRead([]),
            {"a": {0}, "e": {0}}, {"a": 0, "e": 0}, sink,
            max_checkpoint_bytes=0, max_tail_bytes=0)
        self.assertEqual(cuts, {"a": 0, "e": 0})
        self.assertEqual(sink.sealed, {"a": 0, "e": 0})
        self.assertEqual(sink.checkpoints, [])
        self.assertEqual(sink.tails, [])


class DictScratch:
    """Simple in-memory scratch with capacity accounting for tests."""

    def __init__(self, capacity=10**9):
        self.capacity = capacity
        self.items = {}
        self.peak = 0

    def put(self, key, data):
        if key in self.items:
            used = sum(map(len, self.items.values())) - len(self.items[key])
        else:
            used = sum(map(len, self.items.values()))
        if used + len(data) > self.capacity:
            raise Boom("scratch capacity exceeded")
        self.items[key] = data
        self.peak = max(self.peak, sum(map(len, self.items.values())))

    def get(self, key):
        return self.items[key]

    def drop(self, key):
        del self.items[key]


class ScratchAndDeliveryFailureTest(TestCase):
    def setUp(self):
        self.registry, self.rev_a, self.rev_b = acceptance_registry()
        self.records = acceptance_records(self.rev_a, self.rev_b)
        self.marks = {"a": {0, 1, 2, 3}, "b": {0, 1, 2}}
        self.fence = {"a": 3, "b": 1}

    def compact(self, sink, scratch, read=None):
        return compact_changelog(
            self.registry, len(self.records),
            read or CountingRead(self.records), self.marks, self.fence, sink,
            max_checkpoint_bytes=CHECKPOINT_BUDGET, max_tail_bytes=TAIL_BUDGET,
            scratch=scratch)

    def test_scratch_round_trip_and_cleanup_on_success(self):
        scratch = DictScratch()
        sink = RecordingSink()
        cuts = self.compact(sink, scratch)
        self.assertEqual(cuts, {"a": 2, "b": 1})
        self.assertEqual(sink.checkpoints, [("b", "b", "ready")])
        # Tail tuples are rebuilt from metadata plus the staged original bytes.
        self.assertEqual(sink.tails, [
            ("a", 2, 3, self.rev_a, PAYLOAD_A2),
            ("b", 1, 4, self.rev_b, PAYLOAD_B1),
        ])
        self.assertEqual(scratch.items, {})  # staging fully cleared
        self.assertGreater(scratch.peak, 0)

    def test_scratch_failure_propagates_and_staging_is_dropped(self):
        class FailingScratch(DictScratch):
            def put(self, key, data):
                if key == 3:
                    raise Boom("scratch put failure")
                super().put(key, data)

        scratch = FailingScratch()
        sink = RecordingSink()
        with self.assertRaises(Boom) as ctx:
            self.compact(sink, scratch)
        self.assertEqual(str(ctx.exception), "scratch put failure")
        self.assertNotIsInstance(ctx.exception, CompactionError)
        self.assertEqual(scratch.items, {})
        self.assertEqual(sink.discarded, 0)

    def test_no_fit_still_clears_scratch(self):
        scratch = DictScratch()
        sink = RecordingSink()
        with self.assertRaises(CompactionError):
            compact_changelog(
                self.registry, len(self.records), CountingRead(self.records),
                self.marks, {"a": 3, "b": 0}, sink,
                max_checkpoint_bytes=CHECKPOINT_BUDGET,
                max_tail_bytes=TAIL_BUDGET, scratch=scratch)
        self.assertEqual(scratch.items, {})
        self.assertEqual(sink.discarded, 0)

    def test_delivery_failure_discards_and_propagates_the_caller_error(self):
        boom = Boom("sink tail failure")

        class FailingSink(RecordingSink):
            def tail(self, record):
                raise boom

        scratch = DictScratch()
        sink = FailingSink()
        with self.assertRaises(Boom) as ctx:
            self.compact(sink, scratch)
        self.assertIs(ctx.exception, boom)  # the very same object
        self.assertEqual(sink.discarded, 1)
        self.assertIsNone(sink.sealed)
        self.assertEqual(len(sink.checkpoints), 1)  # checkpoint was streamed
        self.assertEqual(scratch.items, {})

    def test_seal_failure_discards_the_unsealed_batch(self):
        class FailingSealSink(RecordingSink):
            def seal(self, cuts):
                raise Boom("seal failed")

        sink = FailingSealSink()
        with self.assertRaises(Boom):
            self.compact(sink, DictScratch())
        self.assertEqual(sink.discarded, 1)
        self.assertIsNone(sink.sealed)


class CompactionMemorySubprocessTest(TestCase):
    """Acceptance 2 and 3: fresh-process quota/RSS/failure checks."""

    @skipUnless(sys.platform.startswith("linux"), "Linux RSS acceptance")
    def run_check(self, *args):
        script = Path(__file__).with_name("compaction_memory_check.py")
        result = subprocess.run(
            [sys.executable, "-I", str(script), *args],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
            timeout=300, check=False)
        self.assertEqual(result.returncode, 0, result.stderr)
        return json.loads(result.stdout.strip().splitlines()[-1])

    def test_reread_scenario_seals_within_120k_reads_and_64_mib(self):
        payload = self.run_check("--scenario", "reread")
        self.assertEqual(payload["cuts"], {"a": 24000, "b": 24000})
        self.assertEqual(payload["sealed"], {"a": 24000, "b": 24000})
        self.assertEqual(payload["reads"], 120_000)
        self.assertEqual(payload["distinct_reads"], 72_000)
        self.assertEqual(payload["checkpoints"], 24_000)
        self.assertEqual(payload["tails"], 24_000)
        self.assertEqual(payload["discarded"], 0)
        self.assertLessEqual(payload["peak_rss_kib"], 65536, payload)

    def test_scratch_scenario_seals_with_single_supply_and_160_mib_staging(self):
        payload = self.run_check("--scenario", "scratch")
        self.assertEqual(payload["cuts"], {"a": 24000, "b": 24000})
        self.assertEqual(payload["reads"], 72_000)
        self.assertEqual(payload["distinct_reads"], 72_000)
        self.assertEqual(payload["checkpoints"], 24_000)
        self.assertEqual(payload["tails"], 24_000)
        self.assertLessEqual(payload["scratch_peak"], 160 * 1024 * 1024)
        self.assertEqual(payload["scratch_used"], 0)
        self.assertEqual(payload["scratch_leftover"], 0)
        self.assertLessEqual(payload["peak_rss_kib"], 65536, payload)

    def test_sink_failure_discards_the_batch_in_both_scenarios(self):
        for scenario in ("reread", "scratch"):
            payload = self.run_check("--scenario", scenario,
                                     "--failure", "sink-tail")
            self.assertEqual(payload["error"], "Boom", payload)
            self.assertIsNone(payload["sealed"])
            self.assertEqual(payload["discarded"], 1)
            self.assertEqual(payload["tails"], 0)
            self.assertEqual(payload["checkpoints"], 24_000)
            self.assertEqual(payload["scratch_leftover"], 0)
            self.assertLessEqual(payload["peak_rss_kib"], 65536, payload)

    def test_last_record_read_failure_delivers_nothing(self):
        for scenario in ("reread", "scratch"):
            payload = self.run_check("--scenario", scenario,
                                     "--failure", "read-last")
            self.assertEqual(payload["error"], "Boom", payload)
            self.assertIsNone(payload["sealed"])
            self.assertEqual(payload["discarded"], 0)
            self.assertEqual(payload["checkpoints"], 0)
            self.assertEqual(payload["tails"], 0)
            self.assertEqual(payload["scratch_leftover"], 0)
            self.assertLessEqual(payload["peak_rss_kib"], 65536, payload)
