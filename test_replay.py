import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from unittest import TestCase, mock, skipUnless

from contracts import Field, Registry, replay_window


class Boom(RuntimeError):
    pass


def acceptance_registry(echo_default="ready"):
    registry = Registry()
    first = registry.register(
        {
            "eid": Field(str, "nonempty-id-default"),
            "text": Field(str, ""),
        },
        aliases={"eid": "event_id", "text": "state"},
    )
    variant = registry.register(
        {
            "name": Field(str, "nonempty-id-default"),
            "phase": Field(str, echo_default),
        },
        aliases={"name": "event_id", "phase": "state"},
    )
    return registry, first, variant


def acceptance_archive():
    helper = Registry()
    rev1 = helper.register(
        {
            "eid": Field(str, "nonempty-id-default"),
            "text": Field(str, ""),
        },
        aliases={"eid": "event_id", "text": "state"},
    )
    rev2 = helper.register(
        {
            "name": Field(str, "nonempty-id-default"),
            "phase": Field(str, "ready"),
        },
        aliases={"name": "event_id", "phase": "state"},
    )
    c1, c2 = helper.codec(rev1), helper.codec(rev2)
    records = [
        ("a", 10, rev1, c1.encode({"eid": "x", "text": "same"})),
        ("a", 12, rev1, c1.encode({"eid": "after-gap", "text": "later"})),
        ("a", 13, rev2, c2.encode({"name": "echo"})),
        ("b", 20, rev1, c1.encode({"eid": "only", "text": "solo"})),
        ("b", 21, rev1, c1.encode({"eid": "echo", "text": "ready"})),
        ("b", 22, rev1, c1.encode({"eid": "x", "text": "same"})),
        ("b", 23, rev1, c1.encode({"eid": "tail", "text": "last"})),
    ]
    # Deliberately not coordinate order.
    return list(reversed(records))


class AcceptanceScenarioTest(TestCase):
    def setUp(self):
        self.windows = {"a": (10, 14), "b": (20, 24)}
        self.archive = acceptance_archive()
        self.archive_snapshot = list(self.archive)

    def test_ready_default_delivers_deduped_safe_representatives(self):
        registry, _, _ = acceptance_registry("ready")
        calls = []
        with tempfile.TemporaryDirectory() as scratch:
            next_offsets, blocked = replay_window(
                registry, iter(self.archive), self.windows,
                lambda *call: calls.append(call), scratch_dir=scratch)
            self.assertEqual(os.listdir(scratch), [])

        self.assertEqual(calls, [
            ("a", 10, "x", "same"),
            ("b", 20, "only", "solo"),
            ("b", 21, "echo", "ready"),
            ("b", 23, "tail", "last"),
        ])
        self.assertEqual(next_offsets, {"a": 11, "b": 24})
        self.assertEqual(blocked, {"a": "gap"})
        # Replay does not mutate or normalize the archive tuples/bytes.
        self.assertEqual(self.archive, self.archive_snapshot)

    def test_changed_default_makes_cross_partition_conflict(self):
        registry, _, _ = acceptance_registry("changed")
        calls = []
        with tempfile.TemporaryDirectory() as scratch:
            next_offsets, blocked = replay_window(
                registry, iter(self.archive), self.windows,
                lambda *call: calls.append(call), scratch_dir=scratch)
            self.assertEqual(os.listdir(scratch), [])

        self.assertEqual(calls, [
            ("a", 10, "x", "same"),
            ("b", 20, "only", "solo"),
        ])
        self.assertEqual(next_offsets, {"a": 11, "b": 21})
        self.assertEqual(blocked, {"a": "gap", "b": "conflict"})
        self.assertEqual(self.archive, self.archive_snapshot)

    def test_shuffled_stream_has_same_actual_values_and_order(self):
        registry, _, _ = acceptance_registry("ready")
        shuffled = self.archive[:]
        # The fixture is reversed; use an explicit non-coordinate permutation.
        shuffled = [self.archive[i] for i in (3, 0, 6, 2, 5, 1, 4)]
        calls = []
        with tempfile.TemporaryDirectory() as scratch:
            replay_window(registry, iter(shuffled), self.windows,
                          lambda *call: calls.append(call), scratch_dir=scratch)
        self.assertEqual([(p, o) for p, o, _, _ in calls],
                         [("a", 10), ("b", 20), ("b", 21), ("b", 23)])
        self.assertEqual({eid for _, _, eid, _ in calls},
                         {"x", "only", "echo", "tail"})


class PoisonAndFailureTest(TestCase):
    def setUp(self):
        self.registry = Registry()
        self.rev = self.registry.register(
            {"eid": Field(str, "nonempty-id-default"),
             "st": Field(str, "state-default")},
            aliases={"eid": "event_id", "st": "state"},
        )
        self.codec = self.registry.codec(self.rev)

    def valid(self, partition, offset, event_id, state="ok", revision=None):
        return (partition, offset, revision or self.rev,
                self.codec.encode({"eid": event_id, "st": state}))

    def test_id_default_duplicate_json_and_unknown_revision_are_poison(self):
        windows = {
            "a_id": (0, 2),
            "b_dup": (0, 2),
            "c_unknown": (0, 2),
            "d_healthy": (0, 2),
        }
        records = [
            self.valid("a_id", 0, "a0", "s"),
            ("a_id", 1, self.rev, b"{}"),  # default identity must not count

            self.valid("b_dup", 0, "b0", "s"),
            ("b_dup", 1, self.rev,
             b'{"eid":"b1","eid":"b1-duplicate","st":"s"}'),

            self.valid("c_unknown", 0, "c0", "s"),
            ("c_unknown", 1, 99, b'{"eid":"unknown"}'),

            self.valid("d_healthy", 0, "d0", "s0"),
            self.valid("d_healthy", 1, "d1", "s1"),
        ]
        calls = []
        with tempfile.TemporaryDirectory() as scratch:
            next_offsets, blocked = replay_window(
                self.registry, iter(records), windows,
                lambda *call: calls.append(call), scratch_dir=scratch)
            self.assertEqual(os.listdir(scratch), [])

        self.assertEqual(calls, [
            ("a_id", 0, "a0", "s"),
            ("b_dup", 0, "b0", "s"),
            ("c_unknown", 0, "c0", "s"),
            ("d_healthy", 0, "d0", "s0"),
            ("d_healthy", 1, "d1", "s1"),
        ])
        self.assertEqual(next_offsets, {
            "a_id": 1, "b_dup": 1, "c_unknown": 1, "d_healthy": 2})
        self.assertEqual(blocked, {
            "a_id": "poison", "b_dup": "poison", "c_unknown": "poison"})

    def test_duplicate_coordinate_rejects_before_any_delivery(self):
        records = [
            self.valid("p", 0, "e0"),
            self.valid("p", 1, "e1"),
            self.valid("p", 1, "e1"),  # same bytes still an archive error
        ]
        deliver = mock.Mock()
        with tempfile.TemporaryDirectory() as scratch:
            with self.assertRaises(ValueError):
                replay_window(self.registry, iter(records), {"p": (0, 3)},
                              deliver, scratch_dir=scratch)
            deliver.assert_not_called()
            self.assertEqual(os.listdir(scratch), [])

    def test_iteration_exception_is_original_and_delivers_nothing(self):
        def source():
            yield self.valid("p", 0, "e0")
            yield self.valid("p", 1, "e1")
            raise Boom("source end")

        deliver = mock.Mock()
        with tempfile.TemporaryDirectory() as scratch:
            with self.assertRaises(Boom) as ctx:
                replay_window(self.registry, source(), {"p": (0, 3)},
                              deliver, scratch_dir=scratch)
            self.assertEqual(str(ctx.exception), "source end")
            deliver.assert_not_called()
            self.assertEqual(os.listdir(scratch), [])

    def test_consumer_exception_preserves_error_and_stops_calls(self):
        records = [self.valid("p", i, f"e{i}") for i in range(4)]
        calls = []

        def deliver(partition, offset, event_id, state):
            calls.append((partition, offset))
            if len(calls) == 2:
                raise Boom("consumer second call")

        with tempfile.TemporaryDirectory() as scratch:
            with self.assertRaises(Boom) as ctx:
                replay_window(self.registry, iter(records), {"p": (0, 4)},
                              deliver, scratch_dir=scratch)
            self.assertEqual(str(ctx.exception), "consumer second call")
            self.assertEqual(calls, [("p", 0), ("p", 1)])
            self.assertEqual(os.listdir(scratch), [])

    def test_successful_replay_leaves_no_scratch_items(self):
        records = [self.valid("p", i, f"e{i}") for i in range(3)]
        with tempfile.TemporaryDirectory() as scratch:
            replay_window(self.registry, iter(records), {"p": (0, 3)},
                          lambda *a: None, scratch_dir=scratch)
            self.assertEqual(os.listdir(scratch), [])


class ReplayValidationTest(TestCase):
    def setUp(self):
        self.registry = Registry()
        self.rev = self.registry.register(
            {"eid": Field(str), "st": Field(str, "")},
            aliases={"eid": "event_id", "st": "state"})
        self.codec = self.registry.codec(self.rev)

    def test_window_validation(self):
        valid_record = ("p", 0, 1, b'{"eid":"x"}')
        with tempfile.TemporaryDirectory() as scratch:
            for windows in (
                {},
                {f"p{i}": (0, 1) for i in range(65)},
                {"": (0, 1)},
                {"bad!": (0, 1)},
                {"p": (-1, 1)},
                {"p": (True, 1)},
                {"p": (0, False)},
                {"p": (2, 1)},
                {"p": (0, 2**63)},
                {"p": ("0", 1)},
            ):
                with self.assertRaises(ValueError, msg=windows):
                    replay_window(self.registry, iter([valid_record]), windows,
                                  mock.Mock(), scratch_dir=scratch)
                self.assertEqual(os.listdir(scratch), [])

    def test_outer_record_errors_and_out_of_window_are_fatal(self):
        windows = {"p": (0, 2)}
        good = b'{"eid":"x"}'
        bad_records = [
            [("p", 0, 1, good), ("p", True, 1, good)],
            [("p", False, 1, good)],
            [("p", 0, True, good)],
            [("p", 0, 0, good)],
            [("p", 0, -1, good)],
            [("p", 2, 1, good)],
            [("other", 0, 1, good)],
            [["p", 0, 1, good]],
            [("p", 0, 1, "not-bytes")],
            [("p", 0, 1, bytearray(good))],
            [("p", 0, 1, b"x" * 4097)],
        ]
        with tempfile.TemporaryDirectory() as scratch:
            for records in bad_records:
                deliver = mock.Mock()
                with self.assertRaises(ValueError, msg=records[0]):
                    replay_window(self.registry, iter(records), windows,
                                  deliver, scratch_dir=scratch)
                deliver.assert_not_called()
                self.assertEqual(os.listdir(scratch), [])

    def test_revision_reference_limit(self):
        records = [("p", i, i + 1, b"{}") for i in range(33)]
        with tempfile.TemporaryDirectory() as scratch:
            with self.assertRaises(ValueError):
                replay_window(Registry(), iter(records), {"p": (0, 33)},
                              mock.Mock(), scratch_dir=scratch)
            self.assertEqual(os.listdir(scratch), [])

    def test_empty_window_is_acknowledged_without_delivery(self):
        with tempfile.TemporaryDirectory() as scratch:
            next_offsets, blocked = replay_window(
                self.registry, iter(()), {"p": (7, 7)}, mock.Mock(),
                scratch_dir=scratch)
        self.assertEqual(next_offsets, {"p": 7})
        self.assertEqual(blocked, {})

    def test_utf8_logical_lengths_and_oversized_state_default(self):
        for state_length, poisoned in ((4096, False), (4097, True)):
            registry = Registry()
            rev = registry.register(
                {"eid": Field(str), "st": Field(str, "x" * state_length)},
                aliases={"eid": "event_id", "st": "state"})
            payload = registry.codec(rev).encode({"eid": "id"})
            calls = []
            with tempfile.TemporaryDirectory() as scratch:
                next_offsets, blocked = replay_window(
                    registry, [("p", 0, rev, payload)], {"p": (0, 1)},
                    lambda *call: calls.append(call), scratch_dir=scratch)
                self.assertEqual(os.listdir(scratch), [])
            if poisoned:
                self.assertEqual(next_offsets, {"p": 0})
                self.assertEqual(blocked, {"p": "poison"})
                self.assertEqual(calls, [])
            else:
                self.assertEqual(next_offsets, {"p": 1})
                self.assertEqual(blocked, {})
                self.assertEqual(len(calls[0][3].encode("utf-8")), 4096)

        # 64 two-byte characters are exactly the 128 UTF-8 byte limit.
        good_id = "é" * 64
        good_payload = self.codec.encode({"eid": good_id, "st": "s"})
        calls = []
        with tempfile.TemporaryDirectory() as scratch:
            replay_window(self.registry, [("p", 0, self.rev, good_payload)],
                          {"p": (0, 1)}, lambda *c: calls.append(c),
                          scratch_dir=scratch)
        self.assertEqual(calls[0][2], good_id)

        bad_payload = self.codec.encode({"eid": "x" * 129, "st": "s"})
        with tempfile.TemporaryDirectory() as scratch:
            next_offsets, blocked = replay_window(
                self.registry, [("p", 0, self.rev, bad_payload)],
                {"p": (0, 1)}, mock.Mock(), scratch_dir=scratch)
        self.assertEqual(next_offsets, {"p": 0})
        self.assertEqual(blocked, {"p": "poison"})

    def test_referenced_schema_field_cap_is_a_fatal_replay_error(self):
        fields = {"eid": Field(str)}
        fields.update({f"f{i}": Field(int, 0) for i in range(24)})  # 25 total
        registry = Registry()
        rev = registry.register(fields)
        payload = registry.codec(rev).encode({"eid": "x"})
        deliver = mock.Mock()
        with tempfile.TemporaryDirectory() as scratch:
            with self.assertRaises(ValueError):
                replay_window(registry, [("p", 0, rev, payload)], {"p": (0, 1)},
                              deliver, scratch_dir=scratch)
            deliver.assert_not_called()
            self.assertEqual(os.listdir(scratch), [])


class StreamingMemorySubprocessTest(TestCase):
    @skipUnless(sys.platform.startswith("linux"), "Linux RSS acceptance")
    def test_70k_reverse_stream_uses_under_64_mib(self):
        script = Path(__file__).with_name("replay_memory_check.py")
        result = subprocess.run(
            [sys.executable, "-I", str(script),
             "--records", "70000", "--state-bytes", "1200"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=120,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        payload = json.loads(result.stdout.strip().splitlines()[-1])
        self.assertEqual(payload["delivered"], 70001)
        self.assertEqual(payload["next_offsets"]["p0"], 70000)
        self.assertEqual(payload["next_offsets"]["p1"], 1)
        self.assertEqual(payload["blocked"], {"p1": "gap"})
        self.assertLessEqual(payload["peak_rss_kib"], 65536, payload)
        self.assertTrue(payload["within_64_mib"], payload)
