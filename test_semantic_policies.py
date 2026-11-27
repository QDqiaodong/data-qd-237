"""Acceptance tests for semantic policy admission and guarded codecs.

These tests exercise the staged rollout described in the task: tighten the
old writer first, then admit a reader whose default for a safety switch
flips.  All checks run on real bytes produced and consumed by the original
physical codecs; nothing here samples or assumes solver internals.
"""
import json
from unittest import TestCase

from contracts import (
    Field,
    Incompatible,
    PolicyUnsatisfiable,
    PolicyViolation,
    Registry,
    SemanticIncompatible,
)


RECEIVE = [[("ne", "channel", "live"), ("eq", "armed", True)]]
# One OR clause: offline writes are untouched; a live write must carry the
# physical switch.  Written as a single clause so offline omissions pass.
LIVE_LIMIT = [[("ne", "channel", "live"), ("present", "armed", None)]]


def armed_fields(default):
    return {"id": Field(int), "channel": Field(str, "offline"),
            "armed": Field(bool, default)}


class StagedRolloutTest(TestCase):
    def test_tighten_old_writer_before_new_safe_default_reader(self):
        registry = Registry()
        old = registry.register(armed_fields(True), policy=RECEIVE)

        # Before any production limit, the flipped-default candidate must be
        # refused with a real byte-level counterexample in the old->new
        # direction.
        with self.assertRaises(SemanticIncompatible) as ctx:
            registry.register(armed_fields(False), policy=RECEIVE)
        error = ctx.exception
        self.assertIsInstance(error, Incompatible)
        self.assertEqual(error.writer_revision, old)
        self.assertEqual(error.reader_revision, 2)
        self.assertEqual(error.clause_index, 0)
        raw = json.loads(error.payload)
        self.assertEqual(raw["channel"], "live")
        self.assertNotIn("armed", raw)
        # The same bytes read differently under the two revisions' defaults.
        self.assertIs(registry.codec(old).decode(error.payload)["armed"], True)
        with self.assertRaises(KeyError):
            registry.codec(2)  # the rejected number is not claimable

        held_codec = registry.codec(old, guarded=True)
        live_omission = {"id": 1, "channel": "live"}

        # Tighten the old writer, then the exact same candidate registers and
        # takes the number that was refused earlier.
        self.assertIsNone(registry.set_emit_policy(old, LIVE_LIMIT))
        new = registry.register(armed_fields(False), policy=RECEIVE)
        self.assertEqual(new, 2)

        # The held codec observes the new production limit immediately.
        with self.assertRaises(PolicyViolation) as emit_ctx:
            held_codec.encode(live_omission)
        self.assertEqual(emit_ctx.exception.policy_kind, "emit")
        self.assertEqual(emit_ctx.exception.clause_index, 0)

        # The original bytes are still directly readable by the old revision,
        # while the new reader's receiving policy rejects them.
        raw_bytes = registry.codec(old).encode(live_omission)
        self.assertIs(registry.codec(old).decode(raw_bytes)["armed"], True)
        self.assertIs(registry.codec(new).decode(raw_bytes)["armed"], False)
        with self.assertRaises(PolicyViolation) as read_ctx:
            registry.codec(new, guarded=True).decode(raw_bytes)
        self.assertEqual(read_ctx.exception.policy_kind, "read")
        self.assertEqual(read_ctx.exception.clause_index, 0)

        # An explicit switch satisfies both readers on the same bytes.
        explicit = registry.codec(old).encode(
            {"id": 1, "channel": "live", "armed": True})
        registry.codec(old, guarded=True).decode(explicit)
        registry.codec(new, guarded=True).decode(explicit)

        # Removing the production limit would reopen the counterexample, so
        # the replacement must fail and leave every codec restricted.
        with self.assertRaises(SemanticIncompatible) as relax_ctx:
            registry.set_emit_policy(old, None)
        self.assertEqual(relax_ctx.exception.writer_revision, old)
        self.assertEqual(relax_ctx.exception.reader_revision, new)
        self.assertEqual(json.loads(relax_ctx.exception.payload)["channel"],
                         "live")
        for codec in (held_codec, registry.codec(old, guarded=True)):
            with self.assertRaises(PolicyViolation):
                codec.encode(live_omission)

    def test_emit_policy_can_relax_but_never_stop_production(self):
        registry = Registry()
        old = registry.register(armed_fields(True), policy=RECEIVE)
        registry.set_emit_policy(old, LIVE_LIMIT)
        new = registry.register(armed_fields(False), policy=RECEIVE)
        guarded = registry.codec(old, guarded=True)
        offline = {"id": 1}  # channel defaults offline, switch omitted

        # Tighten: the offline omission now also needs the physical switch.
        registry.set_emit_policy(
            old, LIVE_LIMIT + [[("present", "armed", None)]])
        with self.assertRaises(PolicyViolation) as ctx:
            guarded.encode(offline)
        self.assertEqual(ctx.exception.clause_index, 1)

        # Relax back to the live-only limit: the offline omission is writable.
        registry.set_emit_policy(old, LIVE_LIMIT)
        self.assertEqual(guarded.encode(offline), b'{"id":1}')

        # A limit that admits no object at all is refused and changes nothing.
        with self.assertRaises(PolicyUnsatisfiable):
            registry.set_emit_policy(
                old, [[("present", "id", None)], [("absent", "id", None)]])
        self.assertEqual(guarded.encode(offline), b'{"id":1}')
        self.assertEqual(new, 2)


class PresenceAndNonAdjacentReaderTest(TestCase):
    def test_presence_needs_the_exact_physical_key(self):
        # Two revisions that give a differently named physical key the same
        # logical alias.  The receiving policy keys off the physical name, so
        # the other revision's key and the reader's own defaulted read value
        # both fail to satisfy a present() atom.
        reader_registry = Registry()
        reader = reader_registry.register(
            {"id": Field(int), "lever": Field(bool, False)},
            aliases={"id": "id", "lever": "toggle"},
            policy=[[("present", "lever", None)]])

        writer_registry = Registry()
        writer = writer_registry.register(
            {"id": Field(int), "switch": Field(bool, False)},
            aliases={"id": "id", "switch": "toggle"})

        other_name_bytes = writer_registry.codec(writer).encode(
            {"id": 1, "switch": True})
        # The shared logical alias cannot rebind the physical name: the
        # reader's own default (False) is read, but no "lever" key exists.
        self.assertEqual(reader_registry.codec(reader).decode(other_name_bytes),
                         {"id": 1, "lever": False})
        with self.assertRaises(PolicyViolation) as ctx:
            reader_registry.codec(reader, guarded=True).decode(
                other_name_bytes)
        self.assertEqual(ctx.exception.policy_kind, "read")
        self.assertEqual(ctx.exception.clause_index, 0)

        # A defaulted read value likewise does not count as key presence.
        omitted = reader_registry.codec(reader).encode({"id": 1})
        self.assertEqual(reader_registry.codec(reader).decode(omitted)["lever"],
                         False)
        with self.assertRaises(PolicyViolation) as ctx:
            reader_registry.codec(reader, guarded=True).decode(omitted)
        self.assertEqual(ctx.exception.clause_index, 0)

        # Writing the actual physical key satisfies the atom.
        present = reader_registry.codec(reader).encode(
            {"id": 1, "lever": True})
        self.assertEqual(
            reader_registry.codec(reader, guarded=True).decode(present)["lever"],
            True)

    def test_non_adjacent_reader_still_blocks_the_candidate(self):
        registry = Registry()
        registry.register({"id": Field(int), "flag": Field(bool, False)},
                          policy=[[("ne", "flag", True)]])
        registry.register({"id": Field(int)})  # unconstrained middle revision
        with self.assertRaises(SemanticIncompatible) as ctx:
            registry.register({"id": Field(int), "flag": Field(bool, False)})
        self.assertEqual(ctx.exception.writer_revision, 3)
        self.assertEqual(ctx.exception.reader_revision, 1)
        self.assertEqual(json.loads(ctx.exception.payload)["flag"], True)
        with self.assertRaises(KeyError):
            registry.codec(3)

    def test_string_counterexample_is_neither_default_nor_literal(self):
        # The reader only accepts the two literals; the default is one of them
        # so the violating object must carry a present third string value.
        registry = Registry()
        declaration = [[("eq", "mode", "auto"), ("eq", "mode", "manual")]]
        registry.register({"id": Field(int), "mode": Field(str, "auto")},
                          policy=declaration)
        with self.assertRaises(SemanticIncompatible) as ctx:
            registry.register(
                {"id": Field(int), "mode": Field(str, "auto")})
        value = json.loads(ctx.exception.payload)["mode"]
        self.assertNotEqual(value, "auto")
        self.assertNotIn(value, ("auto", "manual"))


class DeclarationValidationTest(TestCase):
    FIELDS = {"id": Field(int), "ok": Field(bool, False),
              "name": Field(str, "")}

    def assert_rejected(self, declaration):
        with self.assertRaises(ValueError):
            Registry().register(dict(self.FIELDS), policy=declaration)

    def test_invalid_atoms_and_structure(self):
        self.assert_rejected("nope")
        self.assert_rejected([[]])
        self.assert_rejected([[("frobnicate", "id", None)]])
        self.assert_rejected([[("present", "ghost", None)]])
        self.assert_rejected([[("eq", "id", True)]])
        self.assert_rejected([[("eq", "ok", 1)]])
        self.assert_rejected([[("present", "id", 1)]])
        self.assert_rejected([[("eq", "name", b"x")]])
        self.assert_rejected([[("eq", "id", 2**63)]])
        self.assert_rejected([[("eq", "id", -(2**63) - 1)]])
        self.assert_rejected([[("eq", "name", "x" * 257)]])
        self.assert_rejected([[("present", "id", None)] for _ in range(65)])
        self.assert_rejected([[("eq", "id", k) for k in range(7)]])
        self.assert_rejected([[("eq", "id", k)] for k in range(7)])
        self.assert_rejected([("present", "id", None)])
        self.assert_rejected([[("eq", "id", 1, 2)]])

    def test_declarations_are_deep_copied(self):
        fields = dict(self.FIELDS)
        declaration = [[("present", "name", None)]]
        registry = Registry()
        registry.register(fields, policy=declaration)
        declaration[0][0] = ("absent", "name", None)
        declaration.append([("present", "ok", None)])
        with self.assertRaises(PolicyViolation):
            registry.codec(1, guarded=True).decode(b'{"id":1}')

        # Use a fresh registry so the object under test passes the receiving
        # policy; only the (frozen) production limit is then exercised.
        emit_registry = Registry()
        emit_registry.register(fields)
        emit = [[("present", "ok", None)]]
        emit_registry.set_emit_policy(1, emit)
        emit.clear()
        with self.assertRaises(PolicyViolation) as ctx:
            emit_registry.codec(1, guarded=True).encode({"id": 1})
        self.assertEqual(ctx.exception.policy_kind, "emit")
        self.assertEqual(ctx.exception.clause_index, 0)

    def test_unguarded_codec_keeps_original_behavior(self):
        registry = Registry()
        registry.register(dict(self.FIELDS),
                          policy=[[("present", "name", None)]])
        codec = registry.codec(1)
        # No semantic check on the original entry point.
        self.assertEqual(codec.decode(b'{"id":1}'),
                         {"id": 1, "ok": False, "name": ""})
        with self.assertRaises(ValueError):
            codec.encode({"id": 1, "unknown": 2})
        with self.assertRaises(ValueError):
            codec.decode(b'{"id":1,"id":2}')


class ResourceBoundaryTest(TestCase):
    def _schema(self, b11_default=False):
        fields = {"id": Field(int)}
        for index in range(12):
            fields[f"a{index}"] = Field(bool, False)
            fields[f"b{index}"] = Field(
                bool, b11_default if index == 11 else False)
        return fields

    def _first_policy(self):
        clauses = []
        for index in range(12):
            a, b = f"a{index}", f"b{index}"
            clauses.append([("present", a, None), ("present", b, None)])
            clauses.append([("ne", a, True), ("eq", b, True)])
            clauses.append([("ne", b, True), ("eq", a, True)])
        return clauses

    def _second_policy(self):
        clauses = self._first_policy()
        for index in range(12):
            clauses.append([("ne", f"a{index}", True),
                            ("present", f"b{index}", None)])
        return clauses

    def test_default_flip_is_blocked_then_admitted_via_emit_limits(self):
        registry = Registry()
        first = registry.register(self._schema(), policy=self._first_policy())
        second = registry.register(self._schema(),
                                   policy=self._second_policy())

        candidate_fields = self._schema(b11_default=True)
        with self.assertRaises(SemanticIncompatible) as ctx:
            registry.register(candidate_fields, policy=self._second_policy())
        error = ctx.exception
        self.assertEqual(error.writer_revision, first)
        self.assertEqual(error.reader_revision, 3)
        self.assertEqual(error.clause_index, 35)

        raw = json.loads(error.payload)
        self.assertIn("a11", raw)
        self.assertNotIn("b11", raw)
        writer_view = registry.codec(first).decode(error.payload)
        from contracts import _read_view
        reader_view = _read_view(candidate_fields, raw)
        self.assertEqual((writer_view["a11"], writer_view["b11"]),
                         (False, False))
        self.assertEqual((reader_view["a11"], reader_view["b11"]),
                         (False, True))

        registry.set_emit_policy(first, [[("present", "b11", None)]])
        registry.set_emit_policy(second, [[("present", "b11", None)]])
        self.assertEqual(
            registry.register(candidate_fields,
                              policy=self._second_policy()), 3)

    def test_constrained_registry_caps(self):
        registry = Registry()
        registry.register({"id": Field(int)},
                          policy=[[("present", "id", None)]])
        for _ in range(31):
            registry.register({"id": Field(int)})
        self.assertEqual(len(registry._versions), 32)
        with self.assertRaises(ValueError):
            registry.register({"id": Field(int)})

        many = {"id": Field(int)}
        many.update({f"f{i}": Field(int, 0) for i in range(32)})
        with self.assertRaises(ValueError):
            Registry().register(many, policy=[[("present", "id", None)]])

    def test_unconstrained_registry_keeps_original_scope(self):
        registry = Registry()
        for _ in range(35):
            registry.register({"id": Field(int)})
        wide = {"id": Field(int)}
        wide.update({f"f{i}": Field(int, 0) for i in range(40)})
        registry.register(wide)
        self.assertEqual(len(registry._versions), 36)
        self.assertFalse(registry._semantic_mode)


class CandidateEmitPolicyTest(TestCase):
    def test_candidate_emit_policy_is_enforced_and_can_be_unsatisfiable(self):
        registry = Registry()
        registry.register({"id": Field(int), "x": Field(int, 0)})
        candidate = registry.register(
            {"id": Field(int), "x": Field(int, 0)},
            emit_policy=[[("ne", "x", 5)]])
        guarded = registry.codec(candidate, guarded=True)
        with self.assertRaises(PolicyViolation) as ctx:
            guarded.encode({"id": 1, "x": 5})
        self.assertEqual(ctx.exception.policy_kind, "emit")
        self.assertEqual(ctx.exception.clause_index, 0)
        self.assertEqual(guarded.encode({"id": 1, "x": 4}),
                         b'{"id":1,"x":4}')

        with self.assertRaises(PolicyUnsatisfiable):
            Registry().register(
                {"id": Field(int)},
                emit_policy=[[("present", "id", None)],
                             [("absent", "id", None)]])

    def test_exception_hierarchy_and_violation_attributes(self):
        self.assertTrue(issubclass(PolicyUnsatisfiable, ValueError))
        self.assertTrue(issubclass(PolicyViolation, ValueError))
        self.assertTrue(issubclass(SemanticIncompatible, Incompatible))
        with self.assertRaises(ValueError):
            PolicyViolation("bogus", 0)
        violation = PolicyViolation("read", 3)
        self.assertEqual(violation.policy_kind, "read")
        self.assertEqual(violation.clause_index, 3)

    def test_loosening_an_emit_policy_is_checked_against_existing_readers(self):
        registry = Registry()
        registry.register({"id": Field(int), "x": Field(int, 0)},
                          policy=[[("ne", "x", 5)]])
        registry.set_emit_policy(
            1, [[("ne", "x", 5)], [("ne", "x", 6)]])
        registry.register({"id": Field(int), "x": Field(int, 0)},
                          policy=[[("ne", "x", 5)], [("ne", "x", 6)]])
        # Releasing x=6 would give the second reader a byte it must refuse.
        with self.assertRaises(SemanticIncompatible) as ctx:
            registry.set_emit_policy(1, [[("ne", "x", 5)]])
        self.assertEqual(ctx.exception.writer_revision, 1)
        self.assertEqual(ctx.exception.reader_revision, 2)
        # The failed replacement leaves the old limit in force.
        with self.assertRaises(PolicyViolation):
            registry.codec(1, guarded=True).encode({"id": 1, "x": 6})
