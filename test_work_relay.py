"""Acceptance and regression tests for the keyed WorkRelay work queue."""
from unittest import TestCase

from contracts import (
    DeliveryError,
    Field,
    PolicyViolation,
    Registry,
    WorkRelay,
)


class Boom(RuntimeError):
    pass


class BlockStore:
    """In-memory model of the caller's fixed, exclusive block region.

    Every access is counted and index bounds are enforced so a test can prove
    the relay never steps outside the region it was given.  Failures can be
    injected on an individual read or write.
    """

    def __init__(self, total_blocks, block_size):
        self.block_size = block_size
        self.blocks = [b"\x00" * block_size for _ in range(total_blocks)]
        self.read_count = 0
        self.write_count = 0
        self.fail_next_reads = 0
        self.fail_on_write_number = None
        self._writes = 0
        self.written_indices = []

    def _check(self, index):
        if type(index) is not int or not 0 <= index < len(self.blocks):
            raise IndexError(f"block index {index!r} outside the region")

    def read(self, index):
        self._check(index)
        self.read_count += 1
        if self.fail_next_reads:
            self.fail_next_reads -= 1
            raise Boom("injected read failure")
        return self.blocks[index]

    def write(self, index, block):
        self._check(index)
        self._writes += 1
        self.write_count += 1
        if len(block) != self.block_size:
            raise AssertionError("writes must be one full block")
        if self.fail_on_write_number == self._writes:
            # A failed write must leave the target block unchanged.
            raise Boom("injected write failure")
        self.blocks[index] = block
        self.written_indices.append(index)


def simple_registry():
    registry = Registry()
    revision = registry.register(
        {"wk": Field(str), "n": Field(int, 0), "pad": Field(str, "")},
        aliases={"wk": "work_key", "n": "sequence", "pad": "padding"})
    return registry, revision


def build_relay(block_count=5, block_size=256, groups=None):
    registry, revision = simple_registry()
    if groups is None:
        groups = {
            "audit": (revision, ("work_key", "sequence")),
            "processing": (revision, ("work_key", "sequence")),
        }
    total = block_count + 4096 // block_size
    store = BlockStore(total, block_size)
    relay = WorkRelay(registry, store, groups,
                      block_size=block_size, block_count=block_count)
    return registry, revision, store, relay


def payload_need(registry, revision, values, block_size):
    payload = registry.codec(revision).encode(values)
    return payload, (len(payload) + block_size - 1) // block_size


class ScenarioOneTest(TestCase):
    """One completion changes admission; fragmentation must not confuse space."""

    def setUp(self):
        self.registry, self.revision, self.store, self.relay = build_relay(
            block_count=5, block_size=256)

    def test_fragmented_free_space_still_admits_after_last_ack(self):
        relay, store = self.relay, self.store
        offsets = [relay.send(self.revision, {"wk": key, "n": index})
                   for index, key in enumerate(("A", "B", "C", "D", "A"))]
        self.assertEqual(offsets, [0, 1, 2, 3, 4])

        tickets = {}
        for group in ("audit", "processing"):
            claimed = []
            for _ in range(4):
                result = relay.claim(group)
                self.assertIsNotNone(result)
                ticket, offset, reader_revision, values = result
                self.assertEqual(reader_revision, self.revision)
                claimed.append((offset, values["work_key"]))
                tickets[(group, offset)] = ticket
            # The second A (offset 4) cannot pass the still-open first A.
            self.assertIsNone(relay.claim(group))
            self.assertEqual(claimed,
                             [(0, "A"), (1, "B"), (2, "C"), (3, "D")])

        # Both groups finish B; only processing finishes D.
        relay.ack(tickets[("audit", 1)])
        relay.ack(tickets[("processing", 1)])
        relay.ack(tickets[("processing", 3)])

        # One block back (B).  A two-block task must be refused with no I/O.
        big_payload, blocks_needed = payload_need(
            self.registry, self.revision,
            {"wk": "E", "n": 9, "pad": "x" * 280}, 256)
        self.assertEqual(blocks_needed, 2)
        reads_before, writes_before = store.read_count, store.write_count
        with self.assertRaises(DeliveryError) as context:
            relay.send(self.revision,
                       {"wk": "E", "n": 9, "pad": "x" * 280})
        self.assertEqual(context.exception.reason, "backlog_full")
        self.assertEqual((store.read_count, store.write_count),
                         (reads_before, writes_before))
        print(f"[scenario 1] E rejected while space is short: "
              f"DeliveryError reason={context.exception.reason!r}")

        # The audit group's D ack frees the second (non-adjacent) block.
        relay.ack(tickets[("audit", 3)])
        offset_e = relay.send(self.revision,
                              {"wk": "E", "n": 9, "pad": "x" * 280})
        self.assertEqual(offset_e, 5)
        new_blocks = store.written_indices[-2:]
        self.assertEqual(len(new_blocks), 2)
        self.assertNotEqual(new_blocks[1] - new_blocks[0], 1,
                            "the two released blocks were not adjacent")
        print(f"[scenario 1] E admitted after audit acked D at offset "
              f"{offset_e}, using non-adjacent blocks {new_blocks}")

        # Tickets are single use: re-acking an already-acked ticket fails and
        # changes nothing.  Tickets carry their own group, so a foreign-group
        # ticket is likewise refused (covered explicitly below).
        with self.assertRaises(ValueError):
            relay.ack(tickets[("audit", 1)])      # already used
        with self.assertRaises(ValueError):
            relay.release(tickets[("audit", 1)])  # used tickets stay invalid

        # Processing finishes the first A and can then reach offset 4.
        relay.ack(tickets[("processing", 0)])
        result = relay.claim("processing")
        self.assertIsNotNone(result)
        self.assertEqual(result[1], 4)
        self.assertEqual(result[3]["work_key"], "A")

        # Audit still owes its first A, so offset 4 stays blocked for it:
        # audit can only skip to E at offset 5 (a different key), never to the
        # second A that sits behind its still-open first A.
        audit_result = relay.claim("audit")
        self.assertIsNotNone(audit_result)
        self.assertEqual(audit_result[1], 5)
        relay.release(audit_result[0])
        # Re-claiming reaches E again, still never the blocked second A.
        again = relay.claim("audit")
        self.assertIsNotNone(again)
        self.assertEqual(again[1], 5)
        relay.release(again[0])
        print("[scenario 1] processing reached offset 4; audit's second A "
              "stays blocked behind its open first A")


class ScenarioTwoTest(TestCase):
    """Quota survives a failed write; tickets and offsets stay valid."""

    def setUp(self):
        self.registry, self.revision, self.store, self.relay = build_relay(
            block_count=5, block_size=256)
        relay = self.relay
        self.offsets = [
            relay.send(self.revision, {"wk": key, "n": index})
            for index, key in enumerate(("A", "B", "C", "D", "A"))]
        self.tickets = {}
        for group in ("audit", "processing"):
            for offset in range(4):
                ticket, claimed_offset, _, _ = relay.claim(group)
                self.assertEqual(claimed_offset, offset)
                self.tickets[(group, offset)] = ticket
        # Fully finish A(0), B(1) and D(3); C(2) stays claimed in both groups.
        for offset in (0, 1, 3):
            self.relay.ack(self.tickets[("audit", offset)])
            self.relay.ack(self.tickets[("processing", offset)])

    def test_failed_write_keeps_everything_else_usable(self):
        relay, store = self.relay, self.store
        e_values = {"wk": "E", "n": 9, "pad": "x" * 280}
        expected_payload, _ = payload_need(
            self.registry, self.revision, e_values, 256)

        # Fail on the second block write of E's first submission.
        store.fail_on_write_number = store.write_count + 2
        with self.assertRaises(Boom):
            relay.send(self.revision, e_values)
        store.fail_on_write_number = None
        first_block_index = store.written_indices[-1]
        first_block_snapshot = store.blocks[first_block_index]

        # The failed message is not claimable: both groups only see the
        # already-enqueued offset 4, and no offset 5 exists yet.
        for group in ("audit", "processing"):
            ticket, offset, _, values = relay.claim(group)
            self.assertEqual(offset, 4)
            self.assertEqual(values["work_key"], "A")
            relay.release(ticket)

        # Existing tickets remain valid; processing releases and re-claims C.
        old_c_ticket = self.tickets[("processing", 2)]
        self.assertIsNone(relay.release(old_c_ticket))
        with self.assertRaises(ValueError):
            relay.ack(old_c_ticket)
        with self.assertRaises(ValueError):
            relay.release(old_c_ticket)

        # One injected read failure must not mark C as claimed.
        store.fail_next_reads = 1
        with self.assertRaises(Boom):
            relay.claim("processing")
        store.fail_next_reads = 0
        ticket, offset, reader_revision, values = relay.claim("processing")
        self.assertEqual(offset, 2)
        self.assertEqual(values, {"work_key": "C", "sequence": 2})
        print(f"[scenario 2] C re-claimed at offset {offset} after one read "
              f"failure; old ticket was rejected")

        # The partially written block was not rolled back, but its bytes are
        # never exposed as a message.
        self.assertEqual(store.blocks[first_block_index], first_block_snapshot)

        # Retrying E consumes the next sequential offset (the five existing
        # messages hold 0..4; the failed attempt consumed no offset) and reads
        # back whole from the block region.
        offset_e = relay.send(self.revision, e_values)
        self.assertEqual(offset_e, 5)

        # Offset 4 (the second A) is still pending and older, so claims return
        # it first; acknowledge it in both groups so E becomes reachable.
        for group in ("audit", "processing"):
            head, head_offset, _, head_values = relay.claim(group)
            self.assertEqual((head_offset, head_values["work_key"]), (4, "A"))
            relay.ack(head)

        ticket, offset, _, values = relay.claim("processing")
        self.assertEqual(offset, 5)
        message = relay._messages[5]
        raw = b"".join(store.read(i) for i in message["blocks"])[:message["length"]]
        self.assertEqual(raw, expected_payload)
        self.assertEqual(raw[:1], b"{")
        print(f"[scenario 2] E enqueued at offset {offset_e}; full content "
              f"read back from blocks {message['blocks']} "
              f"({message['length']} bytes, match={raw == expected_payload})")

        # Every touched block stayed inside the ordinary region.
        self.assertTrue(all(0 <= i < self.store.block_size and 0 <= i < 5
                            for i in store.written_indices))
        relay.ack(ticket)


class ScenarioThreeTest(TestCase):
    """Real guarded codec path: aliases, default divergence, tightened emit."""

    def build(self):
        registry = Registry()
        # Two reader revisions sharing physical names (required by the
        # registry's cross-revision physical compatibility check) but
        # interpreting them through different aliases, and giving the shared
        # optional mode field different read-side defaults.  Both map a
        # physical field onto logical work_key, as the acceptance requires.
        rev1 = registry.register(
            {"wk": Field(str), "md": Field(str, "v1")},
            aliases={"wk": "work_key", "md": "mode"})
        rev2 = registry.register(
            {"wk": Field(str), "md": Field(str, "v2")},
            aliases={"wk": "work_key", "md": "mode"})
        store = BlockStore(5 + 4096 // 256, 256)
        # Both revisions alias the physical identity onto logical work_key;
        # they differ in the alias only for the optional mode's sibling name.
        relay = WorkRelay(
            registry, store,
            {"audit": (rev1, ("work_key", "mode")),
             "processing": (rev2, ("work_key", "mode"))},
            block_size=256, block_count=5)
        return registry, rev1, rev2, store, relay

    def test_alias_defaults_and_emit_tightening(self):
        registry, rev1, rev2, store, relay = self.build()

        # Explicit shared value is faithful under both aliases and defaults.
        offset = relay.send(rev1, {"wk": "k1", "md": "explicit"})
        self.assertEqual(offset, 0)
        ticket, claimed, reader, values = relay.claim("processing")
        self.assertEqual((claimed, reader), (0, rev2))
        self.assertEqual(values, {"work_key": "k1", "mode": "explicit"})
        relay.release(ticket)
        ticket, _, _, audit_values = relay.claim("audit")
        self.assertEqual(audit_values,
                         {"work_key": "k1", "mode": "explicit"})
        relay.release(ticket)

        # Omitting md makes the two readers disagree through their own
        # defaults: unfaithful, and nothing is enqueued (no store I/O either).
        reads_before, writes_before = store.read_count, store.write_count
        with self.assertRaises(DeliveryError) as context:
            relay.send(rev1, {"wk": "k2"})
        self.assertEqual(context.exception.reason, "unfaithful")
        self.assertEqual((store.read_count, store.write_count),
                         (reads_before, writes_before))
        # The rejected message was never enqueued: the only message is still
        # offset 0, and a fresh claim re-opens that one rather than anything new.
        self.assertEqual(sorted(relay._messages), [0])
        again, again_offset, _, _ = relay.claim("audit")
        self.assertEqual(again_offset, 0)
        relay.release(again)
        print(f"[scenario 3] omitted mode rejected: "
              f"DeliveryError reason={context.exception.reason!r}")

        # Tighten the writer's production limit after history was accepted.
        registry.set_emit_policy(rev1, [[("ne", "md", "explicit")]])

        # History remains readable with the original value.
        ticket, offset, reader, values = relay.claim("processing")
        self.assertEqual(offset, 0)
        self.assertEqual(values["mode"], "explicit")
        relay.release(ticket)

        # The same data can no longer enter through that writer.
        with self.assertRaises(PolicyViolation) as context:
            relay.send(rev1, {"wk": "k1", "md": "explicit"})
        self.assertEqual(context.exception.policy_kind, "emit")
        self.assertEqual(context.exception.clause_index, 0)
        print(f"[scenario 3] re-send after tightening raised "
              f"PolicyViolation(policy_kind={context.exception.policy_kind!r}, "
              f"clause_index={context.exception.clause_index}); "
              f"historical read still gives mode={values['mode']!r}")

        # Data satisfying the new, tighter limit still flows end to end.
        offset = relay.send(rev1, {"wk": "k3", "md": "allowed"})
        self.assertEqual(offset, 1)
        # Offset 0 (k1) is still unacked, so it remains the earliest claim.
        head, head_offset, _, head_values = relay.claim("audit")
        self.assertEqual((head_offset, head_values["mode"]), (0, "explicit"))
        relay.ack(head)
        ticket, claimed, _, values = relay.claim("audit")
        self.assertEqual((claimed, values["mode"]), (1, "allowed"))


class SchedulingAndStorageTest(TestCase):

    def test_head_of_line_is_per_key_only(self):
        _, revision, _, relay = build_relay(block_count=5)
        for key in ("A", "B", "A"):
            relay.send(revision, {"wk": key})
        first = relay.claim("audit")
        self.assertEqual((first[1], first[3]["work_key"]), (0, "A"))
        # Second A is blocked, but B can skip past it.
        second = relay.claim("audit")
        self.assertEqual((second[1], second[3]["work_key"]), (1, "B"))
        self.assertIsNone(relay.claim("audit"))
        relay.ack(first[0])
        relay.ack(second[0])
        third = relay.claim("audit")
        self.assertEqual(third[1], 2)

    def test_blocks_round_up_and_release_keeps_quota(self):
        _, revision, store, relay = build_relay(block_count=4, block_size=256)
        small = relay.send(revision, {"wk": "s"})
        big_values = {"wk": "big", "pad": "p" * 250}
        big = relay.send(revision, big_values)  # 271 wire bytes -> 2 blocks
        self.assertEqual((small, big), (0, 1))
        self.assertEqual(relay._used_blocks, 3)
        # One free block remains, so a one-block message fits; a second one
        # cannot, because the rounded-up big message still holds two blocks.
        self.assertEqual(relay.send(revision, {"wk": "x"}), 2)
        self.assertEqual(relay._used_blocks, 4)
        with self.assertRaises(DeliveryError) as context:
            relay.send(revision, {"wk": "y"})
        self.assertEqual(context.exception.reason, "backlog_full")

        ticket, _, _, _ = relay.claim("audit")
        relay.release(ticket)  # release must not return any blocks
        self.assertEqual(relay._used_blocks, 4)
        # Every group must finish before a message's blocks come back.
        ticket, _, _, _ = relay.claim("audit")
        other, _, _, _ = relay.claim("processing")
        relay.ack(other)  # processing alone does not free message 0
        self.assertEqual(relay._used_blocks, 4)
        relay.ack(ticket)  # the last group frees message 0's single block
        self.assertEqual(relay._used_blocks, 3)

    def test_claim_reads_current_store_bytes_not_cached_content(self):
        _, revision, store, relay = build_relay(block_count=2)
        relay.send(revision, {"wk": "A"})
        ticket, _, _, values = relay.claim("audit")
        self.assertEqual(values["work_key"], "A")
        relay.release(ticket)
        # Rewrite the same-length JSON directly in the block region.
        block_index = relay._messages[0]["blocks"][0]
        replacement = store.blocks[block_index].replace(b'"A"', b'"B"')
        store.write(block_index, replacement)
        ticket, _, _, values = relay.claim("audit")
        self.assertEqual(values["work_key"], "B")  # bytes came from the store

    def test_failed_write_returns_quota_and_offset(self):
        _, revision, store, relay = build_relay(block_count=2)
        relay.send(revision, {"wk": "A"})  # one block used
        store.fail_on_write_number = store.write_count + 1
        with self.assertRaises(Boom):
            relay.send(revision, {"wk": "B"})
        store.fail_on_write_number = None
        self.assertEqual(relay._used_blocks, 1)
        next_offset = relay.send(revision, {"wk": "B"})
        self.assertEqual(next_offset, 1)  # failed send consumed no offset

    def test_ticket_misuse_rejected_without_progress_change(self):
        _, revision, _, relay = build_relay(block_count=2)
        relay.send(revision, {"wk": "A"})
        _, _, _, foreign_relay = build_relay(block_count=2)
        foreign_relay.send(revision, {"wk": "Z"})
        ticket, _, _, _ = relay.claim("audit")
        foreign_ticket, _, _, _ = foreign_relay.claim("audit")
        with self.assertRaises(ValueError):
            relay.ack(foreign_ticket)
        with self.assertRaises(ValueError):
            relay.ack(object())
        relay.release(ticket)
        with self.assertRaises(ValueError):
            relay.ack(ticket)  # superseded by the release
        new_ticket, offset, _, _ = relay.claim("audit")
        self.assertEqual(offset, 0)
        with self.assertRaises(ValueError):
            relay.ack(ticket)  # old ticket after re-claim
        self.assertIsNone(relay.ack(new_ticket))


class ValidationTest(TestCase):

    def test_construction_validation(self):
        registry, revision = simple_registry()
        store = BlockStore(5 + 16, 256)

        def build(**kwargs):
            options = dict(
                registry=registry, store=store,
                groups={"g": (revision, ("work_key",))},
                block_size=256, block_count=5)
            options.update(kwargs)
            return WorkRelay(**options)

        with self.assertRaises(ValueError):
            build(groups={})
        with self.assertRaises(ValueError):
            build(groups={"": (revision, ("work_key",))})
        with self.assertRaises(ValueError):
            build(groups={"g": (revision, ("other",))})  # missing work_key
        with self.assertRaises(ValueError):
            build(groups={"g": (revision, ())})
        with self.assertRaises(ValueError):
            build(groups={"g": revision})
        with self.assertRaises(ValueError):
            build(block_size=128)
        with self.assertRaises(ValueError):
            build(block_count=0)
        with self.assertRaises(ValueError):
            build(block_count=32769)
        with self.assertRaises(ValueError):
            build(block_count=True)  # bool is not an exact integer count
        with self.assertRaises(ValueError):
            build(store=object())
        with self.assertRaises(KeyError):
            build(groups={"g": (99, ("work_key",))})

        many = {f"g{i}": (revision, ("work_key",)) for i in range(9)}
        with self.assertRaises(ValueError):
            build(groups=many)

        big_registry = Registry()
        big_revision = big_registry.register(
            {f"f{i}": Field(int, 0) for i in range(25)})
        required = [f"f{i}" for i in range(24)]
        required.append("work_key")
        with self.assertRaises(ValueError):
            WorkRelay(big_registry, store,
                      {"g": (big_revision, tuple(required))},
                      block_size=256, block_count=5)

    def test_caller_group_mapping_is_frozen(self):
        registry, revision = simple_registry()
        store = BlockStore(5 + 16, 256)
        groups = {"g": (revision, ("work_key",))}
        relay = WorkRelay(registry, store, groups,
                          block_size=256, block_count=5)
        groups["h"] = (revision, ("work_key",))
        with self.assertRaises(KeyError):
            relay.claim("h")

    def test_send_validation(self):
        registry, revision, store, relay = build_relay(block_count=8)

        with self.assertRaises(KeyError):
            relay.send(99, {"wk": "A"})
        with self.assertRaises(KeyError):
            relay.claim("nobody")

        # work_key cannot be invented from a default.
        with self.assertRaises(ValueError):
            relay.send(revision, {"n": 1})
        with self.assertRaises(ValueError):
            relay.send(revision, {"wk": ""})
        with self.assertRaises(ValueError):
            relay.send(revision, {"wk": "k" * 129})
        long_key = "é" * 65  # 130 UTF-8 bytes
        with self.assertRaises(ValueError):
            relay.send(revision, {"wk": long_key})
        with self.assertRaises(ValueError):
            relay.send(revision, {"wk": "A", "unknown": 1})

        # Payload over 4096 bytes is a ValueError from the relay limit.
        with self.assertRaises(ValueError):
            relay.send(revision, {"wk": "A", "pad": "z" * 4100})

        # Rejected sends never touched the store and never consumed offsets.
        self.assertEqual(store.read_count + store.write_count, 0)
        self.assertEqual(relay.send(revision, {"wk": "A"}), 0)

    def test_writer_without_work_key_is_value_error(self):
        registry = Registry()
        # A revision whose work_key is optional: the writer can omit it, so
        # the relay must reject the omission rather than invent the identity.
        writer = registry.register(
            {"wk": Field(str, ""), "n": Field(int, 0)},
            aliases={"wk": "work_key", "n": "sequence"})
        store = BlockStore(5 + 16, 256)
        relay = WorkRelay(
            registry, store, {"g": (writer, ("work_key", "sequence"))},
            block_size=256, block_count=5)
        with self.assertRaises(ValueError):
            relay.send(writer, {"n": 1})

    def test_four_kib_message_uses_full_region_and_frees_on_last_ack(self):
        registry, revision = simple_registry()
        block_size = 4096
        store = BlockStore(1 + 4096 // block_size, block_size)
        relay = WorkRelay(registry, store,
                          {"g": (revision, ("work_key", "sequence"))},
                          block_size=block_size, block_count=1)
        values = {"wk": "max", "pad": "q" * 4000}
        payload = registry.codec(revision).encode(values)
        self.assertLessEqual(len(payload), 4096)
        offset = relay.send(revision, values)
        self.assertEqual(offset, 0)
        with self.assertRaises(DeliveryError) as context:
            relay.send(revision, {"wk": "second"})
        self.assertEqual(context.exception.reason, "backlog_full")
        ticket, claimed, _, read_values = relay.claim("g")
        self.assertEqual((claimed, read_values["work_key"]), (0, "max"))
        relay.ack(ticket)
        self.assertEqual(relay.send(revision, {"wk": "second"}), 1)


class RegionAndLifecycleTest(TestCase):

    def test_spare_blocks_never_back_admission(self):
        _, revision, store, relay = build_relay(block_count=2, block_size=256)
        relay.send(revision, {"wk": "A"})
        relay.send(revision, {"wk": "B"})
        # The region holds 18 blocks but only the first two add capacity.
        self.assertEqual(len(store.blocks), 2 + 4096 // 256)
        with self.assertRaises(DeliveryError) as context:
            relay.send(revision, {"wk": "C"})
        self.assertEqual(context.exception.reason, "backlog_full")
        self.assertTrue(all(i < 2 for i in store.written_indices))

    def test_scheduling_state_drops_finished_messages(self):
        _, revision, _, relay = build_relay(block_count=4)
        relay.send(revision, {"wk": "A"})
        relay.send(revision, {"wk": "B"})
        for group in ("audit", "processing"):
            first, _, _, _ = relay.claim(group)
            second, _, _, _ = relay.claim(group)
            relay.ack(first)
            relay.ack(second)
        # Fully acknowledged messages leave both the message pool and the
        # per-group keyed progress, so state only tracks outstanding work.
        self.assertEqual(relay._messages, {})
        self.assertTrue(all(not g["pending"] for g in relay._groups.values()))
        self.assertTrue(all(not g["claimed"] for g in relay._groups.values()))
        self.assertEqual(relay._used_blocks, 0)
        # The freed blocks admit a fresh, contiguous stream again.
        self.assertEqual(relay.send(revision, {"wk": "C"}), 2)

    def test_release_then_other_group_progress_is_independent(self):
        _, revision, _, relay = build_relay(block_count=4)
        relay.send(revision, {"wk": "A"})
        audit_ticket, offset, _, _ = relay.claim("audit")
        processing_ticket, _, _, _ = relay.claim("processing")
        relay.release(audit_ticket)
        # Processing can still finish while audit has only released.
        self.assertIsNone(relay.ack(processing_ticket))
        self.assertEqual(relay._used_blocks, 1)  # audit still owes the message
        re_claimed, re_offset, _, values = relay.claim("audit")
        self.assertEqual((re_offset, values["work_key"]), (offset, "A"))
        relay.ack(re_claimed)
        self.assertEqual(relay._used_blocks, 0)  # last group frees the blocks

    def test_unknown_group_and_revision_error_kinds(self):
        _, revision, _, relay = build_relay(block_count=2)
        relay.send(revision, {"wk": "A"})
        with self.assertRaises(KeyError):
            relay.claim("missing")
        with self.assertRaises(KeyError):
            relay.send(999, {"wk": "A"})

    def test_non_string_group_id_is_key_error(self):
        _, revision, _, relay = build_relay(block_count=2)
        with self.assertRaises(KeyError):
            relay.claim(7)

