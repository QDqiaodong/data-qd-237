"""Call example for contracts.replay_window.

It reproduces the cross-physical-name replay scenario: the same archive bytes
are interpreted first with a "ready" default and then with a "changed" default
in an otherwise equivalent registry.
"""
import random
import tempfile

from contracts import Field, Registry, replay_window


def build_registry(echo_default="ready"):
    registry = Registry()
    common = registry.register(
        {
            "eid": Field(str, "default-event-id"),
            "text": Field(str, ""),
        },
        aliases={"eid": "event_id", "text": "state"},
    )
    variant = registry.register(
        {
            "name": Field(str, "default-event-id"),
            "phase": Field(str, echo_default),
        },
        aliases={"name": "event_id", "phase": "state"},
    )
    return registry, common, variant


def build_archive():
    registry = Registry()
    rev1 = registry.register(
        {
            "eid": Field(str, "default-event-id"),
            "text": Field(str, ""),
        },
        aliases={"eid": "event_id", "text": "state"},
    )
    rev2 = registry.register(
        {
            "name": Field(str, "default-event-id"),
            "phase": Field(str, "ready"),
        },
        aliases={"name": "event_id", "phase": "state"},
    )
    codec1 = registry.codec(rev1)
    codec2 = registry.codec(rev2)

    records = [
        ("a", 10, rev1, codec1.encode({"eid": "x", "text": "same"})),
        # a:11 is intentionally absent.
        ("a", 12, rev1, codec1.encode({"eid": "after-gap", "text": "ignored-by-ack"})),
        ("a", 13, rev2, codec2.encode({"name": "echo"})),  # state default
        ("b", 20, rev1, codec1.encode({"eid": "only", "text": "solo"})),
        ("b", 21, rev1, codec1.encode({"eid": "echo", "text": "ready"})),
        ("b", 22, rev1, codec1.encode({"eid": "x", "text": "same"})),
        ("b", 23, rev1, codec1.encode({"eid": "tail", "text": "last"})),
    ]
    # Archive order is not coordinate order.
    random.Random(237).shuffle(records)
    return records


def run_case(echo_default):
    registry, _rev1, _rev2 = build_registry(echo_default)
    records = build_archive()
    windows = {"a": (10, 14), "b": (20, 24)}
    calls = []

    def deliver(partition, offset, event_id, state):
        calls.append((partition, offset, event_id, state))

    with tempfile.TemporaryDirectory(prefix="replay-example-") as scratch:
        next_offsets, blocked = replay_window(
            registry, iter(records), windows, deliver, scratch_dir=scratch
        )
    return calls, next_offsets, blocked


def main():
    calls, offsets, blocked = run_case("ready")
    print("default=ready")
    print("deliveries:", calls)
    print("next_offsets:", offsets)
    print("blocked:", blocked)

    calls, offsets, blocked = run_case("changed")
    print("default=changed")
    print("deliveries:", calls)
    print("next_offsets:", offsets)
    print("blocked:", blocked)


if __name__ == "__main__":
    main()
