"""Call example: tighten an old writer before admitting a new reader.

The old revision defaults a safety switch (``armed``) to True and initially
has no production limit.  The candidate revision flips that default to False.
Admitting the candidate immediately would let an old live message without the
switch be read as "disarmed", so the registry first refuses registration with
a real byte-level counterexample.  After the old writer's production is
restricted, the same candidate registers successfully.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from contracts import (
    Field,
    PolicyUnsatisfiable,
    PolicyViolation,
    Registry,
    SemanticIncompatible,
)


def fields(armed_default):
    return {
        "id": Field(int),
        "channel": Field(str, "offline"),
        "armed": Field(bool, armed_default),
    }


def main():
    registry = Registry()
    # Every revision must either stay off the live channel or be armed.
    receiving = [[("ne", "channel", "live"), ("eq", "armed", True)]]
    old = registry.register(fields(True), policy=receiving)

    try:
        registry.register(fields(False), policy=receiving)
    except SemanticIncompatible as error:
        print("candidate refused:",
              f"writer {error.writer_revision} -> reader "
              f"{error.reader_revision}, clause {error.clause_index}")
        print("  counterexample bytes:", error.payload)
        print("  old read of armed:  ",
              registry.codec(old).decode(error.payload)["armed"])

    guarded = registry.codec(old, guarded=True)
    # One OR clause: offline traffic is untouched; live traffic must carry
    # the physical switch (a defaulted read value does not count as presence).
    limit = [[("ne", "channel", "live"), ("present", "armed", None)]]
    registry.set_emit_policy(old, limit)
    new = registry.register(fields(False), policy=receiving)
    print("candidate now registered as revision", new)

    live_without_switch = {"id": 7, "channel": "live"}
    try:
        guarded.encode(live_without_switch)
    except PolicyViolation as error:
        print("guarded encode refused:",
              error.policy_kind, "clause", error.clause_index)

    wire = registry.codec(old).encode(live_without_switch)
    print("old direct read:    ", registry.codec(old).decode(wire))
    print("new direct read:    ", registry.codec(new).decode(wire))
    try:
        registry.codec(new, guarded=True).decode(wire)
    except PolicyViolation as error:
        print("new guarded read refused:",
              error.policy_kind, "clause", error.clause_index)

    safe = registry.codec(old).encode(
        {"id": 7, "channel": "live", "armed": True})
    print("explicit switch, new read:",
          registry.codec(new, guarded=True).decode(safe))

    try:
        registry.set_emit_policy(
            old, [[("present", "id", None)], [("absent", "id", None)]])
    except PolicyUnsatisfiable:
        print("an unsatisfiable production limit is rejected")
    print("offline object still producible:",
          guarded.encode({"id": 8}) == b'{"id":8}')


if __name__ == "__main__":
    main()
