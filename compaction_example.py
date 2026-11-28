"""Call example for contracts.compact_changelog.

Reproduces the fence-driven acceptance scenario: partition ``a`` writes and
deletes ``x`` before writing ``y = ""``, partition ``b`` shares transaction 2
and deletes its own key in transaction 4.  Only the replica fence decides how
far each partition may be compacted; transaction 2 must stay whole.
"""
import json

from contracts import CompactionError, Field, Registry, compact_changelog


def build_registry():
    registry = Registry()
    rev_a = registry.register(
        {"ak": Field(str, ""), "ad": Field(bool, False), "av": Field(str, "")},
        aliases={"ak": "key", "ad": "deleted", "av": "value"})
    rev_b = registry.register(
        {"bk": Field(str, ""), "bd": Field(bool, False),
         "bv": Field(str, "ready")},
        aliases={"bk": "key", "bd": "deleted", "bv": "value"})
    # The archived bytes may still *read* "ready" through the default even
    # though the revision is currently forbidden from producing it.
    registry.set_emit_policy(rev_b, [[("ne", "bv", "ready")]])
    return registry, rev_a, rev_b


def build_log(rev_a, rev_b):
    long_value = "l" * 200
    return [
        ("a", 0, 1, rev_a, json.dumps(
            {"ak": "x", "ad": False, "av": long_value},
            sort_keys=True, separators=(",", ":")).encode("ascii")),
        ("a", 1, 2, rev_a, b'{"ak":"x","ad":true}'),
        ("a", 2, 3, rev_a, b'{  "ak": "y", "ad": false, "av": "" }'),
        ("b", 0, 2, rev_b, b'{"bk":"b","bd":false}'),   # value reads "ready"
        ("b", 1, 4, rev_b, b'{"bk":"b","bd":true}'),
    ]


class PrintingSink:
    def __init__(self):
        self.checkpoints = []
        self.tails = []

    def checkpoint(self, partition, key, value):
        self.checkpoints.append((partition, key, value))

    def tail(self, record):
        self.tails.append(record)

    def seal(self, cuts):
        self.sealed = cuts

    def discard(self):
        self.discarded = True


def main():
    registry, rev_a, rev_b = build_registry()
    records = build_log(rev_a, rev_b)
    read = records.__getitem__
    marks = {"a": {0, 1, 2, 3}, "b": {0, 1, 2}}
    checkpoint_budget = len(json.dumps(
        ["b", "b", "ready"], ensure_ascii=True,
        separators=(",", ":")).encode("ascii"))
    tail_budget = len(records[2][4]) + len(records[4][4])

    sink = PrintingSink()
    cuts = compact_changelog(
        registry, len(records), read, marks, {"a": 3, "b": 1}, sink,
        max_checkpoint_bytes=checkpoint_budget, max_tail_bytes=tail_budget)
    print("sealed cuts:", cuts)
    print("checkpoint:", sink.checkpoints)
    print("tail:", [(p, o, txn) for p, o, txn, _rev, _payload in sink.tails])

    # A lagging replica at b:0 makes every fence-respecting plan split
    # transaction 2, so the same budgets no longer fit.
    try:
        compact_changelog(
            registry, len(records), read, marks, {"a": 3, "b": 0},
            PrintingSink(),
            max_checkpoint_bytes=checkpoint_budget,
            max_tail_bytes=tail_budget)
    except CompactionError as error:
        print("fence b:0 ->", error.reason)


if __name__ == "__main__":
    main()
