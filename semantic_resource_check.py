"""Fresh-process resource acceptance for the staged semantic preflight.

Runs the full 25-field / 12-pair registration, the rejected default-flip
candidate with its real byte counterexample, the two emit-policy adjustments
and the successful retry.  Prints peak RSS and wall time; the preflight itself
runs entirely in this process (no helper subprocesses).
"""
import json
import os
import resource
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from contracts import Field, Registry, SemanticIncompatible, _read_view


def schema(b11_default=False):
    fields = {"id": Field(int)}
    for index in range(12):
        fields[f"a{index}"] = Field(bool, False)
        fields[f"b{index}"] = Field(
            bool, b11_default if index == 11 else False)
    return fields


def first_policy():
    clauses = []
    for index in range(12):
        a, b = f"a{index}", f"b{index}"
        clauses.append([("present", a, None), ("present", b, None)])
        clauses.append([("ne", a, True), ("eq", b, True)])
        clauses.append([("ne", b, True), ("eq", a, True)])
    return clauses


def second_policy():
    clauses = first_policy()
    for index in range(12):
        clauses.append([("ne", f"a{index}", True),
                        ("present", f"b{index}", None)])
    return clauses


def main():
    start = time.perf_counter()
    registry = Registry()
    first = registry.register(schema(), policy=first_policy())
    second = registry.register(schema(), policy=second_policy())

    candidate_fields = schema(b11_default=True)
    candidate_policy = second_policy()
    try:
        registry.register(candidate_fields, policy=candidate_policy)
    except SemanticIncompatible as error:
        rejected = error
    else:
        raise AssertionError("default-flip candidate must be rejected")

    raw = json.loads(rejected.payload)
    writer_view = registry.codec(first).decode(rejected.payload)
    reader_view = _read_view(candidate_fields, raw)
    assert rejected.writer_revision == first
    assert rejected.reader_revision == 3
    assert rejected.clause_index == 35
    assert "a11" in raw and "b11" not in raw
    assert (writer_view["a11"], writer_view["b11"]) == (False, False)
    assert (reader_view["a11"], reader_view["b11"]) == (False, True)

    registry.set_emit_policy(first, [[("present", "b11", None)]])
    registry.set_emit_policy(second, [[("present", "b11", None)]])
    third = registry.register(candidate_fields, policy=candidate_policy)
    assert third == 3

    elapsed = time.perf_counter() - start
    peak_rss_kib = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    print(json.dumps({
        "revisions": [first, second, third],
        "rejected_clause": rejected.clause_index,
        "elapsed_seconds": round(elapsed, 4),
        "peak_rss_kib": peak_rss_kib,
        "within_5_seconds": elapsed <= 5,
        "within_64_mib": peak_rss_kib <= 65536,
    }, indent=2))
    if not (elapsed <= 5 and peak_rss_kib <= 65536):
        sys.exit(1)


if __name__ == "__main__":
    main()
