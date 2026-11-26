"""A JSON record codec and a cumulative rollout compatibility registry."""
from collections.abc import Mapping
from dataclasses import dataclass
import json
import os
import re
import sqlite3
import tempfile


class Incompatible(ValueError):
    pass


class DeliveryError(ValueError):
    """A valid publish request has no complete delivery plan.

    ``reason`` is ``"uncovered"`` when some subscription has no enabled,
    readable version that can faithfully carry its required logical fields,
    or ``"budget"`` when every subscription has candidates but no complete
    assignment fits both the variant and the byte budget.
    """

    def __init__(self, reason, detail=""):
        if reason not in ("uncovered", "budget"):
            raise ValueError("unknown delivery failure reason")
        self.reason = reason
        super().__init__(f"{reason}: {detail}" if detail else reason)


_REQUIRED = object()
_TYPES = (int, str, bool)


@dataclass(frozen=True)
class Field:
    kind: type
    default: object = _REQUIRED

    def __post_init__(self):
        if self.kind not in _TYPES:
            raise ValueError("expected a scalar field type")
        if self.default is not _REQUIRED and type(self.default) is not self.kind:
            raise ValueError("default has the wrong type")

    @property
    def required(self):
        return self.default is _REQUIRED


def _fields(fields):
    out = dict(fields)
    if not out or any(type(name) is not str or not name or not isinstance(field, Field)
                      for name, field in out.items()):
        raise ValueError("invalid fields")
    return out


def _alias_map(fields, aliases):
    if aliases is None:
        return {name: name for name in fields}
    if not isinstance(aliases, Mapping):
        raise ValueError("aliases must map physical field names to logical names")
    alias_map = dict(aliases)  # frozen copy: later caller mutation cannot leak in
    if set(alias_map) != set(fields):
        raise ValueError("aliases must give one logical name per declared physical field")
    for logical in alias_map.values():
        if type(logical) is not str or not logical:
            raise ValueError("logical names must be non-empty strings")
    if len(set(alias_map.values())) != len(alias_map):
        raise ValueError("two physical fields cannot share a logical name")
    return alias_map


def _duplicates(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON field")
        result[key] = value
    return result


class Codec:
    def __init__(self, fields):
        self._fields = _fields(fields)

    def _normalize(self, values, *, fill_defaults):
        if not isinstance(values, dict):
            raise ValueError("message must be an object")
        output = {}
        for name, field in self._fields.items():
            if name not in values:
                if field.required:
                    raise ValueError(f"missing required field: {name}")
                if fill_defaults:
                    output[name] = field.default
            elif type(values[name]) is not field.kind:
                raise ValueError(f"wrong type: {name}")
            else:
                output[name] = values[name]
        return output

    def encode(self, values):
        if not isinstance(values, dict) or values.keys() - self._fields.keys():
            raise ValueError("producer included undeclared fields")
        return json.dumps(self._normalize(values, fill_defaults=False),
                          sort_keys=True, separators=(",", ":")).encode("ascii")

    def decode(self, payload):
        return self._normalize(json.loads(payload, object_pairs_hook=_duplicates),
                               fill_defaults=True)


def _check(writer, reader, detail):
    for name, field in reader.items():
        sent = writer.get(name)
        if sent is not None and sent.kind is not field.kind:
            raise Incompatible(f"{detail}: type changed for {name}")
        if field.required and (sent is None or not sent.required):
            raise Incompatible(f"{detail}: writer may omit {name}")


class Registry:
    """Preflight both read directions against every registered version."""

    MAX_SUBSCRIPTIONS = 10
    MAX_ENABLED = 12
    MAX_FIELDS = 24
    MAX_REQUIRED_KEYS = 24
    MAX_PAYLOAD_BYTES = 16 * 1024

    def __init__(self):
        self._versions = []
        self._aliases = []

    def register(self, fields, *, aliases=None):
        candidate = _fields(fields)
        alias_map = _alias_map(candidate, aliases)
        for revision, existing in enumerate(self._versions, 1):
            _check(existing, candidate, f"revision {revision} to candidate")
            _check(candidate, existing, f"candidate to revision {revision}")
        self._versions.append(candidate)
        self._aliases.append(alias_map)
        return len(self._versions)

    def _version(self, revision):
        if type(revision) is not int or not 1 <= revision <= len(self._versions):
            raise KeyError(revision)
        return self._versions[revision - 1], self._aliases[revision - 1]

    def codec(self, revision):
        fields, _ = self._version(revision)
        return Codec(fields)

    def publish(self, source_revision, payload, *, enabled, subscriptions,
                max_variants, max_bytes):
        """Plan one relay publication for subscribers at mixed upgrade stages.

        Returns ``(variants, routes)``: the target revision -> JSON bytes to
        emit once, and the subscription id -> its single target revision.
        Raises ``DeliveryError`` when no complete plan exists, ``KeyError``
        for unknown revisions and ``ValueError`` for invalid input.
        """
        for budget in (max_variants, max_bytes):
            if type(budget) is not int or budget < 0:
                raise ValueError("budgets must be non-negative integers")
        source_fields, source_aliases = self._version(source_revision)
        if not isinstance(payload, (bytes, bytearray)):
            raise ValueError("payload must be JSON bytes")
        if len(payload) > self.MAX_PAYLOAD_BYTES:
            raise ValueError("payload exceeds the 16 KiB publish limit")
        decoded = Codec(source_fields).decode(bytes(payload))
        source_logical = {source_aliases[name]: value for name, value in decoded.items()}

        enabled_set = self._revision_set(enabled, "enabled")
        if len(enabled_set) > self.MAX_ENABLED:
            raise ValueError("too many enabled versions")
        subs = self._subscription_list(subscriptions)

        referenced = {source_revision} | enabled_set
        for _, accepted, _ in subs:
            referenced |= accepted
        for revision in referenced:
            if len(self._versions[revision - 1]) > self.MAX_FIELDS:
                raise ValueError(f"revision {revision} exceeds the 24-field publish limit")
        if not subs:
            return {}, {}
        return self._plan(source_logical, enabled_set, subs, max_variants, max_bytes)

    def _revision_set(self, values, what):
        if isinstance(values, (str, bytes, bytearray)) or not hasattr(values, "__iter__"):
            raise ValueError(f"{what} must be a collection of revision ids")
        out = set()
        for item in values:
            self._version(item)  # unknown or non-integer revisions are KeyError
            out.add(item)
        return out

    def _subscription_list(self, subscriptions):
        if not isinstance(subscriptions, Mapping):
            raise ValueError("subscriptions must map ids to (accepted_revisions, required_keys)")
        if len(subscriptions) > self.MAX_SUBSCRIPTIONS:
            raise ValueError("too many subscriptions")
        subs = []
        for sid, spec in subscriptions.items():
            if type(sid) is not str or not sid:
                raise ValueError("subscription ids must be non-empty strings")
            try:
                accepted, required = spec
            except (TypeError, ValueError):
                raise ValueError(
                    "subscription value must be (accepted_revisions, required_keys)") from None
            accepted_set = self._revision_set(accepted, "accepted_revisions")
            if isinstance(required, (str, bytes, bytearray)) or not hasattr(required, "__iter__"):
                raise ValueError("required_keys must be a collection of logical names")
            required_set = set()
            for key in required:
                if type(key) is not str or not key:
                    raise ValueError("required keys must be non-empty strings")
                required_set.add(key)
            if len(required_set) > self.MAX_REQUIRED_KEYS:
                raise ValueError("too many required keys")
            subs.append((sid, accepted_set, required_set))
        return subs

    def _plan(self, source_logical, enabled_set, subs, max_variants, max_bytes):
        views = {}
        for revision in enabled_set:
            fields, aliases = self._versions[revision - 1], self._aliases[revision - 1]
            logical = {aliases[name]: field for name, field in fields.items()}
            views[revision] = (fields, aliases, logical)

        def encodable(logical):
            # Every required target field needs a same-named, same-typed source value.
            return all(not field.required
                       or (lname in source_logical
                           and type(source_logical[lname]) is field.kind)
                       for lname, field in logical.items())

        usable = {rev for rev in enabled_set if encodable(views[rev][2])}

        candidates = []
        for sid, accepted, required in subs:
            cands = set()
            for rev in usable & accepted:
                logical = views[rev][2]
                if all(key in logical and key in source_logical
                       and type(source_logical[key]) is logical[key].kind
                       for key in required):
                    cands.add(rev)
            if not cands:
                raise DeliveryError(
                    "uncovered", f"subscription {sid!r} has no faithful enabled version")
            candidates.append(cands)

        def encode_variant(revision, required_union):
            fields, aliases, _ = views[revision]
            values = {}
            for name, field in fields.items():
                lname = aliases[name]
                if field.required:
                    values[name] = source_logical[lname]
                elif lname in required_union:
                    value = source_logical[lname]
                    if value != field.default:  # omitting would read the same value
                        values[name] = value
            return Codec(fields).encode(values)

        count = len(subs)
        req_sets = [required for _, _, required in subs]
        all_cands = set().union(*candidates)
        cand_mask = {rev: sum(1 << i for i in range(count) if rev in candidates[i])
                     for rev in all_cands}
        versions_for = [tuple(rev for rev in all_cands
                              if not mask or mask & cand_mask[rev] == mask)
                        for mask in range(1 << count)]
        sizes = {}

        def variant_size(rev, mask):
            key = (rev, mask)
            if key not in sizes:
                union = set()
                for i in range(count):
                    if mask >> i & 1:
                        union |= req_sets[i]
                sizes[key] = len(encode_variant(rev, union))
            return sizes[key]

        # dp[mask][variants_used] = minimum total real bytes covering exactly mask.
        full = (1 << count) - 1
        dp = [dict() for _ in range(1 << count)]
        dp[0][0] = 0
        parent = {}
        for mask in range(1, 1 << count):
            sub = mask
            while sub:
                prev = mask ^ sub
                if dp[prev]:
                    for rev in versions_for[sub]:
                        added = variant_size(rev, sub)
                        for used, total in dp[prev].items():
                            new_total = total + added
                            if new_total < dp[mask].get(used + 1, max_bytes + 1):
                                dp[mask][used + 1] = new_total
                                parent[(mask, used + 1)] = (prev, rev, sub)
                sub = (sub - 1) & mask
        best = min(((total, used) for used, total in dp[full].items()
                    if used <= max_variants and total <= max_bytes), default=None)
        if best is None:
            raise DeliveryError("budget", "no complete assignment fits both budgets")

        groups = {}
        mask, used = full, best[1]
        while mask:
            prev, rev, sub = parent[(mask, used)]
            union, members = groups.setdefault(rev, (set(), []))
            for i in range(count):
                if sub >> i & 1:
                    union |= req_sets[i]
                    members.append(i)
            mask, used = prev, used - 1
        variants = {rev: encode_variant(rev, union) for rev, (union, _) in groups.items()}
        routes = {}
        for rev, (_, members) in groups.items():
            for i in members:
                routes[subs[i][0]] = rev
        return variants, routes


# Replay-window admission limits.  These deliberately apply only to
# replay_window; registration, Codec and Registry.publish keep their own rules.
REPLAY_MAX_PARTITIONS = 64
REPLAY_MAX_RECORDS = 200_000
REPLAY_MAX_REVISIONS = 32
REPLAY_MAX_SCHEMA_FIELDS = 24
REPLAY_MAX_PAYLOAD_BYTES = 4 * 1024
REPLAY_MAX_EVENT_ID_BYTES = 128
REPLAY_MAX_STATE_BYTES = 4_096
_REPLAY_OFFSET_MAX = 2**63 - 1
_REPLAY_PARTITION_RE = re.compile(r"[0-9A-Za-z_-]{1,64}\Z")


def _valid_replay_integer(value):
    # bool is a subclass of int, but is not an exact integer coordinate.
    return type(value) is int


def _validate_replay_windows(windows):
    if not isinstance(windows, Mapping) or len(windows) == 0:
        raise ValueError("windows must be a non-empty mapping")
    if len(windows) > REPLAY_MAX_PARTITIONS:
        raise ValueError("too many replay partitions")

    normalized = []
    for partition, bounds in windows.items():
        if type(partition) is not str or _REPLAY_PARTITION_RE.fullmatch(partition) is None:
            raise ValueError("invalid replay partition name")
        try:
            start, stop = bounds
        except (TypeError, ValueError):
            raise ValueError("replay window must be a (start, stop) pair") from None
        if not _valid_replay_integer(start) or not _valid_replay_integer(stop):
            raise ValueError("replay window offsets must be exact integers")
        if not 0 <= start <= stop <= _REPLAY_OFFSET_MAX:
            raise ValueError("invalid replay window range")
        normalized.append((partition, start, stop))
    return normalized


def _replay_logical_values(registry, revision, payload):
    """Decode one record and enforce the no-default event identity rule."""
    fields = registry._versions[revision - 1]
    aliases = registry._aliases[revision - 1]
    if len(fields) > REPLAY_MAX_SCHEMA_FIELDS:
        raise ValueError("replay schema has too many physical fields")

    logical_to_physical = {logical: physical for physical, logical in aliases.items()}
    id_physical = logical_to_physical.get("event_id")
    state_physical = logical_to_physical.get("state")
    if id_physical is None or state_physical is None:
        raise ValueError("revision does not alias both event_id and state")

    codec = registry.codec(revision)
    # This is intentionally the same parse/normalization path as Codec.decode,
    # while retaining the raw object for physical-field presence checking.
    raw = json.loads(payload, object_pairs_hook=_duplicates)
    decoded = codec._normalize(raw, fill_defaults=True)
    if id_physical not in raw:
        raise ValueError("event_id must be present physically")

    event_id = decoded[id_physical]
    state = decoded[state_physical]
    if type(event_id) is not str or type(state) is not str:
        raise ValueError("event_id and state must be strings")

    event_id_bytes = event_id.encode("utf-8")
    state_bytes = state.encode("utf-8")
    if not 1 <= len(event_id_bytes) <= REPLAY_MAX_EVENT_ID_BYTES:
        raise ValueError("event_id has invalid UTF-8 length")
    if len(state_bytes) > REPLAY_MAX_STATE_BYTES:
        raise ValueError("state has invalid UTF-8 length")
    return event_id_bytes, state_bytes


def replay_window(registry, records, windows, deliver, *, scratch_dir):
    """Decode, deduplicate across variants, and safely replay a sealed window.

    The full stream is read and spooled below ``scratch_dir`` before
    ``deliver`` is called.  Returns ``(next_offsets, blocked)``.
    """
    window_list = _validate_replay_windows(windows)
    if not callable(deliver):
        raise ValueError("deliver must be callable")

    # Rank by the actual partition-name ordering used by (partition, offset).
    # The returned dictionaries still use the caller's original window order.
    ranked_names = sorted(partition for partition, _, _ in window_list)
    rank = {partition: index for index, partition in enumerate(ranked_names)}
    bounds_by_rank = {rank[partition]: (start, stop)
                      for partition, start, stop in window_list}

    with tempfile.TemporaryDirectory(prefix=".replay-window-", dir=scratch_dir) as scratch:
        scratch_path = os.path.abspath(scratch)
        conn = sqlite3.connect(os.path.join(scratch_path, "spool.sqlite3"))
        try:
            quoted_scratch = "'" + scratch_path.replace("'", "''") + "'"
            conn.execute(f"PRAGMA temp_store_directory={quoted_scratch}")
            conn.execute("PRAGMA temp_store=FILE")
            conn.execute("PRAGMA journal_mode=OFF")
            conn.execute("PRAGMA synchronous=OFF")
            conn.execute("PRAGMA mmap_size=0")
            conn.execute("PRAGMA cache_size=-2048")
            conn.execute(
                """
                CREATE TABLE records (
                    p INTEGER NOT NULL,
                    off INTEGER NOT NULL,
                    status INTEGER NOT NULL,
                    eid BLOB,
                    state BLOB,
                    PRIMARY KEY (p, off)
                ) WITHOUT ROWID
                """
            )
            conn.execute(
                """
                CREATE TABLE events (
                    eid BLOB PRIMARY KEY,
                    first_p INTEGER NOT NULL,
                    first_off INTEGER NOT NULL,
                    conflict INTEGER NOT NULL DEFAULT 0
                ) WITHOUT ROWID
                """
            )

            referenced_revisions = set()
            record_count = 0

            for item in records:
                record_count += 1
                if record_count > REPLAY_MAX_RECORDS:
                    raise ValueError("too many replay records")

                if not isinstance(item, tuple) or len(item) != 4:
                    raise ValueError("replay record must be a four-item tuple")
                partition, offset, revision, payload = item

                if (type(partition) is not str
                        or _REPLAY_PARTITION_RE.fullmatch(partition) is None):
                    raise ValueError("invalid replay partition name")
                p = rank.get(partition)
                if p is None:
                    raise ValueError("record is outside replay windows")

                if not _valid_replay_integer(offset):
                    raise ValueError("replay offset must be an exact integer")
                start, stop = bounds_by_rank[p]
                if not start <= offset < stop:
                    raise ValueError("record offset is outside its replay window")

                if not _valid_replay_integer(revision) or revision <= 0:
                    raise ValueError("revision must be a positive exact integer")
                if revision not in referenced_revisions:
                    if len(referenced_revisions) >= REPLAY_MAX_REVISIONS:
                        raise ValueError("too many referenced replay revisions")
                    referenced_revisions.add(revision)

                if not isinstance(payload, bytes):
                    raise ValueError("replay payload must be bytes")
                if len(payload) > REPLAY_MAX_PAYLOAD_BYTES:
                    raise ValueError("replay payload exceeds the 4 KiB limit")

                status = 0
                event_id_bytes = None
                state_bytes = None
                known_revision = 1 <= revision <= len(registry._versions)
                if known_revision:
                    if len(registry._versions[revision - 1]) > REPLAY_MAX_SCHEMA_FIELDS:
                        raise ValueError("replay schema has too many physical fields")
                    try:
                        event_id_bytes, state_bytes = _replay_logical_values(
                            registry, revision, payload)
                        status = 1
                    except Exception:
                        # A poison site remains an offset barrier, but is not an
                        # event replica.  Fatal admission errors are raised
                        # outside this decoding block.
                        status = 0
                        event_id_bytes = None
                        state_bytes = None

                try:
                    conn.execute(
                        "INSERT INTO records (p, off, status, eid, state)"
                        " VALUES (?, ?, ?, ?, ?)",
                        (p, offset, status, event_id_bytes, state_bytes),
                    )
                except sqlite3.IntegrityError:
                    raise ValueError("duplicate partition offset in archive") from None

                if status:
                    conn.execute(
                        "INSERT OR IGNORE INTO events"
                        " (eid, first_p, first_off, conflict)"
                        " VALUES (?, ?, ?, 0)",
                        (event_id_bytes, p, offset),
                    )
                    # Keep the first state as the exact comparison reference.
                    # BLOB equality is equivalent to original Python str
                    # equality for validated UTF-8-encodable strings.
                    conn.execute(
                        """
                        UPDATE events
                           SET conflict = 1
                         WHERE eid = ?
                           AND conflict = 0
                           AND ? != (
                               SELECT state
                                 FROM records
                                WHERE p = events.first_p
                                  AND off = events.first_off
                           )
                        """,
                        (event_id_bytes, state_bytes),
                    )

            next_offsets = {partition: stop
                            for partition, _, stop in window_list}
            blocked = {}

            for partition, start, stop in window_list:
                p = rank[partition]
                cursor = conn.execute(
                    """
                    SELECT r.off, r.status, COALESCE(e.conflict, 0)
                      FROM records r
                      LEFT JOIN events e ON e.eid = r.eid
                     WHERE r.p = ?
                     ORDER BY r.off
                    """,
                    (p,),
                )
                expected = start
                reason = None
                try:
                    for row_offset, row_status, conflict in cursor:
                        if row_offset != expected:
                            reason = "gap"
                            break
                        if row_status == 0:
                            reason = "poison"
                            break
                        if conflict:
                            reason = "conflict"
                            break
                        expected += 1
                finally:
                    cursor.close()

                if reason is None and expected < stop:
                    reason = "gap"

                if reason is not None:
                    # For gap, expected is the first missing offset.  For a
                    # message barrier, expected is that poison/conflict offset.
                    next_offsets[partition] = expected
                    blocked[partition] = reason
                else:
                    next_offsets[partition] = expected

            conn.execute(
                """
                CREATE TABLE safe_bounds (
                    p INTEGER PRIMARY KEY,
                    start_off INTEGER NOT NULL,
                    safe_off INTEGER NOT NULL
                ) WITHOUT ROWID
                """
            )
            conn.executemany(
                "INSERT INTO safe_bounds (p, start_off, safe_off)"
                " VALUES (?, ?, ?)",
                [(rank[partition], start, next_offsets[partition])
                 for partition, start, _ in window_list],
            )

            conn.execute(
                """
                CREATE TABLE delivered (
                    eid BLOB PRIMARY KEY
                ) WITHOUT ROWID
                """
            )

            # The primary key gives (p, off) order.  The first safe row for an
            # event is therefore its minimum safe (partition, offset) representative.
            candidates = conn.execute(
                """
                SELECT r.p, r.off, r.eid, r.state
                  FROM records r
                  JOIN safe_bounds b ON b.p = r.p
                  LEFT JOIN events e ON e.eid = r.eid
                 WHERE r.status = 1
                   AND COALESCE(e.conflict, 0) = 0
                   AND r.off >= b.start_off
                   AND r.off < b.safe_off
                 ORDER BY r.p, r.off
                """
            )
            try:
                for p, offset, event_id_bytes, state_bytes in candidates:
                    marker = conn.execute(
                        "INSERT OR IGNORE INTO delivered (eid) VALUES (?)",
                        (event_id_bytes,),
                    )
                    is_representative = marker.rowcount == 1
                    marker.close()
                    if is_representative:
                        deliver(
                            ranked_names[p],
                            offset,
                            event_id_bytes.decode("utf-8"),
                            state_bytes.decode("utf-8"),
                        )
            finally:
                candidates.close()

            return next_offsets, blocked
        finally:
            conn.close()
