"""A JSON record codec and a cumulative rollout compatibility registry."""
from collections.abc import Mapping
from dataclasses import dataclass
import json


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
