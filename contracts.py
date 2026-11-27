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


class PolicyUnsatisfiable(ValueError):
    """No strictly encodable message can satisfy every mandatory clause."""


class PolicyViolation(ValueError):
    """A guarded codec rejected real bytes through a semantic policy clause."""

    def __init__(self, policy_kind, clause_index):
        if policy_kind not in ("read", "emit"):
            raise ValueError("policy_kind must be 'read' or 'emit'")
        self.policy_kind = policy_kind
        self.clause_index = clause_index
        super().__init__(
            f"{policy_kind} policy rejected clause {clause_index}")


class SemanticIncompatible(Incompatible):
    """Real bytes the writer may produce are refused by a reader's policy.

    ``payload`` is producible by the writer's original codec, satisfies the
    writer's policies under the proposed rules, and violates the reader's
    receiving clause at ``clause_index`` when the reader interprets those
    exact bytes with its own read rules and defaults.
    """

    def __init__(self, writer_revision, reader_revision, payload, clause_index):
        self.writer_revision = writer_revision
        self.reader_revision = reader_revision
        self.payload = payload
        self.clause_index = clause_index
        super().__init__(
            f"revision {writer_revision} output is rejected by revision "
            f"{reader_revision} receiving clause {clause_index}: {payload!r}")


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


# ---------------------------------------------------------------------------
# Semantic policies
#
# A declaration is a conjunction (outer list/tuple) of clauses, where each
# clause is a disjunction (non-empty list/tuple) of atoms.  An atom is a
# triple ``(op, physical_name, operand)``.  Atoms only ever address physical
# names declared by the revision that owns the declaration; logical aliases
# never rebind them.
# ---------------------------------------------------------------------------

_POLICY_OPS = frozenset(("present", "absent", "eq", "ne"))
POLICY_MAX_CLAUSES = 64
POLICY_MAX_ATOMS_PER_CLAUSE = 6
POLICY_MAX_LITERALS_PER_FIELD = 6
POLICY_MAX_STRING_LITERAL = 256
POLICY_INTEGER_MIN = -(2**63)
POLICY_INTEGER_MAX = 2**63 - 1
# Resource bounds that only apply while any non-empty declaration is in use.
POLICY_MAX_REVISIONS = 32
POLICY_MAX_FIELDS = 32


def _parse_policy(declaration, fields):
    """Validate and deep-copy one policy declaration.

    The stored form is a tuple of tuples of ``(op, name, operand)`` triples,
    so later mutation of the caller's lists or literals cannot leak in.
    ``None`` or an empty outer declaration means no extra restriction.
    """
    if declaration is None:
        return ()
    if not isinstance(declaration, (list, tuple)):
        raise ValueError("policy must be None or a list/tuple of clauses")
    if len(declaration) > POLICY_MAX_CLAUSES:
        raise ValueError("policy declares too many clauses")
    parsed_clauses = []
    literals_by_field = {}
    for clause in declaration:
        if not isinstance(clause, (list, tuple)) or not clause:
            raise ValueError("each policy clause must be a non-empty list/tuple")
        if len(clause) > POLICY_MAX_ATOMS_PER_CLAUSE:
            raise ValueError("policy clause contains too many atoms")
        parsed_atoms = []
        for atom in clause:
            if not isinstance(atom, (list, tuple)) or len(atom) != 3:
                raise ValueError("each policy atom must be a (op, name, operand) triple")
            op, name, operand = atom
            if op not in _POLICY_OPS:
                raise ValueError(f"unknown policy operator: {op!r}")
            if type(name) is not str or name not in fields:
                raise ValueError("policy atoms may only reference declared physical names")
            field = fields[name]
            if op in ("present", "absent"):
                if operand is not None:
                    raise ValueError("present/absent atoms require an operand of None")
            else:
                # Exact type match: bool must never pass as an int.
                if type(operand) is not field.kind:
                    raise ValueError("policy literal has the wrong type for its field")
                if field.kind is str:
                    if len(operand) > POLICY_MAX_STRING_LITERAL:
                        raise ValueError("policy string literal is too long")
                else:
                    if not POLICY_INTEGER_MIN <= operand <= POLICY_INTEGER_MAX:
                        raise ValueError("policy integer literal is outside signed 64-bit")
                seen = literals_by_field.setdefault(name, set())
                if operand not in seen:
                    if len(seen) >= POLICY_MAX_LITERALS_PER_FIELD:
                        raise ValueError("too many comparison literals for one field")
                    seen.add(operand)
            parsed_atoms.append((op, name, operand))
        parsed_clauses.append(tuple(parsed_atoms))
    return tuple(parsed_clauses)


def _read_view(fields, raw):
    """The revision's read values for a raw object, including own defaults."""
    return {name: raw[name] if name in raw else field.default
            for name, field in fields.items()}


def _atom_holds(atom, raw, view):
    op, name, operand = atom
    if op == "present":
        return name in raw
    if op == "absent":
        return name not in raw
    if op == "eq":
        return view[name] == operand
    return view[name] != operand  # ne


def _first_failing_clause(policy, raw, view):
    """Return the zero-based index of the first violated clause, else None."""
    for index, clause in enumerate(policy):
        if not any(_atom_holds(atom, raw, view) for atom in clause):
            return index
    return None


def _policy_literals(policies):
    """Comparison literals used against each field across declarations."""
    literals = {}
    for policy in policies:
        for clause in policy:
            for op, name, operand in clause:
                if op in ("eq", "ne"):
                    literals.setdefault(name, set()).add(operand)
    return literals


def _build_domains(fields, policies):
    """Finite raw-object domains that cover every policy truth assignment.

    Each optional field can be absent or present; every field may take its
    default value and every literal compared against it.  For optional fields
    the default is reachable through absence anyway, so one state is enough.
    """
    literals = _policy_literals(policies)
    type_bases = {int: 0, str: "", bool: False}
    domains = {}
    for name, field in fields.items():
        values = set(literals.get(name, ()))
        # The type base value plus the revision's own default cover every
        # value a satisfying assignment or a violating assignment can need:
        # comparisons only distinguish literals, and presence/absence is
        # represented by the state wrapper rather than the value itself.
        values.add(type_bases[field.kind])
        if not field.required:
            values.add(field.default)
        states = tuple(sorted(values, key=lambda value: (type(value).__name__, value)))
        if field.required:
            domains[name] = [(True, value) for value in states]
        else:
            domains[name] = [(False, None)] + [(True, value) for value in states]
    return domains


def _state_predicate(atom, fields):
    """Compile an atom over this revision's variables to a state predicate."""
    op, name, operand = atom
    field = fields[name]

    def predicate(state):
        present, value = state
        if op == "present":
            return present
        if op == "absent":
            return not present
        read_value = value if present else field.default
        if op == "eq":
            return read_value == operand
        return read_value != operand

    return predicate


def _solve_cnf(domains, clauses):
    """Backtracking CNF satisfiability over finite per-variable state domains.

    Each clause is a tuple of ``(variable_name, state_predicate)`` pairs and
    every predicate is unary.  For every clause the support states of each
    participating variable are precomputed.  Search uses:

    * sound unit propagation: an open clause whose other variables are all
      assigned without satisfying it restricts its sole remaining variable to
      its support states (a domain restriction, never an unsound "force");
    * wipeout detection: an open clause with no unassigned variable still
      holding a support state contradicts the branch;
    * minimum-domain branching, tied on the number of open clauses involved.

    The result is exact (no sampling, no SAT uncertainty) and cheap for the
    bounded 32-field / 64-clause declarations; every branch works on its own
    domain copy, so failed branches leave no residue.
    """
    clause_vars = []
    supports = []
    for clause in clauses:
        variables = tuple(dict.fromkeys(name for name, _ in clause))
        clause_vars.append(variables)
        support = {}
        for name, predicate in clause:
            support.setdefault(name, ())
            support[name] = tuple(dict.fromkeys(
                support[name] + tuple(state for state in domains[name]
                                      if predicate(state))))
        supports.append(support)

    def open_clause_degree(name, assignment):
        degree = 0
        for index, variables in enumerate(clause_vars):
            if name not in variables:
                continue
            clause = clauses[index]
            if not any(n in assignment and p(assignment[n])
                       for n, p in clause):
                degree += 1
        return degree

    def search(active, assignment):
        # Fixed-point unit propagation and wipeout detection on the
        # branch-local domain copy.
        changed = True
        while changed:
            changed = False
            for index, clause in enumerate(clauses):
                if any(n in assignment and p(assignment[n])
                       for n, p in clause):
                    continue
                unassigned = [n for n in clause_vars[index]
                              if n not in assignment]
                if not unassigned:
                    return None
                if len(unassigned) == 1:
                    name = unassigned[0]
                    allowed = [state for state in active[name]
                               if state in supports[index][name]]
                    if not allowed:
                        return None
                    if len(allowed) != len(active[name]):
                        active[name] = allowed
                        changed = True
                elif not any(any(state in supports[index][name]
                                 for state in active[name])
                             for name in unassigned):
                    return None

        if len(assignment) == len(domains):
            return assignment

        name = min(
            (n for n in domains if n not in assignment),
            key=lambda n: (len(active[n]), -open_clause_degree(n, assignment)))
        for state in active[name]:
            next_active = {n: list(states) for n, states in active.items()}
            next_active[name] = [state]
            next_assignment = dict(assignment)
            next_assignment[name] = state
            result = search(next_active, next_assignment)
            if result is not None:
                return result
        return None

    initial = {name: list(states) for name, states in domains.items()}
    return search(initial, {})


def _find_own_assignment(fields, policies):
    """One raw object satisfying every own declaration, or None."""
    policies = [policy for policy in policies if policy]
    if not policies:
        return None
    domains = _build_domains(fields, policies)
    clauses = []
    for policy in policies:
        for clause in policy:
            clauses.append(tuple((atom[1], _state_predicate(atom, fields))
                                 for atom in clause))
    assignment = _solve_cnf(domains, clauses)
    if assignment is None:
        return None
    return {name: state[1] for name, state in assignment.items() if state[0]}


def _find_semantic_counterexample(writer_fields, writer_policies,
                                  reader_fields, reader_policy):
    """Search writer-raw bytes that pass every writer rule but fail a reader.

    The reader always interprets the writer's exact raw object with its own
    read rules and defaults; defaults are never filled in for the writer and
    logical aliases never rebind physical names.
    """
    policies = list(writer_policies) + [reader_policy]
    domains = _build_domains(writer_fields, policies)

    clauses = []
    for policy in writer_policies:
        for clause in policy:
            clauses.append(tuple((atom[1], _state_predicate(atom, writer_fields))
                                 for atom in clause))

    for clause_index, clause in enumerate(reader_policy):
        reader_clause = []
        permanently_satisfied = False
        for atom in clause:
            op, name, operand = atom
            if name not in writer_fields:
                # The writer can never put this physical key on the wire, so
                # the reader's atom has one fixed truth value for every writer
                # object: permanent absence, or its own defaulted read value.
                if op == "present":
                    continue  # always False, cannot help satisfy the clause
                if op == "absent":
                    permanently_satisfied = True
                    break
                read_value = reader_fields[name].default
                if ((op == "eq" and read_value == operand)
                        or (op == "ne" and read_value != operand)):
                    permanently_satisfied = True
                    break
                continue  # always False
            field = reader_fields[name]

            def predicate(state, op=op, field=field, operand=operand):
                present, value = state
                if op == "present":
                    return present
                if op == "absent":
                    return not present
                read_value = value if present else field.default
                if op == "eq":
                    return read_value == operand
                return read_value != operand

            reader_clause.append((name, predicate))
        if permanently_satisfied:
            # This reader clause holds for every object the writer may make.
            continue
        if not reader_clause:
            # Every atom is false for every object the writer may produce, so
            # the whole reader clause is violated.
            assignment = _solve_cnf(domains, clauses)
        else:
            # Negating a disjunction requires every atom to be false at once.
            # Each negated atom becomes its own single-atom conjunct; atoms
            # that mention the same variable are combined into one predicate.
            negated_by_name = {}
            for name, predicate in reader_clause:
                negated_by_name.setdefault(name, []).append(predicate)
            negation_clauses = []
            for name, predicates in negated_by_name.items():
                combined = (lambda state, preds=predicates:
                            all(not predicate(state) for predicate in preds))
                negation_clauses.append([(name, combined)])
            assignment = _solve_cnf(domains, clauses + negation_clauses)
        if assignment is not None:
            raw = {name: state[1] for name, state in assignment.items()
                   if state[0]}
            return raw, clause_index
    return None, None


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


class GuardedCodec:
    """Original physical codec plus this revision's semantic policy checks.

    The wrapper always consults the registry's *current* production limit, so
    codecs handed out before a successful ``set_emit_policy`` call observe the
    same production decisions as codecs handed out afterwards.
    """

    def __init__(self, registry, revision):
        self._registry = registry
        self._revision = revision
        self._codec = Codec(registry._versions[revision - 1])

    @property
    def fields(self):
        return self._codec._fields

    def encode(self, values):
        # Physical rules first, exactly as the original codec enforces them.
        if not isinstance(values, dict) or values.keys() - self._codec._fields.keys():
            raise ValueError("producer included undeclared fields")
        raw = self._codec._normalize(values, fill_defaults=False)
        payload = json.dumps(raw, sort_keys=True,
                             separators=(",", ":")).encode("ascii")
        fields = self._codec._fields
        view = _read_view(fields, raw)
        revision = self._revision
        policy = self._registry._policies[revision - 1]
        failing = _first_failing_clause(policy, raw, view)
        if failing is not None:
            raise PolicyViolation("read", failing)
        emit_policy = self._registry._emit_policies[revision - 1]
        failing = _first_failing_clause(emit_policy, raw, view)
        if failing is not None:
            raise PolicyViolation("emit", failing)
        return payload

    def decode(self, payload):
        # The original decode path keeps every physical rule (duplicate keys,
        # type checks, required fields, projection of undeclared keys).
        raw = json.loads(payload, object_pairs_hook=_duplicates)
        decoded = self._codec._normalize(raw, fill_defaults=True)
        fields = self._codec._fields
        failing = _first_failing_clause(
            self._registry._policies[self._revision - 1],
            raw, _read_view(fields, raw))
        if failing is not None:
            raise PolicyViolation("read", failing)
        return decoded


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
        self._policies = []
        self._emit_policies = []
        # Becomes true exactly when a non-empty semantic declaration exists.
        # It never goes back to false, which keeps the bounded scope stable.
        self._semantic_mode = False

    def _check_constrained_limits(self, candidate_fields, candidate_policy,
                                  candidate_emit, adding_revision):
        constrained = self._semantic_mode or bool(candidate_policy) \
            or bool(candidate_emit)
        if not constrained:
            return
        revision_count = len(self._versions) + (1 if adding_revision else 0)
        if revision_count > POLICY_MAX_REVISIONS:
            raise ValueError("at most 32 revisions while semantic policies are in use")
        if len(candidate_fields) > POLICY_MAX_FIELDS:
            raise ValueError("at most 32 declared fields per policy-participating revision")
        for fields in self._versions:
            if len(fields) > POLICY_MAX_FIELDS:
                raise ValueError(
                    "at most 32 declared fields per policy-participating revision")

    def register(self, fields, *, aliases=None, policy=None, emit_policy=None):
        candidate = _fields(fields)
        alias_map = _alias_map(candidate, aliases)
        candidate_policy = _parse_policy(policy, candidate)
        candidate_emit = _parse_policy(emit_policy, candidate)
        # Semantic limits only apply while declarations are actually in use;
        # the original unbounded entry points keep their historical scope.
        self._check_constrained_limits(
            candidate, candidate_policy, candidate_emit, adding_revision=True)

        candidate_revision = len(self._versions) + 1
        # All original physical compatibility checks remain mandatory and no
        # semantic declaration can exempt a revision from them.
        for revision, existing in enumerate(self._versions, 1):
            _check(existing, candidate, f"revision {revision} to candidate")
            _check(candidate, existing, f"candidate to revision {revision}")

        # The candidate itself must still have at least one producible object
        # under both of its own declarations.
        own = _find_own_assignment(
            candidate, [candidate_policy, candidate_emit])
        if (candidate_policy or candidate_emit) and own is None:
            raise PolicyUnsatisfiable(
                "candidate has no object satisfying its semantic declarations")

        # Exact, byte-level cross-revision preflight in both directions.
        for revision, existing in enumerate(self._versions, 1):
            raw, clause_index = _find_semantic_counterexample(
                candidate, [candidate_policy, candidate_emit],
                existing, self._policies[revision - 1])
            if raw is not None:
                self._raise_semantic(candidate_revision, revision, candidate,
                                     existing, raw, clause_index)
            raw, clause_index = _find_semantic_counterexample(
                existing,
                [self._policies[revision - 1],
                 self._emit_policies[revision - 1]],
                candidate, candidate_policy)
            if raw is not None:
                self._raise_semantic(revision, candidate_revision, existing,
                                     candidate, raw, clause_index)

        self._versions.append(candidate)
        self._aliases.append(alias_map)
        self._policies.append(candidate_policy)
        self._emit_policies.append(candidate_emit)
        if candidate_policy or candidate_emit:
            self._semantic_mode = True
        return len(self._versions)

    def _raise_semantic(self, writer_revision, reader_revision, writer_fields,
                        reader_fields, raw, clause_index):
        payload = Codec(writer_fields).encode(raw)
        # The counterexample must survive both ends' original read rules:
        # the writer produced it, and the reader can decode the same bytes
        # before its receiving policy turns them down.
        Codec(writer_fields).decode(payload)
        Codec(reader_fields).decode(payload)
        raise SemanticIncompatible(writer_revision, reader_revision,
                                   payload, clause_index)

    def set_emit_policy(self, revision, emit_policy):
        fields, _ = self._version(revision)
        new_emit = _parse_policy(emit_policy, fields)
        self._check_constrained_limits(
            fields, self._policies[revision - 1], new_emit,
            adding_revision=False)

        own = _find_own_assignment(
            fields, [self._policies[revision - 1], new_emit])
        if (self._policies[revision - 1] or new_emit) and own is None:
            raise PolicyUnsatisfiable(
                "revision has no object left to produce under the new limit")

        # Every currently registered reader, including this revision, must
        # still accept every object the writer may produce under the proposal.
        for reader_revision, reader_fields in enumerate(self._versions, 1):
            raw, clause_index = _find_semantic_counterexample(
                fields,
                [self._policies[revision - 1], new_emit],
                reader_fields, self._policies[reader_revision - 1])
            if raw is not None:
                self._raise_semantic(revision, reader_revision, fields,
                                     reader_fields, raw, clause_index)

        # Commit only after every check succeeded: failed replacements never
        # change the production limit observed by existing codecs.
        self._emit_policies[revision - 1] = new_emit
        if new_emit:
            self._semantic_mode = True
        return None

    def _version(self, revision):
        if type(revision) is not int or not 1 <= revision <= len(self._versions):
            raise KeyError(revision)
        return self._versions[revision - 1], self._aliases[revision - 1]

    def codec(self, revision, *, guarded=False):
        fields, _ = self._version(revision)
        if not guarded:
            return Codec(fields)
        return GuardedCodec(self, revision)

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
