"""A JSON record codec and a cumulative rollout compatibility registry."""
from dataclasses import dataclass
import json


class Incompatible(ValueError):
    pass


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

    def __init__(self):
        self._versions = []

    def register(self, fields):
        candidate = _fields(fields)
        for revision, existing in enumerate(self._versions, 1):
            _check(existing, candidate, f"revision {revision} to candidate")
            _check(candidate, existing, f"candidate to revision {revision}")
        self._versions.append(candidate)
        return len(self._versions)

    def codec(self, revision):
        if type(revision) is not int or not 1 <= revision <= len(self._versions):
            raise KeyError(revision)
        return Codec(self._versions[revision - 1])
