from unittest import TestCase

from contracts import Field, Incompatible, Registry
from demo import run


class ContractsTest(TestCase):
    def test_real_cross_revision_decode(self):
        self.assertEqual(run(), ({"request": 7, "zone": "unknown"}, {"request": 8}))

    def test_nonadjacent_writer_still_counts(self):
        registry = Registry()
        registry.register({"id": Field(int), "extra": Field(int, 0)})
        registry.register({"id": Field(int)})
        with self.assertRaises(Incompatible):
            registry.register({"id": Field(int), "extra": Field(str, "")})
        with self.assertRaises(KeyError):
            registry.codec(3)
        self.assertEqual(registry.register({"id": Field(int), "extra": Field(int, 3)}), 3)

    def test_candidate_cannot_stop_writing_an_old_required_field(self):
        registry = Registry()
        registry.register({"id": Field(int)})
        with self.assertRaisesRegex(Incompatible, "candidate to revision 1"):
            registry.register({"id": Field(int, 0)})

    def test_strict_encoding_and_projection(self):
        registry = Registry()
        fields = {"id": Field(int)}
        codec = registry.codec(registry.register(fields))
        fields.clear()
        for values in ({"unknown": 1}, {"id": True}, {}):
            with self.assertRaises(ValueError):
                codec.encode(values)
        with self.assertRaises(ValueError):
            codec.decode(b'{"id":1,"id":2}')
        self.assertEqual(codec.decode(b'{"id":1,"extra":2}'), {"id": 1})
