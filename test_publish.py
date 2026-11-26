from unittest import TestCase

from contracts import DeliveryError, Field, Incompatible, Registry


def build_registry(shared_zone_logical="zone", alias_refs=None):
    """The four-schema rollout family used by the relay acceptance tests."""
    registry = Registry()
    aliases = {
        "source": {"id": "id", "origin_zone": "zone", "origin_active": "active"},
        "shared": {"id": "id", "deployment_zone": shared_zone_logical,
                   "enabled_for_dispatch": "active"},
        "zone_narrow": {"id": "id", "z": "zone"},
        "state_narrow": {"id": "id", "a": "active"},
    }
    source = registry.register(
        {"id": Field(int), "origin_zone": Field(str, "edge"),
         "origin_active": Field(bool, True)}, aliases=aliases["source"])
    shared = registry.register(
        {"id": Field(int), "deployment_zone": Field(str, ""),
         "enabled_for_dispatch": Field(bool, False)}, aliases=aliases["shared"])
    zone_narrow = registry.register(
        {"id": Field(int), "z": Field(str, "")}, aliases=aliases["zone_narrow"])
    state_narrow = registry.register(
        {"id": Field(int), "a": Field(bool, False)}, aliases=aliases["state_narrow"])
    if alias_refs is not None:
        alias_refs.extend(aliases.values())
    return registry, source, shared, zone_narrow, state_narrow


def subscriptions_for(shared, zone_narrow, state_narrow):
    return {
        "zone-sub": ({shared, zone_narrow}, {"zone"}),
        "state-sub": ({shared, state_narrow}, {"active"}),
    }


class PublishSharedVsSplitTest(TestCase):
    """Acceptance 1: one shared representation versus two narrow ones."""

    def setUp(self):
        (self.registry, self.source, self.shared,
         self.zone_narrow, self.state_narrow) = build_registry()
        self.payload = self.registry.codec(self.source).encode({"id": 7})
        self.subscriptions = subscriptions_for(self.shared, self.zone_narrow,
                                               self.state_narrow)
        self.enabled = {self.shared, self.zone_narrow, self.state_narrow}

    def publish(self, max_variants, max_bytes):
        return self.registry.publish(
            self.source, self.payload, enabled=self.enabled,
            subscriptions=self.subscriptions,
            max_variants=max_variants, max_bytes=max_bytes)

    def test_shared_variant_serves_both_subscribers(self):
        variants, routes = self.publish(max_variants=1, max_bytes=61)
        self.assertEqual(set(variants), {self.shared})
        self.assertEqual(sum(map(len, variants.values())), 61)
        self.assertEqual(routes, {"zone-sub": self.shared, "state-sub": self.shared})
        # Decode the real delivered bytes with each subscriber's read version.
        view = self.registry.codec(self.shared).decode(variants[self.shared])
        self.assertEqual(view, {"id": 7, "deployment_zone": "edge",
                                "enabled_for_dispatch": True})
        self.assertIs(view["enabled_for_dispatch"], True)

    def test_two_narrow_variants_split_the_subscribers(self):
        variants, routes = self.publish(max_variants=2, max_bytes=36)
        self.assertEqual(set(variants), {self.zone_narrow, self.state_narrow})
        self.assertEqual(sum(map(len, variants.values())), 36)
        self.assertEqual(routes, {"zone-sub": self.zone_narrow,
                                  "state-sub": self.state_narrow})
        zone = self.registry.codec(self.zone_narrow).decode(variants[self.zone_narrow])
        state = self.registry.codec(self.state_narrow).decode(variants[self.state_narrow])
        self.assertEqual(zone, {"id": 7, "z": "edge"})
        self.assertEqual(state, {"id": 7, "a": True})
        self.assertIs(state["a"], True)

    def test_one_variant_with_narrow_budget_is_a_budget_error(self):
        with self.assertRaises(DeliveryError) as ctx:
            self.publish(max_variants=1, max_bytes=36)
        self.assertEqual(ctx.exception.reason, "budget")
        self.assertIsInstance(ctx.exception, ValueError)

    def test_delivery_error_reasons_are_stable_strings(self):
        self.assertEqual(DeliveryError("uncovered").reason, "uncovered")
        self.assertEqual(DeliveryError("budget").reason, "budget")


class PublishReboundAliasTest(TestCase):
    """Acceptance 2: rebinding a physical name changes who can be served."""

    def setUp(self):
        (self.registry, self.source, self.shared,
         self.zone_narrow, self.state_narrow) = build_registry(
            shared_zone_logical="locality")
        self.payload = self.registry.codec(self.source).encode({"id": 7})
        self.subscriptions = subscriptions_for(self.shared, self.zone_narrow,
                                               self.state_narrow)

    def publish(self, enabled, subscriptions=None, max_variants=1, max_bytes=61):
        return self.registry.publish(
            self.source, payload=self.payload, enabled=enabled,
            subscriptions=subscriptions if subscriptions is not None else self.subscriptions,
            max_variants=max_variants, max_bytes=max_bytes)

    def test_shared_version_still_works_physically(self):
        codec = self.registry.codec(self.shared)
        wire = codec.encode({"id": 1, "deployment_zone": "core"})
        self.assertEqual(codec.decode(wire),
                         {"id": 1, "deployment_zone": "core",
                          "enabled_for_dispatch": False})

    def test_rebound_shared_version_forces_budget_error(self):
        # zone-sub can now only use the zone-narrow version, so one variant
        # cannot cover both subscriptions any more.
        with self.assertRaises(DeliveryError) as ctx:
            self.publish(enabled={self.shared, self.zone_narrow, self.state_narrow})
        self.assertEqual(ctx.exception.reason, "budget")

    def test_without_zone_narrow_the_zone_subscription_is_uncovered(self):
        with self.assertRaises(DeliveryError) as ctx:
            self.publish(enabled={self.shared, self.state_narrow})
        self.assertEqual(ctx.exception.reason, "uncovered")

    def test_true_is_not_delivered_as_integer_one(self):
        int_active = self.registry.register(
            {"id": Field(int), "a_flag": Field(int, 0)},
            aliases={"id": "id", "a_flag": "active"})
        # The integer-typed candidate cannot faithfully carry logical "active".
        with self.assertRaises(DeliveryError) as ctx:
            self.publish(enabled={int_active},
                         subscriptions={"state-sub": ({int_active}, {"active"})},
                         max_variants=1, max_bytes=1000)
        self.assertEqual(ctx.exception.reason, "uncovered")
        # Given both, the relay picks the bool version and ships a real true.
        variants, routes = self.publish(
            enabled={self.state_narrow, int_active},
            subscriptions={"state-sub": ({self.state_narrow, int_active}, {"active"})},
            max_variants=2, max_bytes=1000)
        self.assertEqual(routes, {"state-sub": self.state_narrow})
        view = self.registry.codec(self.state_narrow).decode(variants[self.state_narrow])
        self.assertIs(view["a"], True)
        self.assertNotIn(b"1", variants[self.state_narrow])


class AliasRegistrationTest(TestCase):
    """Acceptance 3: alias validation, freezing, and registry invariants."""

    def test_invalid_aliases_do_not_consume_a_revision(self):
        registry = Registry()
        self.assertEqual(registry.register({"id": Field(int)}), 1)
        for aliases in ({}, {"id": ""}, {"id": 7}, {"id": "x", "ghost": "y"},
                        ["id"], "id"):
            with self.assertRaises(ValueError):
                registry.register({"id": Field(int)}, aliases=aliases)
        with self.assertRaises(ValueError):
            registry.register({"id": Field(int), "tag": Field(str, "")},
                              aliases={"id": "same", "tag": "same"})
        with self.assertRaises(KeyError):
            registry.codec(2)
        self.assertEqual(registry.register({"id": Field(int)},
                                           aliases={"id": "ident"}), 2)

    def test_mutating_the_alias_dict_after_registration_changes_nothing(self):
        alias_refs = []
        (registry, source, shared,
         zone_narrow, state_narrow) = build_registry(alias_refs=alias_refs)
        payload = registry.codec(source).encode({"id": 7})
        subscriptions = subscriptions_for(shared, zone_narrow, state_narrow)
        enabled = {shared, zone_narrow, state_narrow}
        baseline = registry.publish(source, payload, enabled=enabled,
                                    subscriptions=subscriptions,
                                    max_variants=1, max_bytes=61)
        for aliases in alias_refs:
            aliases.clear()
            aliases["tampered"] = "zone"
        again = registry.publish(source, payload, enabled=enabled,
                                 subscriptions=subscriptions,
                                 max_variants=1, max_bytes=61)
        self.assertEqual(baseline, again)

    def test_physical_compatibility_still_guards_nonadjacent_versions(self):
        registry = Registry()
        registry.register({"id": Field(int), "extra": Field(int, 0)},
                          aliases={"id": "ident", "extra": "extra"})
        registry.register({"id": Field(int)})
        with self.assertRaises(Incompatible):
            registry.register({"id": Field(int), "extra": Field(str, "")},
                              aliases={"id": "ident", "extra": "extra"})
        with self.assertRaises(KeyError):
            registry.codec(3)
        self.assertEqual(
            registry.register({"id": Field(int), "extra": Field(int, 3)}), 3)


class PublishInputValidationTest(TestCase):
    def setUp(self):
        (self.registry, self.source, self.shared,
         self.zone_narrow, self.state_narrow) = build_registry()
        self.payload = self.registry.codec(self.source).encode({"id": 7})

    def publish(self, source=None, payload=None, **overrides):
        call = dict(enabled={self.shared}, subscriptions={},
                    max_variants=1, max_bytes=61)
        call.update(overrides)
        return self.registry.publish(
            self.source if source is None else source,
            self.payload if payload is None else payload, **call)

    def test_empty_subscriptions_still_validate_everything(self):
        self.assertEqual(self.publish(enabled=set(), max_variants=0, max_bytes=0),
                         ({}, {}))
        with self.assertRaises(ValueError):
            self.publish(payload=b'{"id":1,"id":2}')
        with self.assertRaises(KeyError):
            self.publish(enabled={99})
        with self.assertRaises(ValueError):
            self.publish(max_variants=True)
        with self.assertRaises(ValueError):
            self.publish(max_bytes=-1)

    def test_unknown_revisions_are_key_error(self):
        with self.assertRaises(KeyError):
            self.publish(source=99)
        with self.assertRaises(KeyError):
            self.publish(source="1")
        with self.assertRaises(KeyError):
            self.publish(enabled={self.shared, 99})
        # Declared-but-not-enabled unknown revisions are rejected too.
        with self.assertRaises(KeyError):
            self.publish(subscriptions={"s": ({99}, {"id"})})

    def test_duplicate_json_keys_are_value_error_not_delivery_failure(self):
        with self.assertRaises(ValueError) as ctx:
            self.publish(payload=b'{"id":1,"id":2}',
                         subscriptions={"s": ({self.shared}, {"zone"})})
        self.assertNotIsInstance(ctx.exception, DeliveryError)

    def test_undeclared_source_fields_carry_no_business_meaning(self):
        payload = b'{"id":7,"zone":"forged","origin_zone":"edge"}'
        # "zone" is not a declared physical field of the source version: the
        # codec projects it away instead of letting it impersonate a logical.
        variants, _ = self.publish(
            payload=payload, enabled={self.zone_narrow},
            subscriptions={"s": ({self.zone_narrow}, {"zone"})})
        view = self.registry.codec(self.zone_narrow).decode(variants[self.zone_narrow])
        self.assertEqual(view, {"id": 7, "z": "edge"})

    def test_malformed_subscriptions_and_budgets(self):
        for subscriptions in ([], "s", {"": ({self.shared}, {"id"})},
                              {"s": ({self.shared},)},
                              {"s": ({self.shared}, {"id"}, {"extra"})},
                              {"s": ("1", {"id"})},
                              {"s": ({self.shared}, {"id" * 0})},
                              {"s": ({self.shared}, {"id", 7})}):
            with self.assertRaises(ValueError, msg=repr(subscriptions)):
                self.publish(subscriptions=subscriptions)
        with self.assertRaises(ValueError):
            self.publish(enabled="1")
        with self.assertRaises(ValueError):
            self.publish(max_variants=1.0)
        with self.assertRaises(ValueError):
            self.publish(payload='{"id":7}')

    def test_publish_limits_do_not_constrain_register_or_codec(self):
        registry = Registry()
        many = {"id": Field(int)}
        many.update({f"f{i}": Field(int, 0) for i in range(24)})  # 25 fields
        big = registry.register(many)  # registration itself is not limited
        codec = registry.codec(big)
        self.assertEqual(codec.decode(codec.encode({"id": 1}))["f23"], 0)
        small = registry.register({"id": Field(int)})
        with self.assertRaises(ValueError):
            registry.publish(small, b'{"id":1}', enabled={big}, subscriptions={},
                             max_variants=0, max_bytes=0)
        with self.assertRaises(ValueError):
            registry.publish(big, codec.encode({"id": 1}), enabled=set(),
                             subscriptions={}, max_variants=0, max_bytes=0)

    def test_request_size_limits(self):
        registry = Registry()
        revisions = [registry.register({"id": Field(int)}) for _ in range(13)]
        with self.assertRaises(ValueError):
            registry.publish(revisions[0], b'{"id":1}', enabled=set(revisions),
                             subscriptions={}, max_variants=0, max_bytes=0)
        with self.assertRaises(ValueError):
            self.publish(subscriptions={f"s{i}": ({self.shared}, {"id"})
                                        for i in range(11)})
        with self.assertRaises(ValueError):
            self.publish(subscriptions={"s": ({self.shared},
                                              {f"k{i}" for i in range(25)})})
        big = self.registry.codec(self.source).encode(
            {"id": 1, "origin_zone": "x" * (16 * 1024)})
        with self.assertRaises(ValueError):
            self.publish(payload=big)

    def test_oversized_default_never_silently_exceeds_the_byte_budget(self):
        big = "x" * 5000
        registry = Registry()
        source = registry.register({"id": Field(int), "note": Field(str, big)},
                                   aliases={"id": "id", "note": "note"})
        target = registry.register({"id": Field(int), "note_copy": Field(str, "")},
                                   aliases={"id": "id", "note_copy": "note"})
        payload = registry.codec(source).encode({"id": 1})  # note defaults to big
        with self.assertRaises(DeliveryError) as ctx:
            registry.publish(source, payload, enabled={target},
                             subscriptions={"s": ({target}, {"note"})},
                             max_variants=1, max_bytes=100)
        self.assertEqual(ctx.exception.reason, "budget")
        variants, routes = registry.publish(
            source, payload, enabled={target},
            subscriptions={"s": ({target}, {"note"})},
            max_variants=1, max_bytes=10_000)
        self.assertGreater(len(variants[target]), 5000)
        self.assertEqual(registry.codec(target).decode(variants[target])["note_copy"],
                         big)

    def test_uncovered_when_required_logical_is_unknown_everywhere(self):
        with self.assertRaises(DeliveryError) as ctx:
            self.publish(subscriptions={"s": ({self.shared}, {"nosuch"})})
        self.assertEqual(ctx.exception.reason, "uncovered")

    def test_partial_delivery_is_never_returned(self):
        # One coverable and one uncoverable subscription: no half result.
        with self.assertRaises(DeliveryError) as ctx:
            self.publish(enabled={self.zone_narrow},
                         subscriptions={"ok": ({self.zone_narrow}, {"zone"}),
                                        "stuck": ({self.zone_narrow}, {"active"})})
        self.assertEqual(ctx.exception.reason, "uncovered")
