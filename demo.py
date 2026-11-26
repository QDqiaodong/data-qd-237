from contracts import DeliveryError, Field, Incompatible, Registry


def run():
    registry = Registry()
    v1 = registry.register({"request": Field(int)})
    try:
        registry.register({"request": Field(int), "zone": Field(str)})
    except Incompatible:
        rejected = True
    else:
        raise AssertionError("legacy producer cannot write a required zone")
    v2 = registry.register({"request": Field(int), "zone": Field(str, "unknown")})
    old, new = registry.codec(v1), registry.codec(v2)
    upgraded = new.decode(old.encode({"request": 7}))
    legacy = old.decode(new.encode({"request": 8, "zone": "edge"}))
    assert rejected and v2 == 2
    assert upgraded == {"request": 7, "zone": "unknown"} and legacy == {"request": 8}
    return upgraded, legacy


def publish_demo():
    """One publication, subscribers at different upgrade stages."""
    registry = Registry()
    source = registry.register(
        {"id": Field(int), "origin_zone": Field(str, "edge"),
         "origin_active": Field(bool, True)},
        aliases={"id": "id", "origin_zone": "zone", "origin_active": "active"})
    shared = registry.register(
        {"id": Field(int), "deployment_zone": Field(str, ""),
         "enabled_for_dispatch": Field(bool, False)},
        aliases={"id": "id", "deployment_zone": "zone",
                 "enabled_for_dispatch": "active"})
    zone_narrow = registry.register({"id": Field(int), "z": Field(str, "")},
                                    aliases={"id": "id", "z": "zone"})
    state_narrow = registry.register({"id": Field(int), "a": Field(bool, False)},
                                     aliases={"id": "id", "a": "active"})
    payload = registry.codec(source).encode({"id": 7})
    subscriptions = {"zone-sub": ({shared, zone_narrow}, {"zone"}),
                     "state-sub": ({shared, state_narrow}, {"active"})}
    enabled = {shared, zone_narrow, state_narrow}

    # One shared 61-byte representation can serve both subscribers.
    variants, routes = registry.publish(source, payload, enabled=enabled,
                                        subscriptions=subscriptions,
                                        max_variants=1, max_bytes=61)
    shared_view = registry.codec(shared).decode(variants[shared])
    assert routes == {"zone-sub": shared, "state-sub": shared}
    assert shared_view == {"id": 7, "deployment_zone": "edge",
                           "enabled_for_dispatch": True}

    # Two narrow 36-byte representations split them instead.
    variants, routes = registry.publish(source, payload, enabled=enabled,
                                        subscriptions=subscriptions,
                                        max_variants=2, max_bytes=36)
    zone_view = registry.codec(zone_narrow).decode(variants[zone_narrow])
    state_view = registry.codec(state_narrow).decode(variants[state_narrow])
    assert routes == {"zone-sub": zone_narrow, "state-sub": state_narrow}
    assert zone_view == {"id": 7, "z": "edge"}
    assert state_view == {"id": 7, "a": True}

    # One variant within 36 bytes cannot exist: the relay says so.
    try:
        registry.publish(source, payload, enabled=enabled,
                         subscriptions=subscriptions,
                         max_variants=1, max_bytes=36)
    except DeliveryError as error:
        reason = error.reason
    else:
        raise AssertionError("no complete assignment fits 1 variant and 36 bytes")
    assert reason == "budget"
    return (shared_view, shared), (zone_view, state_view), reason


if __name__ == "__main__":
    upgraded, legacy = run()
    print("required zone: refused")
    print("new reader:", upgraded)
    print("old reader:", legacy)
    (shared_view, shared_rev), (zone_view, state_view), reason = publish_demo()
    print(f"shared variant (rev {shared_rev}):", shared_view)
    print("narrow variants:", zone_view, state_view)
    print("1 variant / 36 bytes:", reason)
