from contracts import Field, Incompatible, Registry


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


if __name__ == "__main__":
    upgraded, legacy = run()
    print("required zone: refused")
    print("new reader:", upgraded)
    print("old reader:", legacy)
