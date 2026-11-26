# data-qd-237 · 消息模式兼容注册表

独立版本的写端和读端需要交换同一种 JSON 记录。`Registry.register(fields)`
检查候选对所有已注册版本的双向读写兼容性：读方必填字段必须由写方保证；
共同字段的类型必须一致。失败不占用版本号，不能取得被拒版本的编解码器。
`Registry.codec(revision)` 返回可生产和消费实际字节的编解码器，写端拒绝未声明字段，
读端只消费已声明字段并填入自己的默认值。重复 JSON 字段拒绝。

只支持单进程、单消息族、扁平 `int`、`str`、`bool`，历史版本均视为服役中；
不包括网络注册、持久化、版本退役或值语义转换。Python 3.9+，仅标准库。

```sh
python3 -m unittest -v
python3 demo.py
```

自制演示拒绝新增必填 zone，允许有默认值的 zone；新读端读取旧消息得到
`{'request': 7, 'zone': 'unknown'}`，旧读端读取新消息得到 `{'request': 8}`。

## 逻辑别名：`register(fields, *, aliases=None)`

`aliases` 把物理字段名映射到逻辑字段名，供跨版本按业务字段对齐；缺省全部同名。
显式提供时必须为每个声明的物理字段给出非空字符串逻辑名，同一版本内两个物理字段
不能指向同一逻辑名。别名随物理模式一起校验、一起冻结：注册后调用方修改原字典
不影响已注册版本；任一检查失败都不消耗版本号。别名不替代物理兼容检查。

## 滚动升级中继：`Registry.publish`

同一次发布要服务处于不同升级阶段的订阅方。`publish` 按订阅能力选择目标版本并
生成可直接交给传输层的交付批次：

```python
from contracts import DeliveryError, Field, Registry

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
variants, routes = registry.publish(
    source, payload,
    enabled={shared, zone_narrow, state_narrow},      # 本次允许生成的目标版本
    subscriptions={"zone-sub": ({shared, zone_narrow}, {"zone"}),
                   "state-sub": ({shared, state_narrow}, {"active"})},
    max_variants=1, max_bytes=61)                     # 份数与总字节预算
# variants == {shared: b'{"deployment_zone":"edge","enabled_for_dispatch":true,"id":7}'}
# routes   == {"zone-sub": shared, "state-sub": shared}
```

- 源消息按源版本真实解码（未发出的可选字段取源端默认值，未声明字段无业务含义），
  再按源版本别名得到逻辑值。
- 每个订阅方分配到唯一一个同时在 `enabled` 与自身 `accepted_revisions` 中、
  且能保真表达其所需逻辑字段的版本：值与精确类型都必须一致，布尔不能顶替整数，
  未声明的逻辑字段不能靠同名字段或默认值冒充。
- 每个被选目标版本只交付一份 JSON，由该版本的全部订阅方共享，用目标版本原
  `Codec.encode` 生成：必填字段取源端同逻辑名同类型的值；可选字段只在有订阅方
  依赖且省略会读出不同值时写出。字节预算按每份真实字节计一次，不按订阅人数计费。
- 只要存在同时满足两项预算的完整分配就返回一个（不保证唯一、最短或最少份数）；
  不存在时抛出 `DeliveryError`（`ValueError` 子类）：有订阅方完全没有可用版本时
  `reason == "uncovered"`，各自都有候选但整体超预算时 `reason == "budget"`。
  绝不降级成部分交付，也不改动注册表。空订阅返回 `({}, {})`。
- 未知版本（含订阅方声明接受但未启用的）以 `KeyError` 拒绝；重复 JSON 键、非法
  预算、非法订阅结构等无效输入以 `ValueError` 拒绝，这些检查对空订阅同样生效。
- 单次发布上限：10 个订阅方、12 个启用版本、每个参与版本 24 个物理字段、
  每个订阅方 24 个逻辑字段、输入 16 KiB。`register` 与 `codec` 不受这些上限约束；
  已注册模式的默认值再大，也不会让超预算的字节交付给调用方。

## 归档回放：`replay_window`

`replay_window(registry, records, windows, deliver, *, scratch_dir)` 先读完并封存
整个窗口，再按业务 `event_id` 去重和交付。它不会改动发布选版规则，也不要求输入流
可重读或可 seek：

```python
from contracts import Field, Registry, replay_window

registry = Registry()
rev1 = registry.register(
    {"eid": Field(str, "id-default"), "text": Field(str, "")},
    aliases={"eid": "event_id", "text": "state"})
rev2 = registry.register(
    {"name": Field(str, "id-default"), "phase": Field(str, "ready")},
    aliases={"name": "event_id", "phase": "state"})

windows = {"a": (10, 14), "b": (20, 24)}
records = [...]  # 单次遍历、任意顺序的 (partition, offset, revision, payload)
calls = []

next_offsets, blocked = replay_window(
    registry, records, windows,
    lambda partition, offset, event_id, state: calls.append(
        (partition, offset, event_id, state)),
    scratch_dir="/tmp/replay-scratch")
# 成功后：
# next_offsets == {"a": 11, "b": 24}
# blocked == {"a": "gap"}
```

回放规则：

- 窗口左闭右开；返回的 `next_offsets` 是每个分区的下一个未确认位点或 `stop`。
  `blocked` 只包含被首个障碍挡住的分区，原因为 `"gap"`、`"poison"` 或
  `"conflict"`。
- 正整数但未注册的版本、JSON/解码失败（含重复键）、缺少逻辑 `event_id`/`state`、
  逻辑值类型或 UTF-8 长度不合格，均为该位点的坏消息（`"poison"`），不是整个回放
  的输入错误。`event_id` 的物理字段必须在线上对象中实际出现；非空默认值不能补造
  身份。`state` 可以使用该版本的解码默认值。
- 同一 `event_id` 在整个窗口内出现不同 `state` 时，其全部可读副本均为冲突；冲突
  判断会参考缺位之后的副本。每个分区只确认从 `start` 开始的连续安全前缀。
- 安全前缀内的每个事件交付一次，代表是安全副本中 `(partition, offset)` 最小者；
  即使全局更小坐标的副本因缺位/冲突不在安全前缀内，也不会漏送安全代表。调用顺序
  按代表坐标升序。
- 重复 `(partition, offset)`、窗口外记录、外层结构错误或超过回放上限会以
  `ValueError` 拒绝整次回放；记录迭代抛出的原异常原样传播。这些路径以及消费者异常
  路径都不会返回确认位点，且成功读完前不会调用 `deliver`。
- 回放上限为 64 个窗口分区、200,000 条记录、32 个被引用版本、每个被引用注册模式
  24 个物理字段、每条 payload 4 KiB。它们只约束 `replay_window`。
- 长状态会溢出到 `scratch_dir` 下的临时 SQLite 暂存库；函数返回或抛异常时删除本次
  创建的临时目录。缺位通过排序后的已见位点定位，不枚举十亿级窗口。

可运行示例与内存验收：

```sh
python3 replay_example.py
python3 -m unittest -v
python3 replay_memory_check.py --records 70000 --state-bytes 1200
```

`demo.py` 演示同一消息、同一组订阅方在 1 份/61 字节（共享表示）、2 份/36 字节
（两份窄表示）与 1 份/36 字节（`budget` 失败）三种预算下的不同交付结果。
