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

## 语义约束注册与分阶段收紧：`policy` / `emit_policy` / guarded Codec

两个进程都能解析同一条 JSON，不代表它们会接受同一组业务字段。`register` 支持两份
语义声明，用真实字节做精确预检，让运维先收紧旧写法、再装入新的读模式：

- `policy` 是修订的**接收约束**，注册后不可变；`emit_policy` 是可替换的**额外生产
  限制**，通过 `Registry.set_emit_policy(revision, emit_policy)` 调整。两者同格式。
- 声明最外层列表/元组是条件的合取（全部成立），每个条件是非空列表/元组的析取
  （至少一个原子成立）。原子是 `(op, physical_name, operand)` 三元组：
  - `("present", name, None)` / `("absent", name, None)` 检查原始 JSON 对象里是否
    真有该物理键，默认值不算键出现；
  - `("eq", name, literal)` / `("ne", name, literal)` 比较本修订按自己的读规则得到
    的值（含自身默认值），字面量类型必须与字段完全相同，布尔不能当整数。
  - 原子只能引用本修订声明的物理名，逻辑别名不参与改绑。`None` 或空外层表示无限制。
- 每份声明至多 64 个条件、每条件至多 6 个原子、同字段至多 6 个比较字面量；字符串
  字面量至多 256 字符，整数字面量须在有符号 64 位内（这是声明字面量的范围，不是消息
  值域）。只要存在任一非空声明，注册表至多 32 个修订、每个参与修订至多 32 个声明
  字段；完全不使用约束时沿用原入口的范围。声明注册时深拷贝，调用方之后修改不生效。

```python
from contracts import Field, PolicyViolation, Registry, SemanticIncompatible

registry = Registry()
receiving = [[("ne", "channel", "live"), ("eq", "armed", True)]]
old = registry.register(
    {"id": Field(int), "channel": Field(str, "offline"),
     "armed": Field(bool, True)}, policy=receiving)

# 候选把 armed 默认翻成 False：旧写端省略开关的 live 字节会被新读端拒绝。
try:
    registry.register(
        {"id": Field(int), "channel": Field(str, "offline"),
         "armed": Field(bool, False)}, policy=receiving)
except SemanticIncompatible as error:
    error.writer_revision, error.reader_revision  # 1, 2
    error.payload          # 可由旧 Codec 生产的真实 bytes
    error.clause_index     # 被违反的接收子句（从 0 开始）

# 先收紧旧写法（一个 OR 条件：非 live，或物理上带开关），候选即可注册。
registry.set_emit_policy(
    old, [[("ne", "channel", "live"), ("present", "armed", None)]])
new = registry.register(
    {"id": Field(int), "channel": Field(str, "offline"),
     "armed": Field(bool, False)}, policy=receiving)
```

预检与运行期规则：

- 修订可生产的对象必须能用其原 `Codec` 严格编码，并同时满足自己的 `policy` 与当时
  生效的 `emit_policy`；不存在任何可生产对象时抛 `PolicyUnsatisfiable`
  （`ValueError` 子类），不能以零写入证明兼容。
- 所有原物理兼容检查保留，语义约束不豁免任何原检查。候选对每个已注册修订做两个方向的
  字节级预检：取写端在本次提议下可生产的某份原始对象，让读端用**自己的默认值**直接
  解释同一份字节；任一会被读端接收约束拒绝即抛 `SemanticIncompatible`（携带
  `writer_revision`、`reader_revision`、`payload`、`clause_index`），不消耗新号、
  不留可取候选。不抽样、不比较声明文本、不依赖求解不确定。
- `set_emit_policy` 替换生产限制：新限制须仍有可生产对象，且这些对象能被每个现存
  修订直读并通过其接收约束。收紧与放宽都允许；失败不改变生效限制。之后的注册按写修订
  **当前**允许生产的对象判断。未知修订抛 `KeyError`。
- `codec(revision)` 默认仍是原物理 Codec，不增加语义检查。`codec(revision,
  guarded=True)` 返回的 Codec 先走原有编解码规则再查约束：编码检查接收约束与**当前**
  生产限制（两者都失败先报接收约束），解码只检查接收约束；失败抛 `PolicyViolation`
  （携带 `policy_kind` 为 `"read"`/`"emit"` 与 `clause_index`）。替换成功前取得的
  guarded Codec 与之后取得的判定始终一致。

```sh
python3 semantic_policy_example.py
python3 -m unittest -v
python3 semantic_resource_check.py   # 全新进程：墙钟 < 5s、峰值 RSS < 64 MiB
```

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
