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
