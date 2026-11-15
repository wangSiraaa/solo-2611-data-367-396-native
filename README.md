# 有界交换工作负载

make_workload.py out生成4096组、12288原始记录，p0每组最大有限值+最小subnormal，p1每组负最大有限值。完整组的精确结果为最小subnormal，但p0部分状态很大且组到达顺序先覆盖全部key，不能用小字典绕过预算。

旧fixtures/input.jsonl仍用于特殊值/抵消小例。源工件与manifest只读。目标实现须真实运行两个worker与reducer并溢写，不能只读本材料提供的参考states。wire_fixture.py仅为可复用协议负例和测试partial输入的环境工具。

小样 fixtures/manifest.json 与生成工作负载使用相同 semantic_profile/chunks/path/range/sha256 字段；a/b/c.jsonl 是精确范围源片，input.jsonl 仅为连接后的oracle参考视图，不另作第四份源片。
