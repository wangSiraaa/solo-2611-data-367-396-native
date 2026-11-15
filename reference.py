#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
reference.py — 独立的成对参考实现（小规模 oracle），用于逐项比对每次发布。

与 dedupstore.py 不共享任何管线代码：直接对全量文档做所有两两精确比较，
再求连通分量与代表。probe.py（材料自带）是阈值 1/4 的硬编码版本，本模块
是同一数学定义的参数化版本；test_dedup.py 会在 t=1/4 下与 probe.py 交叉验证。
"""


def shingle_set(tokens, n=2):
    """相邻 token 对的集合（profile 固定二元 shingle）。"""
    return {tuple(tokens[i:i + n]) for i in range(max(0, len(tokens) - n + 1))}


def similar(a_set, b_set, p, q):
    """精确 Jaccard >= p/q，整数交叉乘法；两空集相似，恰一个空集不相似。"""
    if not a_set and not b_set:
        return True
    if not a_set or not b_set:
        return False
    inter = len(a_set & b_set)
    union = len(a_set) + len(b_set) - inter
    return inter * q >= union * p


def representative(members, docs):
    """quality 最高；并列取 UTF-8 字节序最小 id。"""
    return min(members, key=lambda d: (-docs[d][1], d.encode("utf-8")))


def reference(docs, p, q):
    """docs: {id: (tokens, quality)} → 边集 / 连通分量集合 / 各分量代表。"""
    sets = {d: shingle_set(t) for d, (t, _q) in docs.items()}
    ids = sorted(docs, key=lambda s: s.encode("utf-8"))
    edges = set()
    adj = {i: set() for i in ids}
    for i, a in enumerate(ids):
        for b in ids[i + 1:]:
            if similar(sets[a], sets[b], p, q):
                edges.add((a, b))
                adj[a].add(b)
                adj[b].add(a)
    comps = []
    seen = set()
    for s in ids:
        if s in seen:
            continue
        stack, comp = [s], set()
        seen.add(s)
        while stack:
            d = stack.pop()
            comp.add(d)
            for n in adj[d]:
                if n not in seen:
                    seen.add(n)
                    stack.append(n)
        comps.append(frozenset(comp))
    reps = {comp: representative(comp, docs) for comp in comps}
    return {"edges": edges, "components": set(comps), "representatives": reps}
