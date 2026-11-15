#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
demo_perf.py — 稀疏相似的可复现实例：5000 篇文档，阈值 1/5（探测最不利的低阈值）。

展示：实际精确相似计算次数远少于全量两两（5000*4999/2 = 12,497,500），
候选访问量与精确比较均为实际计数。不承诺高度重复语料也有此节省。

复现：python3 demo_perf.py   （随机种子固定）
"""
import os
import random
import tempfile
import time

import reference
from dedupstore import DedupStore

N = 5000
SEED = 42


def make_docs():
    rng = random.Random(SEED)
    vocab = [f"t{i}" for i in range(30000)]
    docs = []
    # 4600 篇近似独立文档（shingle 大多唯一）
    for i in range(4600):
        n = rng.randint(60, 100)
        docs.append((f"d{i:05d}", [rng.choice(vocab) for _ in range(n)], rng.randint(0, 10)))
    # 200 篇近重复：各自由一篇基础文档替换 1/4 token 派生（真实相似对）
    planted = []
    for j in range(200):
        base_id, base_tokens, _ = docs[j]
        mut = list(base_tokens)
        for _ in range(len(mut) // 4):
            mut[rng.randrange(len(mut))] = rng.choice(vocab)
        dup_id = f"dup{j:05d}"
        docs.append((dup_id, mut, rng.randint(0, 10)))
        planted.append(tuple(sorted((base_id, dup_id))))
    # 200 篇共享 5 个热门 shingle（相邻放置）、其余 token 唯一：
    # 热门 shingle 的 df 最高，被"最稀有探测键"选择系统性避开，甚至不产生候选；
    # 被迫扫描热门 posting 的情形（小集合、探测键覆盖全集）见 test_dedup.py::TestHotToken，
    # 该用例对候选访问量/精确比较次数做精确断言。
    hot_pairs = [("hot", "topic"), ("breaking", "news"), ("just", "in"),
                 ("top", "story"), ("read", "more")]
    for h in range(200):
        toks = []
        for a, b in hot_pairs:
            toks += [a, b]
        toks += [f"x{h}_{k}" for k in range(rng.randint(50, 90))]
        docs.append((f"hot{h:05d}", toks, rng.randint(0, 10)))
    return docs, planted


def main():
    docs, planted = make_docs()
    assert len(docs) == N
    tmp = tempfile.mkdtemp()
    path = os.path.join(tmp, "perf.sqlite3")
    t0 = time.monotonic()
    store = DedupStore.build(path, threshold="1/5", docs=docs)
    elapsed = time.monotonic() - t0
    stats = store.stats()
    all_pairs = N * (N - 1) // 2
    print(f"文档数            : {N}")
    print(f"阈值              : 1/5（profile 允许区间 [1/5, 1] 的低端）")
    print(f"构建耗时          : {elapsed:.1f}s")
    print(f"相似边 / 簇       : {stats['edges']} / {stats['clusters']}")
    print(f"全量两两对比数    : {all_pairs:,}")
    print(f"实际精确比较次数  : {stats['exact_comparisons']:,}  "
          f"（全量的 {stats['exact_comparisons'] / all_pairs:.4%}）")
    print(f"候选访问量        : {stats['candidate_visits']:,}")
    print(f"去重后候选数      : {stats['candidates_unique']:,}")
    assert stats["exact_comparisons"] < all_pairs, "稀疏实例必须少于全量两两"

    # 抽验：200 个植入相似对必须全部成边（候选不漏召回）
    edges = store.state()["edges"]
    missing = [e for e in planted if e not in edges]
    print(f"植入相似对召回    : {len(planted) - len(missing)}/{len(planted)}")
    assert not missing, f"漏边: {missing[:5]}"

    # 抽验：随机 300 篇子集上，库内边与独立成对参考一致
    rng = random.Random(7)
    sample = rng.sample([d[0] for d in docs], 300)
    sub = {d[0]: (d[1], d[2]) for d in docs if d[0] in sample}
    ref = reference.reference(sub, 1, 5)
    got = {(a, b) for a, b in edges if a in sub and b in sub}
    assert got == ref["edges"], "抽验子集边集与成对参考不一致"
    print(f"抽验（300 篇子集）: 边集与独立成对参考一致（{len(got)} 条）")
    print("PASS")


if __name__ == "__main__":
    main()
