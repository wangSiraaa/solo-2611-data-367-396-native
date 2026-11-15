#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
run_materials.py — 用材料（documents.json / updates.json / profile.json）构建
去重语料，每次发布都与 probe.py 成对 oracle 比对，最终导出 export.json。

用法：python3 run_materials.py [db路径] [导出路径]
"""
import json
import os
import sys

import probe
import reference
from dedupstore import DedupStore

HERE = os.path.dirname(os.path.abspath(__file__))


def main():
    db_path = sys.argv[1] if len(sys.argv) > 1 else os.path.join(HERE, "materials.sqlite3")
    out_path = sys.argv[2] if len(sys.argv) > 2 else os.path.join(HERE, "export.json")
    docs_list = json.load(open(os.path.join(HERE, "documents.json"), encoding="utf-8"))
    updates = json.load(open(os.path.join(HERE, "updates.json"), encoding="utf-8"))
    profile = json.load(open(os.path.join(HERE, "profile.json"), encoding="utf-8"))
    p = profile["jaccard_threshold"]["numerator"]
    q = profile["jaccard_threshold"]["denominator"]

    store = DedupStore.build(db_path, threshold=(p, q), overwrite=True)
    running = {}

    def check(tag):
        ref = reference.reference(running, p, q)
        st = store.state()
        assert st["edges"] == ref["edges"], f"{tag}: 边集与参考不一致"
        assert st["components"] == ref["components"], f"{tag}: 连通分量与参考不一致"
        assert st["representatives"] == ref["representatives"], f"{tag}: 代表与参考不一致"
        probe_edges = {tuple(sorted(e)) for e in probe.exact_edges(
            [{"id": i, "tokens": t[0]} for i, t in running.items()])}
        assert st["edges"] == probe_edges, f"{tag}: 边集与 probe.py 不一致"
        print(f"[v{store.version}] {tag}: 边={len(st['edges'])} "
              f"簇={len(st['components'])} 代表={sorted(st['representatives'].values())}")

    for d in docs_list:  # build：逐篇放入，逐版本校验
        store.put(d["id"], d["tokens"], d["quality"])
        running[d["id"]] = (d["tokens"], d["quality"])
        check(f"build put {d['id']}")
    for up in updates:   # 增删流：逐版本校验
        if up["op"] == "remove":
            store.remove(up["id"])
            del running[up["id"]]
            check(f"remove {up['id']}")
        else:
            doc = up["document"]
            store.put(doc["id"], doc["tokens"], doc["quality"])
            running[doc["id"]] = (doc["tokens"], doc["quality"])
            check(f"put {doc['id']}")

    doc = store.export(out_path)
    kept = [c["representative"] for c in doc["clusters"]]
    print(f"\n导出 {out_path}：版本 v{doc['version']}，{doc['summary']['documents']} 篇 → "
          f"保留代表 {kept}，折叠 {doc['summary']['collapsed']} 篇")
    print(f"实际计数器：{doc['statistics']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
