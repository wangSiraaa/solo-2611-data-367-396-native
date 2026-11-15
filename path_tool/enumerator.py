"""已取边上的路径组合与简单路径检查。

输入边池只允许包含“完整翻页完成的请求”返回的边（executor 保证），
因此枚举的完整性可以还原为来源访问的完整性，而不是图上的猜测。

规则：
  * 严格按 labels 序列逐跳匹配；
  * 路径不重复节点（simple path）；平行边（同 src/dst/label、不同 edge_id）
    各自产生独立路径；
  * 负 cost 合法；输出按 (cost 总和, edge_id 序列) 排序；
  * 同一 edge_id 即使经多个 API 请求取得，在边池里只有一条，不会复制路径。
"""


def enumerate_paths(edges, starts, ends, labels):
    starts = set(starts)
    ends = set(ends)
    k = len(labels)

    # adj[hop][node] -> 该跳标签下从 node 出发的边列表
    adj = [dict() for _ in range(k)]
    pool = {}
    for e in edges:
        pool[e["edge_id"]] = e
    for e in edges:
        for i, label in enumerate(labels):
            if e["label"] == label:
                adj[i].setdefault(e["src"], []).append(e)
    for i in range(k):
        for node in adj[i]:
            adj[i][node].sort(key=lambda e: e["edge_id"])

    results = []

    def walk(node, hop, visited, edge_ids, node_ids, cost):
        if hop == k:
            if node in ends:
                results.append({
                    "edge_ids": list(edge_ids),
                    "node_ids": list(node_ids),
                    "cost": cost,
                })
            return
        for e in adj[hop].get(node, ()):
            nxt = e["dst"]
            if nxt in visited:
                continue  # 不重复节点：会回到起点/中间节点的伪路径在此被拒
            visited.add(nxt)
            edge_ids.append(e["edge_id"])
            node_ids.append(nxt)
            walk(nxt, hop + 1, visited, edge_ids, node_ids, cost + e["cost"])
            node_ids.pop()
            edge_ids.pop()
            visited.remove(nxt)

    for s in sorted(starts):
        walk(s, 0, {s}, [], [s], 0)

    results.sort(key=lambda p: (p["cost"], tuple(p["edge_ids"])))
    # 去重：同一 edge_id 组合只能出现一次（边池已按 edge_id 去重，这里双保险）
    deduped = []
    seen = set()
    for p in results:
        key = tuple(p["edge_ids"])
        if key not in seen:
            seen.add(key)
            deduped.append(p)
    return deduped, pool
