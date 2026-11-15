"""独立验收 oracle（不属于候选执行链路）。

候选代码禁止导入本模块；本模块直接读取服务私有边文件穷举全部简单路径，
作为验收时的 ground truth，与候选仅经 API 得到的结果逐路径对账。

穷举口径与候选枚举器严格一致：完整标签序列、节点不重复、平行边各算独立路径、
按 (cost, edge_id 序列) 排序。
"""
import json
from pathlib import Path


def load_private_graph(private_file):
    return json.loads(Path(private_file).read_text())


def oracle_paths(rows, starts, ends, labels):
    starts = set(starts)
    ends = set(ends)
    k = len(labels)
    adj = [dict() for _ in range(k)]
    for e in rows:
        for i, label in enumerate(labels):
            if e["label"] == label:
                adj[i].setdefault(e["src"], []).append(e)
    for i in range(k):
        for node in adj[i]:
            adj[i][node].sort(key=lambda e: e["edge_id"])

    out = []

    def walk(node, hop, visited, edge_ids, node_ids, cost):
        if hop == k:
            if node in ends:
                out.append({
                    "edge_ids": list(edge_ids),
                    "node_ids": list(node_ids),
                    "cost": cost,
                })
            return
        for e in adj[hop].get(node, ()):
            if e["dst"] in visited:
                continue
            visited.add(e["dst"])
            edge_ids.append(e["edge_id"])
            node_ids.append(e["dst"])
            walk(e["dst"], hop + 1, visited, edge_ids, node_ids,
                 cost + e["cost"])
            node_ids.pop()
            edge_ids.pop()
            visited.remove(e["dst"])

    for s in sorted(starts):
        walk(s, 0, {s}, [], [s], 0)
    out.sort(key=lambda p: (p["cost"], tuple(p["edge_ids"])))
    return out


def reconcile_result(result, query, private_file):
    rows = load_private_graph(private_file)
    expected = oracle_paths(rows, query["starts"], query["ends"],
                            query["labels"])
    actual = result["paths"]
    exp_keys = [tuple(p["edge_ids"]) for p in expected]
    act_keys = [tuple(p["edge_ids"]) for p in actual]

    report = {
        "query_id": query.get("query_id"),
        "oracle_path_count": len(expected),
        "candidate_path_count": len(actual),
        "same_identity_set": exp_keys == act_keys,
        "same_order": exp_keys == act_keys,
        "missing": [list(k) for k in exp_keys if k not in set(act_keys)],
        "unexpected": [list(k) for k in act_keys if k not in set(exp_keys)],
        "cost_mismatch": [],
        "node_sequence_mismatch": [],
        "complete_flag": result["complete"],
    }
    by_id = {tuple(p["edge_ids"]): p for p in actual}
    rows_by_edge = {e["edge_id"]: e for e in rows}
    for p in expected:
        key = tuple(p["edge_ids"])
        got = by_id.get(key)
        if got is None:
            continue
        if got["cost"] != p["cost"]:
            report["cost_mismatch"].append(list(key))
        if got["node_ids"] != p["node_ids"]:
            report["node_sequence_mismatch"].append(list(key))
        # 来源核对：路径上的 edge_id 在私有图中确实存在且标签逐跳吻合
        for i, eid in enumerate(key):
            edge = rows_by_edge[eid]
            if edge["label"] != query["labels"][i]:
                report.setdefault("label_violations", []).append(list(key))

    ok = (report["same_identity_set"] and not report["cost_mismatch"]
          and not report["node_sequence_mismatch"]
          and not report.get("label_violations")
          and result["complete"])
    report["ok"] = ok
    return ok, report
