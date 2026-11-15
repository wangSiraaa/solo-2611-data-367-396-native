"""查询规划：只依据 capabilities 与查询本身，不访问任何边数据。

路径标签序列 labels[0..k-1]，第 i 跳：
  * 前向（out）：按 src 取边，要求 labels[i] 支持 out；
  * 后向（in）：从终点侧反向物化，按 dst 取边，要求 labels[i] 支持 in。

一个切分点 s（0..k）表示：前 s 跳前向物化，其余后向物化，在第 s 跳的边处
“两端相遇”。切分点可行当且仅当 labels[0:s] 全部支持 out 且 labels[s:k] 全部支持 in。
目标过滤 target_keys 只能用于 out 且能力声明 target_filter 的标签，
在相遇跳（前向侧最后一跳）用对侧前沿作为目标约束。
"""


def analyze_query(query, capabilities):
    labels = query["labels"]
    if not labels or not (1 <= len(labels) <= 4):
        raise ValueError("labels must be a non-empty sequence of length 1..4")
    if not query.get("starts") or not query.get("ends"):
        raise ValueError("starts and ends must be non-empty")
    if len(query["starts"]) > 4 or len(query["ends"]) > 4:
        raise ValueError("at most 4 starts and 4 ends are supported")
    caps = capabilities["labels"]
    hops = []
    for i, label in enumerate(labels):
        spec = caps.get(label)
        if spec is None:
            raise ValueError("UNKNOWN_LABEL: %s" % label)
        hops.append({
            "hop": i,
            "label": label,
            "out": "out" in spec["directions"],
            "in": "in" in spec["directions"],
            "target_filter": bool(spec.get("target_filter", False)),
        })
    return hops


def feasible_splits(hops):
    """枚举全部可行切分点。至少存在一个可行切分点才可能回答查询。"""
    k = len(hops)
    splits = []
    for s in range(k + 1):
        forward_ok = all(h["out"] for h in hops[:s])
        backward_ok = all(h["in"] for h in hops[s:])
        if forward_ok and backward_ok:
            splits.append(s)
    return splits


def _meeting_target_filter_ok(hops, split):
    """相遇跳能否用 target_keys 收窄：前向侧最后一跳 labels[split-1]。"""
    if split == 0:
        return False  # 纯后向：没有前向 out 请求可挂 target_keys
    last_forward = hops[split - 1]
    return last_forward["out"] and last_forward["target_filter"]


def build_plan(query, capabilities):
    """返回静态计划 dict（不含任何边数据与请求结果）。"""
    hops = analyze_query(query, capabilities)
    splits = feasible_splits(hops)
    k = len(hops)
    if not splits:
        return {
            "feasible": False,
            "reason": "no split point: each label needs out on the prefix "
                      "and in on the suffix under declared capabilities",
            "hops": hops,
            "splits": [],
        }

    can_probe_forward = hops[0]["out"]
    can_probe_backward = hops[-1]["in"]
    # 自适应单探针：静态选向（首个标签 out+target_filter 优先前向，否则末标签 in
    # 优先后向），只对首选侧发一个真实首跳请求，再按真实 total_matches 决定
    # 是否至多换边一次；未选中侧不产生任何请求，避免双探针双倍页成本。
    adaptive = can_probe_forward or can_probe_backward

    split_plans = []
    for s in splits:
        split_plans.append({
            "split": s,
            "forward_labels": [h["label"] for h in hops[:s]],
            "backward_labels": [h["label"] for h in hops[s:]],
            "meeting_hop": None if s in (0, k) else s - 1,
            "meeting_target_filter": _meeting_target_filter_ok(hops, s),
            "shape": ("pure-forward" if s == k else
                      "pure-backward" if s == 0 else "meet-in-middle"),
        })

    # 单探针回退方案（只有一侧首跳可查时，唯一可行侧先物化）
    fallback = None
    if not adaptive:
        if can_probe_forward and k in splits:
            fallback = "forward-only"
        elif can_probe_backward and 0 in splits:
            fallback = "backward-only"

    return {
        "feasible": True,
        "hops": hops,
        "splits": splits,
        "split_plans": split_plans,
        "first_hop_feasible": adaptive,
        "fallback": fallback,
        "probe": {
            "forward": {
                "label": hops[0]["label"],
                "direction": "out",
                "keys": sorted(query["starts"]),
                "target_filter_capable": hops[0]["target_filter"],
            } if can_probe_forward else None,
            "backward": {
                "label": hops[-1]["label"],
                "direction": "in",
                "keys": sorted(query["ends"]),
            } if can_probe_backward else None,
        },
    }
