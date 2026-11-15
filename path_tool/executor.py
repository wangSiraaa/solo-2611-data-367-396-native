"""执行器：把规划落成真实 API 请求，并在已完整取回的边上枚举路径。

自适应策略（single-probe + at-most-one switch）：
  1. 静态选向：首个标签支持 out 且带 target_filter 时优先前向（可在末跳用
     终点约束收窄）；否则在末标签支持 in 时优先后向；都不可行则回退唯一方向。
  2. 只对首选侧发“一个”真实首跳请求并完整翻页，得到服务端真实 total_matches。
  3. 若该侧候选规模明显过大（超过边预算一半）且对侧首跳可查，则换边一次：
     已发请求的调用与边照计预算，但因其结果不参与最终物化，边不进组合边池；
     换边后绝不回头，避免反复试探。
  4. 不换边时首跳结果直接进入物化，无任何重复请求。

完整性协议（三个状态协作，缺一不可）：
  * 来源分页：进入组合边池的每个逻辑请求都逐页取到 next_token is null；
    被放弃的探针单独标记 abandoned，不参与组合，也不构成“未取全的答案边”；
  * 预算：先找到低 cost 路径、某页为空、达到调用预算都不能提前宣布完整；
    预算在发请求前耗尽 -> INCOMPLETE；实际响应使返回边超边预算
    -> BUDGET_EXHAUSTED（超额记录保留并如实上报）；
  * 组合：只有全部所需请求完整完成（status==OK）才枚举并标记 complete，
    否则不发布任何“完整答案”，已枚举内容仅作为不完整前缀附带说明。
"""
from .engine import FetchEngine, BudgetStop, OK, ERROR
from .planner import build_plan, analyze_query
from .enumerator import enumerate_paths


def _add_edges(pool, edges):
    for e in edges:  # 同一 edge_id 经不同合法请求取得仍是一条边
        pool[e["edge_id"]] = e


def _out_frontier(edges):
    return {e["dst"] for e in edges}


def _in_frontier(edges):
    return {e["src"] for e in edges}


def _accumulate(reachable, new_nodes):
    """累积可达节点集：节点在任一深度可达，就应纳入后续每一跳的前沿。

    不能只保留“上一跳”的节点——一条回到早先节点的边（循环）会让该节点
    在新的深度再次需要扩展；简单路径（不重复节点）的判断只在枚举器做，
    物化阶段不得按访问历史剪枝，否则会漏掉该节点其他出边上的合法路径。
    """
    reachable.update(new_nodes)
    return reachable


def execute_query(api, query, strategy="adaptive", force_split=None):
    """对一个查询从空缓存开始执行。

    strategy:
      adaptive       —— 单探针静态选向 + 至多一次换边（默认）
      pure-forward   —— 强制纯前向（需要全部标签支持 out）
      pure-backward  —— 强制纯后向（需要全部标签支持 in）
    force_split: 可选，直接指定相遇切分点 s（0..k），必须是可行切分点；
      用于方案对比/解释，跳过自适应选向，从对应侧按切分点物化。
    """
    engine = FetchEngine(api)  # 每个查询新建引擎 => 全新空本地缓存
    budget = query["budget"]
    engine.bind_budget(budget["max_calls"], budget["max_returned_edges"])

    try:
        hops = analyze_query(query, engine.capabilities)
    except ValueError as exc:
        engine._fail(ERROR, str(exc))
        return _result(engine, query, strategy, plan=None, pool={},
                       decision={"error": str(exc)})

    plan = build_plan(query, engine.capabilities)
    if not plan["feasible"]:
        engine._fail(ERROR, plan["reason"])
        return _result(engine, query, strategy, plan=plan, pool={},
                       decision={"error": plan["reason"]})

    labels = query["labels"]
    k = len(labels)
    pool = {}
    decision = {"strategy": strategy, "actions": []}

    try:
        if force_split is not None:
            if force_split not in plan["splits"]:
                raise _PlanError(
                    "forced split %d not feasible; feasible=%s"
                    % (force_split, plan["splits"]))
            decision["strategy"] = "forced-split-%d" % force_split
            decision["split"] = force_split
            if force_split == k:
                _run_pure_forward(engine, query, hops, pool, decision)
            elif force_split == 0:
                _run_pure_backward(engine, query, hops, pool, decision)
            else:
                _forward_meet(engine, query, plan, pool, decision,
                              force_split, start_hop=0,
                              f_frontier=set(query["starts"]))
        elif strategy == "pure-forward":
            if k not in plan["splits"]:
                raise _PlanError("pure-forward infeasible: some label lacks out")
            decision["start_side"] = "forward"
            _run_pure_forward(engine, query, hops, pool, decision)
        elif strategy == "pure-backward":
            if 0 not in plan["splits"]:
                raise _PlanError("pure-backward infeasible: some label lacks in")
            decision["start_side"] = "backward"
            _run_pure_backward(engine, query, hops, pool, decision)
        elif strategy == "adaptive":
            if not plan["first_hop_feasible"]:  # 不存在任何可查的首跳
                raise _PlanError(plan["reason"])
            _run_adaptive(engine, query, hops, plan, pool, decision)
        else:
            raise _PlanError("unknown strategy: %s" % strategy)
    except BudgetStop as stop:
        engine._fail(stop.status, engine.status_reason or stop.status)
    except _PlanError as exc:
        engine._fail(ERROR, str(exc))
        return _result(engine, query, strategy, plan=plan, pool=pool,
                       decision=decision)

    return _result(engine, query, strategy, plan=plan, pool=pool,
                   decision=decision)


class _PlanError(Exception):
    pass


# ---- 纯前向 / 纯后向 ----------------------------------------------------

def _run_pure_forward(engine, query, hops, pool, decision):
    """s=k：全部跳前向。末跳在能力允许时用 ends 做 target_keys 收窄。"""
    labels = query["labels"]
    k = len(labels)
    reachable = set(query["starts"])
    for i in range(k):
        label = labels[i]
        targets = None
        if i == k - 1 and engine.supports_target_filter(label) \
                and len(query["ends"]) <= 4:
            targets = sorted(query["ends"])
        edges, status = engine.expand_frontier(
            label, "out", reachable,
            phase="forward hop %d (%s)" % (i, label),
            target_keys=targets,
        )
        if status != OK:
            raise BudgetStop(status)
        _add_edges(pool, edges)
        decision["actions"].append({
            "hop": i, "direction": "out", "label": label,
            "frontier_size": len(reachable),
            "target_keys": targets, "new_edges": len(edges),
        })
        _accumulate(reachable, _out_frontier(edges))
        if not edges:
            break


def _run_pure_backward(engine, query, hops, pool, decision):
    """s=0：全部跳后向（in），从 ends 反推到 starts。"""
    labels = query["labels"]
    k = len(labels)
    reachable = set(query["ends"])
    for j in range(k - 1, -1, -1):
        label = labels[j]
        edges, status = engine.expand_frontier(
            label, "in", reachable,
            phase="backward hop %d (%s)" % (j, label),
        )
        if status != OK:
            raise BudgetStop(status)
        _add_edges(pool, edges)
        decision["actions"].append({
            "hop": j, "direction": "in", "label": label,
            "frontier_size": len(reachable),
            "new_edges": len(edges),
        })
        _accumulate(reachable, _in_frontier(edges))
        if not edges:
            break


# ---- 自适应：单探针 + 至多一次换边 --------------------------------------

def _static_start_side(engine, query, hops, plan):
    """依据能力静态选择首探针方向，不访问任何边数据。"""
    first, last = hops[0], hops[-1]
    if first["out"] and first["target_filter"]:
        return "forward"                      # 末跳可挂终点 target 约束
    if last["in"]:
        return "backward"
    if first["out"]:
        return "forward"
    if last["in"]:
        return "backward"
    raise _PlanError(plan["reason"])


def _run_adaptive(engine, query, hops, plan, pool, decision):
    labels = query["labels"]
    k = len(labels)
    start_side = _static_start_side(engine, query, hops, plan)
    decision["start_side"] = start_side
    edge_budget = engine.budget.max_returned_edges

    # ---- 首选侧首跳：只取第一页，立即拿到服务端真实 total_matches ----
    # 关键：是否续页取决于换边判据，避免对一个 200/301 边的请求盲目翻页撑爆预算。
    if start_side == "forward":
        probe_label = labels[0]
        first_edges, total, pending = engine.fetch_probe_page(
            probe_label, "out", sorted(query["starts"]),
            phase="probe forward hop 0 (%s)" % probe_label)
        alt_feasible = hops[-1]["in"]
    else:
        probe_label = labels[-1]
        first_edges, total, pending = engine.fetch_probe_page(
            probe_label, "in", sorted(query["ends"]),
            phase="probe backward hop %d (%s)" % (k - 1, probe_label))
        alt_feasible = hops[0]["out"]
    probe_seq = engine.log[-1]["seq"]
    probe = {"side": start_side, "label": probe_label,
             "total_matches": total,
             "first_page_edges": len(first_edges), "log_seq": probe_seq}

    # ---- 换边判据：真实候选数超过边预算一半且对侧首跳可查 ----
    switch = alt_feasible and total > edge_budget // 2
    decision["probe"] = probe
    decision["switch"] = switch
    decision["switch_rule"] = \
        "switch at most once when real total_matches > edge_budget/2 " \
        "and the opposite first hop is capability-feasible; a switched-away " \
        "probe is never paginated, so only its first page is paid for"

    if switch:
        # 探针第一页不续页：标记 abandoned，边绝不入组合池；调用与第一页边照计。
        engine.log[probe_seq - 1]["abandoned"] = True
        engine.log[probe_seq - 1]["abandon_reason"] = \
            "probe total_matches=%d too large; switched to opposite side " \
            "without paginating this request" % total
        decision["chosen_side"] = "backward" if start_side == "forward" else "forward"
        if decision["chosen_side"] == "forward":
            _materialize_forward(engine, query, hops, plan, pool, decision)
        else:
            _materialize_backward(engine, query, hops, plan, pool, decision)
        return

    # 不换边：续完探针请求，全部边入池，首跳物化完成
    edges, _ = engine.resume_request(pending)
    decision["chosen_side"] = start_side
    _add_edges(pool, edges)
    if total == 0:
        decision["early_empty"] = \
            "first hop returned zero edges over all pages => no path can exist"
        return
    if start_side == "forward":
        _continue_forward(engine, query, hops, plan, pool, decision,
                          completed_hop=0, frontier=_out_frontier(edges),
                          probe_total=total)
    else:
        _continue_backward(engine, query, hops, plan, pool, decision,
                           completed_hop=k - 1, frontier=_in_frontier(edges),
                           probe_total=total)


# ---- 选定方向后的物化 ---------------------------------------------------

def _materialize_forward(engine, query, hops, plan, pool, decision):
    """换边后从零开始纯前向物化（split=k），末跳尽量 target 过滤。"""
    if len(hops) not in plan["splits"]:
        # 纯前向不可行时，采用最大可行切分点的相遇方案
        split = max(plan["splits"])
        _forward_meet(engine, query, plan, pool, decision, split,
                      start_hop=0, f_frontier=set(query["starts"]))
        return
    _run_pure_forward(engine, query, hops, pool, decision)


def _materialize_backward(engine, query, hops, plan, pool, decision):
    """换边后从零开始纯后向物化（split=0）。"""
    if 0 not in plan["splits"]:
        split = min(plan["splits"])
        _backward_meet(engine, query, plan, pool, decision, split,
                       start_hop=len(hops) - 1,
                       b_frontier=set(query["ends"]))
        return
    _run_pure_backward(engine, query, hops, pool, decision)


def _continue_forward(engine, query, hops, plan, pool, decision,
                      completed_hop, frontier, probe_total):
    """前向首跳已物化，选择切分点继续。

    选择启发式（真实候选数驱动，非估计）：
      探针 total_matches 未超边预算一半 => 前向侧选择性好，优先纯前向 split=k
      （末跳可用 target_keys 终点过滤）；否则取最小可行切分点尽早与后向相遇。
    """
    k = len(hops)
    edge_budget = engine.budget.max_returned_edges
    if k in plan["splits"] and probe_total <= edge_budget // 2:
        split = k
    else:
        split = next((s for s in plan["splits"] if s > completed_hop), None)
    if split is None:
        raise _PlanError(plan["reason"])
    decision["split"] = split
    if split == len(hops):
        # 一路前向到终点
        labels = query["labels"]
        k = len(labels)
        for i in range(completed_hop + 1, k):
            label = labels[i]
            targets = None
            if i == k - 1 and engine.supports_target_filter(label) \
                    and len(query["ends"]) <= 4:
                targets = sorted(query["ends"])
            edges, status = engine.expand_frontier(
                label, "out", frontier,
                phase="forward hop %d (%s)" % (i, label),
                target_keys=targets)
            if status != OK:
                raise BudgetStop(status)
            _add_edges(pool, edges)
            decision["actions"].append({
                "hop": i, "direction": "out", "label": label,
                "frontier_size": len(frontier), "target_keys": targets,
                "new_edges": len(edges)})
            _accumulate(frontier, _out_frontier(edges))
            if not edges:
                return
        return
    _forward_meet(engine, query, plan, pool, decision, split,
                  start_hop=completed_hop + 1, f_frontier=frontier)


def _continue_backward(engine, query, hops, plan, pool, decision,
                       completed_hop, frontier, probe_total):
    """后向首跳已物化，选择切分点继续。

    探针 total_matches 未超边预算一半 => 后向侧选择性好，优先纯后向 split=0；
    否则取 <= completed_hop 的最大可行切分点，尽早与前向相遇。
    """
    edge_budget = engine.budget.max_returned_edges
    if 0 in plan["splits"] and probe_total <= edge_budget // 2:
        split = 0
    else:
        split = None
        for s in sorted(plan["splits"], reverse=True):
            if s <= completed_hop:
                split = s
                break
    if split is None:
        raise _PlanError(plan["reason"])
    decision["split"] = split
    if split == 0:
        labels = query["labels"]
        for j in range(completed_hop - 1, -1, -1):
            label = labels[j]
            edges, status = engine.expand_frontier(
                label, "in", frontier,
                phase="backward hop %d (%s)" % (j, label))
            if status != OK:
                raise BudgetStop(status)
            _add_edges(pool, edges)
            decision["actions"].append({
                "hop": j, "direction": "in", "label": label,
                "frontier_size": len(frontier), "new_edges": len(edges)})
            _accumulate(frontier, _in_frontier(edges))
            if not edges:
                return
        return
    _backward_meet(engine, query, plan, pool, decision, split,
                   start_hop=completed_hop - 1, b_frontier=frontier)


def _forward_meet(engine, query, plan, pool, decision, split,
                  start_hop, f_frontier):
    """前向侧物化到 split-1，后向侧物化 split..k-1，相遇跳用 target 收窄。

    同时维护两套集合：累积可达集用于请求 keys（循环回到早先节点也不漏取），
    精确深度节点集 b_exact 用于相遇 target_keys（只含恰在切分深度到达的节点）。
    """
    labels = query["labels"]
    k = len(labels)
    sp = next(sp for sp in plan["split_plans"] if sp["split"] == split)

    # 后向侧先物化，得到“恰在切分深度 split 可达”的相遇节点集合
    b_reachable = set(query["ends"])
    b_exact = set(query["ends"])
    for j in range(k - 1, split - 1, -1):
        label = labels[j]
        edges, status = engine.expand_frontier(
            label, "in", b_reachable,
            phase="backward hop %d (%s)" % (j, label))
        if status != OK:
            raise BudgetStop(status)
        _add_edges(pool, edges)
        decision["actions"].append({
            "hop": j, "direction": "in", "label": label,
            "frontier_size": len(b_reachable), "new_edges": len(edges)})
        b_exact = _in_frontier(edges)
        _accumulate(b_reachable, b_exact)
        if not edges:
            return

    # 前向侧继续到相遇跳
    f_reachable = set(f_frontier)
    for i in range(start_hop, split):
        label = labels[i]
        is_meeting = (i == split - 1)
        targets = None
        if is_meeting and sp["meeting_target_filter"] and len(b_exact) <= 4:
            targets = sorted(b_exact)
        edges, status = engine.expand_frontier(
            label, "out", f_reachable,
            phase=("meeting " if is_meeting else "") +
                  "forward hop %d (%s)" % (i, label),
            target_keys=targets)
        if status != OK:
            raise BudgetStop(status)
        _add_edges(pool, edges)
        decision["actions"].append({
            "hop": i, "direction": "out", "label": label,
            "frontier_size": len(f_reachable), "target_keys": targets,
            "new_edges": len(edges), "meeting": is_meeting})
        _accumulate(f_reachable, _out_frontier(edges))
        if not edges:
            return


def _backward_meet(engine, query, plan, pool, decision, split,
                   start_hop, b_frontier):
    """后向侧物化到 split，前向侧物化 0..split-1，相遇跳用 target 收窄。"""
    labels = query["labels"]
    sp = next(sp for sp in plan["split_plans"] if sp["split"] == split)

    # 后向侧继续到 split；b_exact 跟踪恰在切分深度可达的节点
    b_reachable = set(b_frontier)
    b_exact = set(b_frontier)
    for j in range(start_hop, split - 1, -1):
        label = labels[j]
        edges, status = engine.expand_frontier(
            label, "in", b_reachable,
            phase="backward hop %d (%s)" % (j, label))
        if status != OK:
            raise BudgetStop(status)
        _add_edges(pool, edges)
        decision["actions"].append({
            "hop": j, "direction": "in", "label": label,
            "frontier_size": len(b_reachable), "new_edges": len(edges)})
        b_exact = _in_frontier(edges)
        _accumulate(b_reachable, b_exact)
        if not edges:
            return

    # 前向侧物化到相遇跳
    f_reachable = set(query["starts"])
    for i in range(0, split):
        label = labels[i]
        is_meeting = (i == split - 1)
        targets = None
        if is_meeting and sp["meeting_target_filter"] and len(b_exact) <= 4:
            targets = sorted(b_exact)
        edges, status = engine.expand_frontier(
            label, "out", f_reachable,
            phase=("meeting " if is_meeting else "") +
                  "forward hop %d (%s)" % (i, label),
            target_keys=targets)
        if status != OK:
            raise BudgetStop(status)
        _add_edges(pool, edges)
        decision["actions"].append({
            "hop": i, "direction": "out", "label": label,
            "frontier_size": len(f_reachable), "target_keys": targets,
            "new_edges": len(edges), "meeting": is_meeting})
        _accumulate(f_reachable, _out_frontier(edges))
        if not edges:
            return


# ---- 结果组装 -----------------------------------------------------------

def _result(engine, query, strategy, plan, pool, decision):
    complete = engine.status == OK
    paths, edge_pool = enumerate_paths(
        list(pool.values()), query["starts"], query["ends"], query["labels"]
    )
    provenance = _build_provenance(engine, paths, edge_pool)
    return {
        "query_id": query.get("query_id"),
        "snapshot_id": engine.snapshot_id,
        "strategy": strategy,
        "status": engine.status,
        "status_reason": engine.status_reason,
        "complete": complete,
        "plan": plan,
        "decision": decision,
        "paths": paths if complete else [],
        "partial_paths": [] if complete else paths,
        "path_count": len(paths) if complete else 0,
        "edge_pool_size": len(pool),
        "provenance": provenance,
        "usage": engine.usage(),
        "request_log": engine.log,
    }


def _build_provenance(engine, paths, edge_pool):
    """每条路径边 -> 真正把它纳入答案边池的完整请求（排除 abandoned 探针）。

    abandoned 探针的边从不进入组合池，因此即使其第一页恰好含同名 edge_id，
    也不能记为该路径边的来源；同一 edge_id 经多个完整请求取得时全部列出，
    但边在池中只有一条，不会复制成额外路径。
    """
    edge_to_entries = {}
    abandoned_hits = {}
    for entry in engine.log:
        for eid in entry["edge_ids"]:
            if entry.get("abandoned"):
                abandoned_hits.setdefault(eid, []).append(entry["seq"])
            else:
                edge_to_entries.setdefault(eid, []).append(entry["seq"])
    out = {}
    for p in paths:
        record = {"snapshot_id": engine.snapshot_id, "edges": []}
        for eid in p["edge_ids"]:
            item = {
                "edge_id": eid,
                "fetched_in_log_seq": edge_to_entries.get(eid, []),
                "request": _request_summary(engine, eid),
            }
            if eid in abandoned_hits:
                item["also_seen_in_abandoned_probe_seq"] = abandoned_hits[eid]
            record["edges"].append(item)
        out["|".join(p["edge_ids"])] = record
    return out


def _request_summary(engine, edge_id):
    for entry in engine.log:
        if edge_id in entry["edge_ids"] and not entry.get("abandoned"):
            req = entry["request"]
            return {
                "label": req["label"],
                "direction": req["direction"],
                "keys": req["keys"],
                "target_keys": req.get("target_keys"),
                "log_seq": entry["seq"],
                "has_more": entry["has_more"],
                "abandoned": entry.get("abandoned", False),
            }
    return None
