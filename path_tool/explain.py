"""explain / audit 入口。

explain：把执行结果翻译成人类可读的取数解释——实际发了哪些请求、
为什么这样规划、每条路径由哪些边记录拼出。
audit：只基于公开 API 审计轨迹与结果本身做核对（不读私有图）；
与私有图 oracle 的对账在 path_tool.oracle / tests 中进行。
"""


def explain(result):
    lines = []
    qid = result["query_id"]
    lines.append("查询 %s | snapshot=%s | 策略=%s"
                 % (qid, result["snapshot_id"][:12], result["strategy"]))
    lines.append("终态: %s%s"
                 % (result["status"],
                    " (" + result["status_reason"] + ")"
                    if result["status_reason"] else ""))
    u = result["usage"]
    lines.append("预算消耗: 调用 %d/%d，返回边记录 %d/%d（去重后本地边 %d）"
                 % (u["calls_used"], u["max_calls"],
                    u["edges_returned"], u["max_returned_edges"],
                    u["distinct_edges_cached"]))
    lines.append("真实网络字节: request_bytes=%d, edge_payload_bytes=%d"
                 % (u["request_bytes"], u["edge_payload_bytes"]))

    decision = result.get("decision", {})
    if "probe" in decision:
        p = decision["probe"]
        lines.append("单探针真实候选数(total_matches): %s侧 %s=%d（第一页 %d 边）"
                     % (p["side"], p["label"], p["total_matches"],
                        p["first_page_edges"]))
        if decision.get("switch"):
            lines.append("换边: 是（%s；探针不续页，仅第一页计费并标记 abandoned）"
                         % decision.get("switch_rule"))
        else:
            lines.append("换边: 否（探针续页取全并直接物化）")
        lines.append("最终物化侧: %s" % decision.get("chosen_side"))
    if "split" in decision:
        lines.append("相遇切分点 split=%d" % decision["split"])

    lines.append("")
    lines.append("实际请求日志（每条都绑定同一 snapshot；token 仅在同请求翻页沿用）:")
    for e in result["request_log"]:
        req = e["request"]
        tgt = (" target_keys=%s" % req["target_keys"]
               if req.get("target_keys") is not None else "")
        tok = "" if e["token_offset"] is None else " token_offset=%d" % e["token_offset"]
        lines.append(
            "  #%d [%s] %s/%s keys=%s%s%s -> 本页 %d 边, 该请求候选 %d, has_more=%s"
            % (e["seq"], e.get("phase", ""), req["label"], req["direction"],
               req["keys"], tgt, tok, e["returned_edges"],
               e["total_matches"], e["has_more"]))

    lines.append("")
    if result["complete"]:
        lines.append("完整路径 %d 条（按 (cost, edge_id 序列) 排序）:"
                     % result["path_count"])
        for i, pth in enumerate(result["paths"], 1):
            lines.append("  %d. nodes=%s edges=%s cost=%d"
                         % (i, " -> ".join(pth["node_ids"]),
                            " -> ".join(pth["edge_ids"]), pth["cost"]))
    else:
        lines.append("未发布完整答案（%s）。partial_paths 仅为不完整前缀，共 %d 条。"
                     % (result["status"], len(result["partial_paths"])))

    lines.append("")
    lines.append("路径取边回查:")
    for key, prov in result.get("provenance", {}).items():
        lines.append("  路径 %s:" % key)
        for ed in prov["edges"]:
            r = ed["request"]
            lines.append(
                "    edge %s <- 日志 #%s %s/%s keys=%s target=%s (has_more=%s)"
                % (ed["edge_id"], ed["fetched_in_log_seq"],
                   r["label"], r["direction"], r["keys"],
                   r["target_keys"], r["has_more"]))
    return "\n".join(lines)


def audit(result, api=None):
    """公开轨迹自审。返回 (ok, checks)；checks 为逐条核对结论。"""
    checks = []

    def chk(name, ok, detail=""):
        checks.append({"check": name, "ok": bool(ok), "detail": detail})

    snap = result["snapshot_id"]
    # 1. 每个请求/响应绑定同一 snapshot
    chk("all_requests_bound_to_snapshot",
        all(e["request"]["snapshot_id"] == snap and
            e["snapshot_id"] == snap for e in result["request_log"]),
        "snapshot=%s" % snap)

    # 2. 预算口径与日志一致
    log_calls = len(result["request_log"])
    log_edges = sum(e["returned_edges"] for e in result["request_log"])
    u = result["usage"]
    chk("call_count_matches_log", log_calls == u["calls_used"],
        "%d vs %d" % (log_calls, u["calls_used"]))
    chk("returned_edge_count_matches_log", log_edges == u["edges_returned"],
        "%d vs %d" % (log_edges, u["edges_returned"]))
    chk("within_call_budget", u["calls_used"] <= u["max_calls"])
    if result["status"] == "BUDGET_EXHAUSTED":
        chk("exhausted_implies_edge_overshoot",
            u["edges_returned"] > u["max_returned_edges"],
            "超额记录保留: %d > %d"
            % (u["edges_returned"], u["max_returned_edges"]))
    else:
        chk("within_edge_budget",
            u["edges_returned"] <= u["max_returned_edges"])

    # 3. 字节计量与服务端 audit 交叉验证（若提供 api 实例）
    if api is not None and len(api.audit) == log_calls:
        byte_ok = all(
            e["request_bytes"] == s["request_bytes"] and
            e["edge_payload_bytes"] == s["edge_payload_bytes"]
            for e, s in zip(result["request_log"], api.audit))
        chk("byte_counts_match_server_audit", byte_ok)
        total_req = sum(s["request_bytes"] for s in api.audit)
        total_pay = sum(s["edge_payload_bytes"] for s in api.audit)
        chk("byte_totals_match",
            total_req == u["request_bytes"] and
            total_pay == u["edge_payload_bytes"])

    # 4. 完整性：complete 当且仅当所有“参与答案”的请求都翻到末页。
    #    被放弃的探针（abandoned）不参与组合，其 has_more 残留不影响答案完整性，
    #    但其调用与边仍真实计入预算（见第 2 条）。
    answer_log = [e for e in result["request_log"]
                  if not e.get("abandoned", False)]
    no_dangling_pages = all(not e["has_more"] for e in answer_log)
    chk("complete_iff_all_answer_pages_finished",
        result["complete"] == no_dangling_pages,
        "complete=%s, dangling=%s, abandoned_requests=%s"
        % (result["complete"],
           [e["seq"] for e in answer_log if e["has_more"]],
           [e["seq"] for e in result["request_log"]
            if e.get("abandoned")]))

    # 5. 路径上的每条边都能回查到真实日志条目，且边在本地去重边池中
    pool_ids = set()
    for e in result["request_log"]:
        pool_ids.update(e["edge_ids"])
    prov_ok = True
    for pth in result["paths"]:
        for eid in pth["edge_ids"]:
            if eid not in pool_ids:
                prov_ok = False
    chk("every_path_edge_traceable_to_log", prov_ok)

    # 6. 排序与简单路径
    ordered = sorted(result["paths"],
                     key=lambda p: (p["cost"], tuple(p["edge_ids"])))
    chk("paths_sorted_by_cost_then_edge_ids", ordered == result["paths"])
    simple = all(len(p["node_ids"]) == len(set(p["node_ids"]))
                 for p in result["paths"])
    chk("all_paths_are_simple", simple)

    # 7. 未完成时不得发布完整路径
    chk("no_paths_published_unless_complete",
        result["complete"] or result["path_count"] == 0)

    ok = all(c["ok"] for c in checks)
    return ok, checks
