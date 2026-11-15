"""取数引擎：把所有 GraphAPI 访问收口到这一层。

职责：
  * 绑定开始时读取到的 snapshot_id，后续每个请求都携带它；
  * 管理分页 token：token 只在“同一完整请求（snapshot/方向/label/keys/target_keys）”
    的连续翻页中沿用，前沿或过滤一变就发新请求，绝不复用旧 token；
  * 预算控制：发请求前检查剩余调用数；某响应返回边数超过剩余边预算时，
    先如实记账（超额的调用与边照计），再以 BUDGET_EXHAUSTED 结束，不丢弃记录；
  * 记录真实 request_bytes / edge_payload_bytes（与服务端 audit 口径一致，
    不把服务端扫描数当网络传输）；
  * 同一逻辑请求的结果按 edge_id 去重缓存；重复请求仍然真实发出并照计预算，
    只是返回同一边时不会在候选边集合里复制成第二条。
"""
import json

# 执行终态
OK = "OK"                      # 所有需要的请求都完整翻页，答案完整
INCOMPLETE = "INCOMPLETE"      # 预算在“发出下一个请求之前”耗尽，无法保证完整
BUDGET_EXHAUSTED = "BUDGET_EXHAUSTED"  # 某次实际响应使累计返回边超出边预算
ERROR = "ERROR"                # 协议/参数类错误


def canonical_request(request):
    """与服务端 query_hash 同口径的规范编码，用于本地缓存键。"""
    return json.dumps(request, sort_keys=True, separators=(",", ":")).encode()


class Budget:
    def __init__(self, max_calls, max_returned_edges):
        self.max_calls = int(max_calls)
        self.max_returned_edges = int(max_returned_edges)
        self.calls_used = 0
        self.edges_returned = 0          # 累计“返回边记录数”，重复传输照计
        self.request_bytes = 0
        self.edge_payload_bytes = 0

    def call_room(self):
        return self.max_calls - self.calls_used

    def edge_room(self):
        return self.max_returned_edges - self.edges_returned


class FetchEngine:
    """所有真实 API 访问的唯一出口。executor 只与本类打交道。"""

    PAGE_SIZE = 25

    def __init__(self, api):
        self.api = api
        caps = api.capabilities()           # 元数据调用，不计边查询预算
        self.snapshot_id = caps["snapshot_id"]
        self.capabilities = caps
        self.max_keys = caps.get("max_keys", 4)
        self.budget = None
        self.log = []                       # 逐请求真实日志
        # 完整请求 -> 该请求已取全的边列表（按 edge_id 排序，去重）
        self._cache = {}
        self.status = OK
        self.status_reason = None

    # ---- 基础工具 -------------------------------------------------------

    def bind_budget(self, max_calls, max_returned_edges):
        self.budget = Budget(max_calls, max_returned_edges)

    @staticmethod
    def chunks(keys, size):
        keys = list(keys)
        return [keys[i:i + size] for i in range(0, len(keys), size)]

    def supports(self, label, direction):
        spec = self.capabilities["labels"].get(label)
        return bool(spec) and direction in spec["directions"]

    def supports_target_filter(self, label):
        spec = self.capabilities["labels"].get(label)
        return bool(spec) and spec.get("target_filter", False)

    # ---- 低层调用 -------------------------------------------------------

    def _call(self, request, token):
        """发一次真实 API 调用并记账。返回 response，或在预算/协议失败时抛异常。"""
        b = self.budget
        # 发请求前检查剩余调用数
        if b.call_room() <= 0:
            self._fail(INCOMPLETE, "no call budget before issuing request")
            raise BudgetStop(INCOMPLETE)
        try:
            resp = self.api.query_edges(request, token)
        except ValueError as exc:
            # 服务端协议错误（SNAPSHOT_MISMATCH / UNSUPPORTED_DIRECTION /
            # BAD_KEYS / UNSUPPORTED_TARGET_FILTER / TOKEN_QUERY_MISMATCH）
            self._fail(ERROR, str(exc))
            raise
        if resp["snapshot_id"] != self.snapshot_id:
            self._fail(ERROR, "response snapshot drifted from bound snapshot")
            raise BudgetStop(ERROR)
        # 字节口径与 graph_api.GraphAPI.audit 完全一致（真实网络负载，非扫描数）
        req_bytes = len(canonical_request(request))
        page = resp["edges"]
        payload_bytes = len(
            json.dumps(page, sort_keys=True, separators=(",", ":")).encode()
        )
        b.calls_used += 1
        b.edges_returned += len(page)
        b.request_bytes += req_bytes
        b.edge_payload_bytes += payload_bytes
        # 服务端自身也留了一份审计，对账时可交叉验证
        server_audit = self.api.audit[-1]
        self.log.append({
            "seq": len(self.log) + 1,
            "request": dict(request),
            "token_offset": None if token is None else token.get("offset"),
            "returned_edges": len(page),
            "total_matches": resp["total_matches"],
            "has_more": resp["next_token"] is not None,
            "request_bytes": req_bytes,
            "edge_payload_bytes": payload_bytes,
            "server_request_bytes": server_audit["request_bytes"],
            "server_edge_payload_bytes": server_audit["edge_payload_bytes"],
            "snapshot_id": resp["snapshot_id"],
            "edge_ids": [e["edge_id"] for e in page],
        })
        # 响应边数超过剩余边预算：先记账（上面已记），再结束。
        # 不丢弃任何已返回记录，也不谎报未超预算。
        if b.edges_returned > b.max_returned_edges:
            self._fail(
                BUDGET_EXHAUSTED,
                "returned edge records %d exceed budget %d"
                % (b.edges_returned, b.max_returned_edges),
            )
            raise BudgetStop(BUDGET_EXHAUSTED)
        return resp

    def _fail(self, status, reason):
        if self.status == OK:           # 保留首个失败原因
            self.status = status
            self.status_reason = reason

    # ---- 完整请求：取全一个请求的所有页 ---------------------------------

    def fetch_request(self, label, direction, keys, target_keys=None,
                      phase="", note=""):
        """取全“一个完整请求”的所有页；keys 已保证 1..4 个且不重复。

        返回 (edges, status)：
          * status == OK 时 edges 为该请求全部候选边（已跨页去重）；
          * 其余状态表示未能取全，edges 为已实际拿到的边（仅审计用，不参与组路径）。
        同一完整请求重复发起时：真实请求照发、预算照计，结果仍按 edge_id 去重。
        """
        request = {
            "snapshot_id": self.snapshot_id,
            "label": label,
            "direction": direction,
            "keys": list(keys),
        }
        if target_keys is not None:
            request["target_keys"] = list(target_keys)
        cache_key = canonical_request(request).decode()

        token = None
        collected = []
        seen_ids = set()
        start_seq = len(self.log)
        while True:
            resp = self._call(request, token)   # 可能抛 BudgetStop
            for e in resp["edges"]:
                if e["edge_id"] not in seen_ids:
                    seen_ids.add(e["edge_id"])
                    collected.append(e)
            token = resp["next_token"]
            if token is None:
                break  # 只有 next_token is null 才证明该请求取全
        collected.sort(key=lambda e: e["edge_id"])
        self._cache[cache_key] = collected
        for entry in self.log[start_seq:]:  # 精确标注本次请求产生的日志条目
            entry["phase"] = phase
            entry["note"] = note
        return collected, OK

    def fetch_probe_page(self, label, direction, keys, target_keys=None,
                         phase="probe", note=""):
        """探针：只取一个请求的第一页（用于真实候选数 total_matches 比较）。

        第一页不代表取全；若后续决定沿用该请求，必须用 resume_request 接续 token，
        不能把“读到第一页”当作该前沿已取全。
        返回 (first_page_edges, total_matches, pending)；pending 保存续页所需状态。
        """
        request = {
            "snapshot_id": self.snapshot_id,
            "label": label,
            "direction": direction,
            "keys": list(keys),
        }
        if target_keys is not None:
            request["target_keys"] = list(target_keys)
        resp = self._call(request, None)
        self.log[-1]["phase"] = phase
        self.log[-1]["note"] = note
        pending = {
            "request": request,
            "token": resp["next_token"],
            "first_edges": resp["edges"],
            "phase": phase,
            "note": note,
        }
        return resp["edges"], resp["total_matches"], pending

    def resume_request(self, pending):
        """接续探针请求直到 next_token is null，返回该请求的完整去重边列表。"""
        request = pending["request"]
        collected = list(pending["first_edges"])
        seen = {e["edge_id"] for e in collected}
        token = pending["token"]
        start_seq = len(self.log)
        while token is not None:
            resp = self._call(request, token)  # token 与完整请求绑定，原样沿用
            for e in resp["edges"]:
                if e["edge_id"] not in seen:
                    seen.add(e["edge_id"])
                    collected.append(e)
            token = resp["next_token"]
        collected.sort(key=lambda e: e["edge_id"])
        cache_key = canonical_request(request).decode()
        self._cache[cache_key] = collected
        for entry in self.log[start_seq:]:
            entry["phase"] = pending["phase"]
            entry["note"] = pending["note"] + " (resumed page)"
        return collected, OK

    # ---- 前沿批量取数 ---------------------------------------------------

    def expand_frontier(self, label, direction, frontier, phase,
                        target_keys=None):
        """把一个节点前沿（任意大小）按 max_keys 分批，逐批取全所有页。

        target_keys 仅在 direction=='out' 且能力声明支持时允许透传；
        调用方负责可行性判断。返回 (all_edges, status)。
        任何一批预算失败都会立即停止，已完成批次的边进入已取边池，
        未完成批次的边不参与路径组合（完整性由 status 非 OK 体现）。
        """
        all_edges = []
        for batch in self.chunks(sorted(set(frontier)), self.max_keys):
            edges, status = self.fetch_request(
                label, direction, batch,
                target_keys=target_keys, phase=phase,
                note="frontier batch size=%d" % len(batch),
            )
            if status != OK:
                return all_edges, status
            all_edges.extend(edges)
        return all_edges, OK

    def usage(self):
        b = self.budget
        return {
            "status": self.status,
            "status_reason": self.status_reason,
            "calls_used": b.calls_used,
            "max_calls": b.max_calls,
            "edges_returned": b.edges_returned,
            "max_returned_edges": b.max_returned_edges,
            "request_bytes": b.request_bytes,
            "edge_payload_bytes": b.edge_payload_bytes,
            "distinct_edges_cached": len(self.cached_edge_ids()),
        }

    def cached_edge_ids(self):
        ids = set()
        for edges in self._cache.values():
            ids.update(e["edge_id"] for e in edges)
        return ids


class BudgetStop(Exception):
    """预算/协议中断控制流，携带终态。"""

    def __init__(self, status):
        super().__init__(status)
        self.status = status
