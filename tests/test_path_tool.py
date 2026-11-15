"""候选工具测试：基准、协议负例、预算、完整性、oracle 对账。

约束自检：本文件中候选执行只经 GraphAPI 公开接口；只有 OracleTests 直接读私有图，
且通过 path_tool.oracle（独立验收链路）完成。
"""
import copy
import json
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from graph_api import GraphAPI
from path_tool.executor import execute_query
from path_tool.engine import FetchEngine
from path_tool.enumerator import enumerate_paths
from path_tool.explain import explain, audit
from path_tool.oracle import reconcile_result

from fixtures import (
    make_api, edge, dense_page_graph, cycle_graph, parallel_graph,
    multi_endpoint_graph, standard_caps, restore_standard_caps,
    use_synthetic_caps,
)

PRIVATE = str(ROOT / "private" / "edges.json")
QUERIES = json.loads((ROOT / "queries.json").read_text())


class CapRestoringTest(unittest.TestCase):
    def tearDown(self):
        restore_standard_caps()


def q_by_name(name):
    return copy.deepcopy(next(q for q in QUERIES if q["query_id"] == name))


class BenchmarkTests(CapRestoringTest):
    """两个明确可行基准：各自全新 API 实例 + 空缓存，预算内完整且与 oracle 一致。"""

    def setUp(self):
        restore_standard_caps()

    def _run_benchmark(self, name):
        q = q_by_name(name)
        api = GraphAPI(PRIVATE)              # 独立新实例
        result = execute_query(api, q)       # 引擎内部新建空缓存
        ok, checks = audit(result, api=api)
        self.assertTrue(ok, checks)
        self.assertEqual(result["status"], "OK")
        self.assertTrue(result["complete"])
        # 硬预算门槛
        self.assertLessEqual(result["usage"]["calls_used"], 12)
        self.assertLessEqual(result["usage"]["edges_returned"], 40)
        # 独立全图 oracle 逐路径对账
        orecon_ok, report = reconcile_result(result, q, PRIVATE)
        self.assertTrue(orecon_ok, report)
        self.assertEqual(report["oracle_path_count"],
                         report["candidate_path_count"])
        return api, result

    def test_forward_selective(self):
        api, result = self._run_benchmark("forward-selective")
        self.assertEqual(result["path_count"], 2)
        # 材料陷阱：终点无约束反查 located 是 301 条；计划不应走那条路
        for e in result["request_log"]:
            if e["request"]["label"] == "located" and \
                    e["request"]["direction"] == "in":
                self.fail("forward-selective must not do unconstrained reverse "
                          "located scan (301 candidates)")
        # 真实记录的请求/边数量
        self.assertEqual(result["usage"]["calls_used"], 3)
        self.assertEqual(result["usage"]["edges_returned"], 4)

    def test_reverse_selective(self):
        api, result = self._run_benchmark("reverse-selective")
        self.assertEqual(result["path_count"], 1)
        # 起点全取首层 owns 是 200 条；探针第一页后必须换边且不续页
        self.assertTrue(result["decision"]["switch"])
        abandoned = [e for e in result["request_log"]
                     if e.get("abandoned")]
        self.assertEqual(len(abandoned), 1)
        self.assertEqual(abandoned[0]["returned_edges"], 25)  # 只付第一页
        self.assertTrue(all(not e["has_more"] for e in result["request_log"]
                            if not e.get("abandoned")))

    def test_benchmarks_use_fresh_instances_and_caches(self):
        # 连续执行两个基准，第二个结果不得依赖第一个的任何缓存
        r1 = execute_query(GraphAPI(PRIVATE), q_by_name("forward-selective"))
        r2 = execute_query(GraphAPI(PRIVATE), q_by_name("reverse-selective"))
        self.assertTrue(r1["complete"] and r2["complete"])
        # 两实例各自独立的审计轨迹长度等于各自日志长度
        self.assertEqual(len(r1["request_log"]), r1["usage"]["calls_used"])
        self.assertEqual(len(r2["request_log"]), r2["usage"]["calls_used"])

    def test_cycle_rejected(self):
        q = q_by_name("cycle-rejected-path")
        result = execute_query(GraphAPI(PRIVATE), q)
        self.assertEqual(result["status"], "OK")
        self.assertEqual(result["path_count"], 0)
        ok, report = reconcile_result(result, q, PRIVATE)
        self.assertTrue(ok)


class MultiPageTests(CapRestoringTest):
    def setUp(self):
        use_synthetic_caps()

    def test_pagination_returns_all_candidates(self):
        rows = dense_page_graph(page_edges=30)   # 30 + 1 平行边 = 31 > 25
        api, private = make_api(rows)
        q = {"query_id": "mp", "starts": ["s"], "ends": ["z"],
             "labels": ["L", "T"],
             "budget": {"max_calls": 20, "max_returned_edges": 100}}
        result = execute_query(api, q)
        self.assertEqual(result["status"], "OK", result["status_reason"])
        # 31 条 L 边必须经两页取全（25 + 6）
        log_l = [e for e in result["request_log"]
                 if e["request"]["label"] == "L"]
        self.assertEqual(sum(e["returned_edges"] for e in log_l), 31)
        self.assertEqual([e["token_offset"] for e in log_l],
                         [None, 25])
        # 两条 s->m0 平行边各自产生路径，按 (cost, edge_ids) 排序
        ids = [tuple(p["edge_ids"]) for p in result["paths"]]
        self.assertIn(("L-e000", "T-t"), ids)
        self.assertIn(("L-parallel", "T-t"), ids)
        costs = {tuple(p["edge_ids"]): p["cost"] for p in result["paths"]}
        self.assertEqual(costs[("L-e000", "T-t")], -5 + 1)
        self.assertEqual(costs[("L-parallel", "T-t")], 101)
        # oracle 对账
        ok, report = reconcile_result(result, q, private)
        self.assertTrue(ok, report)

    def test_single_page_does_not_prove_completeness(self):
        # 手工引擎：只读第一页时，has_more=True 必须阻止“取全”判断
        rows = dense_page_graph(page_edges=30)
        api, _ = make_api(rows)
        eng = FetchEngine(api)
        eng.bind_budget(20, 200)
        _, total, pending = eng.fetch_probe_page("L", "out", ["s"])
        self.assertEqual(total, 31)
        self.assertIsNotNone(pending["token"])
        edges, _ = eng.resume_request(pending)
        self.assertEqual(len(edges), 31)


class CapabilityTests(CapRestoringTest):
    def setUp(self):
        restore_standard_caps()

    def test_unsupported_direction_via_api(self):
        import graph_api
        graph_api.CAPABILITIES.clear()
        graph_api.CAPABILITIES.update({
            "L1": {"directions": ["out"], "target_filter": False},
            "L2": {"directions": ["out", "in"], "target_filter": False},
        })
        api, _ = make_api(cycle_graph())
        # 直接打 API：in 不支持
        req = {"snapshot_id": api.capabilities()["snapshot_id"],
               "label": "L1", "direction": "in", "keys": ["a"]}
        with self.assertRaises(ValueError) as ctx:
            api.query_edges(req)
        self.assertEqual(str(ctx.exception), "UNSUPPORTED_DIRECTION")

    def test_planner_marks_infeasible_when_no_split(self):
        import graph_api
        # L1 仅 out，L2 仅 in，L3 仅 out：
        # 任何切分点的前缀都需全 out、后缀都需全 in，无 s 可同时满足
        graph_api.CAPABILITIES.clear()
        graph_api.CAPABILITIES.update({
            "L1": {"directions": ["out"], "target_filter": False},
            "L2": {"directions": ["in"], "target_filter": False},
            "L3": {"directions": ["out"], "target_filter": False},
        })
        api, _ = make_api(cycle_graph())
        q = {"query_id": "x", "starts": ["s"], "ends": ["z"],
             "labels": ["L1", "L2", "L3"],
             "budget": {"max_calls": 10, "max_returned_edges": 50}}
        result = execute_query(api, q)
        self.assertEqual(result["status"], "ERROR")
        self.assertFalse(result["complete"])
        self.assertEqual(result["paths"], [])

    def test_target_filter_rejected_when_not_capable(self):
        import graph_api
        graph_api.CAPABILITIES.clear()
        graph_api.CAPABILITIES.update({
            "L1": {"directions": ["out", "in"], "target_filter": False},
        })
        api, _ = make_api([edge("e", "s", "a", "L1")])
        snap = api.capabilities()["snapshot_id"]
        with self.assertRaises(ValueError) as ctx:
            api.query_edges({
                "snapshot_id": snap, "label": "L1", "direction": "out",
                "keys": ["s"], "target_keys": ["a"]})
        self.assertEqual(str(ctx.exception), "UNSUPPORTED_TARGET_FILTER")

    def test_target_filter_never_used_on_in_direction(self):
        use_synthetic_caps()
        rows = multi_endpoint_graph()
        api, _ = make_api(rows)
        q = {"query_id": "me", "starts": ["s"],
             "ends": ["z0", "z1", "z2", "z3"], "labels": ["L1", "L2"],
             "budget": {"max_calls": 20, "max_returned_edges": 100}}
        result = execute_query(api, q)
        self.assertEqual(result["status"], "OK")
        for e in result["request_log"]:
            if e["request"]["direction"] == "in":
                self.assertNotIn("target_keys", e["request"])


class TokenTests(CapRestoringTest):
    def setUp(self):
        use_synthetic_caps()

    def test_token_reuse_after_request_change_rejected(self):
        rows = dense_page_graph(page_edges=30)
        api, _ = make_api(rows)
        snap = api.capabilities()["snapshot_id"]
        req1 = {"snapshot_id": snap, "label": "L", "direction": "out",
                "keys": ["s"]}
        resp = api.query_edges(req1)
        self.assertIsNotNone(resp["next_token"])
        # 更换 keys 后沿用旧 token -> 服务端拒绝
        req2 = dict(req1, keys=["s", "x"])
        with self.assertRaises(ValueError) as ctx:
            api.query_edges(req2, resp["next_token"])
        self.assertEqual(str(ctx.exception), "TOKEN_QUERY_MISMATCH")

    def test_token_bound_to_snapshot(self):
        rows = dense_page_graph(page_edges=30)
        api, _ = make_api(rows)
        snap = api.capabilities()["snapshot_id"]
        resp = api.query_edges(
            {"snapshot_id": snap, "label": "L", "direction": "out",
             "keys": ["s"]})
        self.assertIsNotNone(resp["next_token"])
        bad_token = dict(resp["next_token"], snapshot_id="deadbeef")
        with self.assertRaises(ValueError) as ctx:
            api.query_edges(
                {"snapshot_id": snap, "label": "L", "direction": "out",
                 "keys": ["s"]}, bad_token)
        self.assertEqual(str(ctx.exception), "TOKEN_QUERY_MISMATCH")

    def test_snapshot_mismatch_request_rejected(self):
        rows = cycle_graph()
        api, _ = make_api(rows)
        with self.assertRaises(ValueError) as ctx:
            api.query_edges(
                {"snapshot_id": "wrong", "label": "L1", "direction": "out",
                 "keys": ["s"]})
        self.assertEqual(str(ctx.exception), "SNAPSHOT_MISMATCH")


class BudgetTests(CapRestoringTest):
    def setUp(self):
        use_synthetic_caps()

    def test_empty_budget_incomplete(self):
        rows = cycle_graph()
        api, _ = make_api(rows)
        q = {"query_id": "eb", "starts": ["s"], "ends": ["z"],
             "labels": ["L1", "L2", "L3"],
             "budget": {"max_calls": 0, "max_returned_edges": 10}}
        result = execute_query(api, q)
        self.assertEqual(result["status"], "INCOMPLETE")
        self.assertFalse(result["complete"])
        self.assertEqual(result["paths"], [])
        self.assertEqual(result["usage"]["calls_used"], 0)

    def test_zero_edge_budget_incomplete(self):
        rows = cycle_graph()
        api, _ = make_api(rows)
        q = {"query_id": "zeb", "starts": ["s"], "ends": ["z"],
             "labels": ["L1", "L2", "L3"],
             "budget": {"max_calls": 10, "max_returned_edges": 0}}
        result = execute_query(api, q)
        # 第一页实际返回边使累计超过 0 -> BUDGET_EXHAUSTED，记录保留
        self.assertEqual(result["status"], "BUDGET_EXHAUSTED")
        self.assertGreaterEqual(result["usage"]["edges_returned"], 1)
        self.assertFalse(result["complete"])

    def test_call_budget_runs_out_mid_pagination(self):
        rows = dense_page_graph(page_edges=30)
        api, _ = make_api(rows)
        q = {"query_id": "cb", "starts": ["s"], "ends": ["z"],
             "labels": ["L", "T"],
             "budget": {"max_calls": 1, "max_returned_edges": 200}}
        result = execute_query(api, q)
        self.assertEqual(result["status"], "INCOMPLETE")
        self.assertFalse(result["complete"])
        # 第一页已取但未取全，不得发布路径
        self.assertEqual(result["path_count"], 0)
        self.assertTrue(len(result["partial_paths"]) >= 0)

    def test_edge_budget_overshoot_records_actual_consumption(self):
        rows = dense_page_graph(page_edges=30)
        api, _ = make_api(rows)
        q = {"query_id": "eo", "starts": ["s"], "ends": ["z"],
             "labels": ["L", "T"],
             "budget": {"max_calls": 20, "max_returned_edges": 10}}
        result = execute_query(api, q)
        self.assertEqual(result["status"], "BUDGET_EXHAUSTED")
        # 第一页 25 条是真实消耗，必须如实记录而不是谎报 10
        self.assertEqual(result["usage"]["edges_returned"], 25)
        self.assertFalse(result["complete"])

    def test_budget_exhausted_does_not_claim_complete_on_early_path(self):
        # 即使第一页里已含可组成低 cost 路径的边，也不能宣布完整
        rows = dense_page_graph(page_edges=30)
        api, _ = make_api(rows)
        q = {"query_id": "early", "starts": ["s"], "ends": ["z"],
             "labels": ["L", "T"],
             "budget": {"max_calls": 20, "max_returned_edges": 10}}
        result = execute_query(api, q)
        self.assertFalse(result["complete"])
        self.assertEqual(result["paths"], [])


class SnapshotMutationTests(CapRestoringTest):
    def setUp(self):
        use_synthetic_caps()

    def test_in_place_snapshot_id_tampering_rejected(self):
        rows = cycle_graph()
        api, private = make_api(rows)
        q = {"query_id": "mut", "starts": ["s"], "ends": ["z"],
             "labels": ["L1", "L2", "L3"],
             "budget": {"max_calls": 20, "max_returned_edges": 100}}
        result = execute_query(api, q)
        bound = result["snapshot_id"]
        # 原地修改请求里的 snapshot_id：引擎每个请求都用绑定值重建，
        # 手工构造一个被篡改的请求应被服务端拒绝
        bad = {"snapshot_id": bound[:-1] + ("0" if bound[-1] != "0" else "1"),
               "label": "L1", "direction": "out", "keys": ["s"]}
        with self.assertRaises(ValueError) as ctx:
            api.query_edges(bad)
        self.assertEqual(str(ctx.exception), "SNAPSHOT_MISMATCH")

    def test_engine_rebinds_snapshot_per_instance(self):
        rows1 = cycle_graph()
        api1, p1 = make_api(rows1)
        api2, p2 = make_api(rows1 + [edge("extra", "s", "z", "L3")])
        self.assertNotEqual(
            FetchEngine(api1).snapshot_id, FetchEngine(api2).snapshot_id)


class PathSemanticsTests(CapRestoringTest):
    def setUp(self):
        use_synthetic_caps()

    def test_parallel_edges_produce_distinct_paths(self):
        api, private = make_api(parallel_graph())
        q = {"query_id": "par", "starts": ["s"], "ends": ["z"],
             "labels": ["L1", "L2"],
             "budget": {"max_calls": 20, "max_returned_edges": 100}}
        result = execute_query(api, q)
        self.assertEqual(result["path_count"], 4)  # 2 x 2 平行边组合
        ids = [tuple(p["edge_ids"]) for p in result["paths"]]
        self.assertEqual(len(set(ids)), 4)
        # 负 cost 合法且参与排序
        self.assertEqual(result["paths"][0]["cost"], 1 - 4)
        self.assertEqual(result["paths"][0]["edge_ids"], ["p1", "p3"])
        ok, report = reconcile_result(result, q, private)
        self.assertTrue(ok, report)

    def test_cycle_node_paths_rejected(self):
        api, private = make_api(cycle_graph())
        q = {"query_id": "cyc", "starts": ["s"], "ends": ["z"],
             "labels": ["L1", "L2", "L3"],
             "budget": {"max_calls": 20, "max_returned_edges": 100}}
        result = execute_query(api, q)
        # 唯一合法简单路径 s->b->z（c3,c4,c5）；s->a->s 因第二跳重复节点被拒，
        # 不会产生任何包含 c1,c2 的路径
        self.assertEqual(result["path_count"], 1)
        self.assertEqual(result["paths"][0]["edge_ids"], ["c3", "c4", "c5"])
        all_result_edges = {eid for p in result["paths"]
                            for eid in p["edge_ids"]}
        self.assertNotIn("c1", all_result_edges)
        self.assertNotIn("c2", all_result_edges)
        ok, report = reconcile_result(result, q, private)
        self.assertTrue(ok, report)

    def test_no_result_query(self):
        api, _ = make_api(cycle_graph())
        q = {"query_id": "none", "starts": ["ghost"], "ends": ["z"],
             "labels": ["L1", "L2", "L3"],
             "budget": {"max_calls": 20, "max_returned_edges": 100}}
        result = execute_query(api, q)
        self.assertEqual(result["status"], "OK")
        self.assertEqual(result["path_count"], 0)
        self.assertTrue(result["complete"])

    def test_multiple_starts_and_ends(self):
        api, private = make_api(multi_endpoint_graph())
        q = {"query_id": "multi", "starts": ["s"],
             "ends": ["z0", "z3"], "labels": ["L1", "L2"],
             "budget": {"max_calls": 20, "max_returned_edges": 100}}
        result = execute_query(api, q)
        self.assertEqual(result["path_count"], 2)
        ok, report = reconcile_result(result, q, private)
        self.assertTrue(ok, report)

    def test_duplicate_edge_via_multiple_requests_not_cloned(self):
        # 同一 edge 经两个不同 keys 批次各取一次：路径仍只有一条
        rows = [edge("e1", "k1", "a", "L1"), edge("e2", "k2", "a", "L1"),
                edge("e3", "a", "z", "L2")]
        api, _ = make_api(rows)
        eng = FetchEngine(api)
        eng.bind_budget(20, 100)
        e_a, _ = eng.fetch_request("L1", "out", ["k1"], phase="t")
        e_b, _ = eng.fetch_request("L1", "out", ["k1", "k2"], phase="t")
        pool = {}
        for e in e_a + e_b:
            pool[e["edge_id"]] = e
        paths, _ = enumerate_paths(list(pool.values()),
                                   ["k1", "k2"], ["z"], ["L1", "L2"])
        # 补取 L2
        e_c, _ = eng.fetch_request("L2", "out", ["a"], phase="t")
        for e in e_c:
            pool[e["edge_id"]] = e
        paths, _ = enumerate_paths(list(pool.values()),
                                   ["k1", "k2"], ["z"], ["L1", "L2"])
        self.assertEqual(len(paths), 2)  # k1->a->z, k2->a->z，无重复
        # 重复请求照计预算
        self.assertEqual(eng.budget.calls_used, 3)
        self.assertEqual(eng.budget.edges_returned, 1 + 2 + 1)


class AlternativeStrategyTests(CapRestoringTest):
    """替代方案的真实请求/候选边差异（非估计值）。"""

    def setUp(self):
        restore_standard_caps()

    def test_pure_forward_vs_adaptive_reverse_selective(self):
        q = q_by_name("reverse-selective")
        # 纯前向：起点 owns 全取 200 边，需要多页，真实消耗远超自适应
        r_forward = execute_query(GraphAPI(PRIVATE), q, "pure-forward")
        r_adaptive = execute_query(GraphAPI(PRIVATE), q, "adaptive")
        self.assertTrue(r_adaptive["complete"])
        # 纯前向在 40 边预算内必然爆预算（首层就 200）
        self.assertEqual(r_forward["status"], "BUDGET_EXHAUSTED")
        self.assertGreater(r_forward["usage"]["edges_returned"],
                           r_adaptive["usage"]["edges_returned"])
        # 真实数字而非估计：纯前向至少把第一页 25 条全部取回
        self.assertGreaterEqual(r_forward["usage"]["edges_returned"], 25)
        self.assertLessEqual(r_adaptive["usage"]["edges_returned"], 40)

    def test_meet_split_vs_adaptive_forward_selective(self):
        # owns 仅支持 out，纯后向不可行（能力检查直接拒绝）；
        # 真正可行的替代是 split=1 相遇方案：后向 located/in [zF] 真实拉回 301 候选
        q = q_by_name("forward-selective")
        r_meet = execute_query(GraphAPI(PRIVATE), q, force_split=1)
        r_adaptive = execute_query(GraphAPI(PRIVATE), q, "adaptive")
        self.assertTrue(r_adaptive["complete"])
        self.assertEqual(r_meet["status"], "BUDGET_EXHAUSTED")
        # 相遇方案的 located/in 请求真实候选数 301（来自服务端 total_matches）
        located_reqs = [e for e in r_meet["request_log"]
                        if e["request"]["label"] == "located"
                        and e["request"]["direction"] == "in"]
        self.assertTrue(located_reqs)
        self.assertEqual(located_reqs[0]["total_matches"], 301)
        # 真实请求/边数量对比，不是估计值：相遇方案边数远超自适应
        # （调用数可能因提前爆预算而更少，但返回边记录必然更多）
        self.assertGreater(r_meet["usage"]["edges_returned"],
                           r_adaptive["usage"]["edges_returned"])
        self.assertGreaterEqual(r_meet["usage"]["calls_used"], 1)

    def test_pure_backward_rejected_when_capability_missing(self):
        q = q_by_name("forward-selective")  # owns 仅 out
        r_backward = execute_query(GraphAPI(PRIVATE), q, "pure-backward")
        self.assertEqual(r_backward["status"], "ERROR")
        self.assertFalse(r_backward["complete"])


class GenericShapeTests(CapRestoringTest):
    """通用输入形状：4 跳、>4 前沿分批、target 超 4 个降级、多起点。"""

    def setUp(self):
        use_synthetic_caps()

    def test_four_hop_path(self):
        # 最长 4 跳标签序列，含一条合法路径与一条断链
        rows = [
            edge("h1", "s", "n1", "L1"), edge("h2", "n1", "n2", "L2"),
            edge("h3", "n2", "n3", "L3"), edge("h4", "n3", "z", "T"),
            edge("d1", "s", "x1", "L1"),  # x1 无 L2 出边，断链
        ]
        api, private = make_api(rows)
        q = {"query_id": "4hop", "starts": ["s"], "ends": ["z"],
             "labels": ["L1", "L2", "L3", "T"],
             "budget": {"max_calls": 30, "max_returned_edges": 100}}
        result = execute_query(api, q)
        self.assertEqual(result["status"], "OK", result["status_reason"])
        self.assertEqual(result["path_count"], 1)
        self.assertEqual(result["paths"][0]["edge_ids"],
                         ["h1", "h2", "h3", "h4"])
        ok, report = reconcile_result(result, q, private)
        self.assertTrue(ok, report)

    def test_frontier_larger_than_four_batches_keys(self):
        # 第二跳前沿 6 个节点 > max_keys=4，必须拆成多个请求且结果不重不漏
        rows = []
        for i in range(6):
            rows.append(edge("a-%d" % i, "s", "m%d" % i, "L1"))
            rows.append(edge("b-%d" % i, "m%d" % i, "z", "L2"))
        api, private = make_api(rows)
        q = {"query_id": "batch", "starts": ["s"], "ends": ["z"],
             "labels": ["L1", "L2"],
             "budget": {"max_calls": 30, "max_returned_edges": 100}}
        result = execute_query(api, q)
        self.assertEqual(result["status"], "OK")
        self.assertEqual(result["path_count"], 6)
        # 第二跳 L2 请求至少两个批次（6 键按 4 拆分）
        l2_reqs = [e for e in result["request_log"]
                   if e["request"]["label"] == "L2"]
        key_batches = [tuple(e["request"]["keys"]) for e in l2_reqs]
        self.assertGreaterEqual(len(key_batches), 2)
        for keys in key_batches:
            self.assertLessEqual(len(keys), 4)
        ok, report = reconcile_result(result, q, private)
        self.assertTrue(ok, report)

    def test_target_filter_downgrade_when_more_than_four_targets(self):
        # 相遇中间节点集合 >4 时不能发非法 target_keys（服务端上限 4），
        # 必须降级为不带 target_keys 的请求，仍要给出完整答案。
        # ends 仍遵守 ≤4；通过让 6 个中间节点汇聚到 4 个终点构造 >4 相遇集。
        rows = []
        for i in range(6):
            rows.append(edge("a-%d" % i, "s", "m%d" % i, "L1"))
            rows.append(edge("b-%d" % i, "m%d" % i, "z%d" % (i % 4), "L2"))
        api, private = make_api(rows)
        q = {"query_id": "tg", "starts": ["s"],
             "ends": ["z0", "z1", "z2", "z3"],
             "labels": ["L1", "L2"],
             "budget": {"max_calls": 30, "max_returned_edges": 200}}
        result = execute_query(api, q)
        self.assertEqual(result["status"], "OK", result["status_reason"])
        for e in result["request_log"]:
            t = e["request"].get("target_keys")
            if t is not None:
                self.assertLessEqual(len(t), 4)
        self.assertEqual(result["path_count"], 6)
        ok, report = reconcile_result(result, q, private)
        self.assertTrue(ok, report)

    def test_multiple_starts_four_keys_single_request(self):
        rows = []
        for i in range(4):
            rows.append(edge("ms-%d" % i, "s%d" % i, "a", "L1"))
        rows.append(edge("ms-z", "a", "z", "L2"))
        api, private = make_api(rows)
        q = {"query_id": "ms", "starts": ["s0", "s1", "s2", "s3"],
             "ends": ["z"], "labels": ["L1", "L2"],
             "budget": {"max_calls": 30, "max_returned_edges": 100}}
        result = execute_query(api, q)
        self.assertEqual(result["path_count"], 4)
        first = result["request_log"][0]["request"]
        self.assertEqual(len(first["keys"]), 4)  # 4 起点单请求
        ok, report = reconcile_result(result, q, private)
        self.assertTrue(ok, report)

    def test_negative_cost_sorting(self):
        rows = [
            edge("n1", "s", "a", "L1", 10),
            edge("n2", "s", "b", "L1", -10),
            edge("n3", "a", "z", "L2", 0),
            edge("n4", "b", "z", "L2", -3),
        ]
        api, private = make_api(rows)
        q = {"query_id": "neg", "starts": ["s"], "ends": ["z"],
             "labels": ["L1", "L2"],
             "budget": {"max_calls": 20, "max_returned_edges": 100}}
        result = execute_query(api, q)
        costs = [p["cost"] for p in result["paths"]]
        self.assertEqual(costs, sorted(costs))      # 负 cost 合法且决定排序
        self.assertEqual(costs[0], -13)
        ok, report = reconcile_result(result, q, private)
        self.assertTrue(ok, report)

    def test_wide_frontier_scale_matches_oracle(self):
        # 300 个中间节点：多批 keys + 多页，预算充足时必须完整且与 oracle 一致
        rows = []
        for i in range(300):
            rows.append(edge("a%03d" % i, "s", "m%d" % i, "L1", i % 5 - 2))
            rows.append(edge("b%03d" % i, "m%d" % i, "z", "L2"))
        api, private = make_api(rows)
        q = {"query_id": "wide", "starts": ["s"], "ends": ["z"],
             "labels": ["L1", "L2"],
             "budget": {"max_calls": 200, "max_returned_edges": 5000}}
        result = execute_query(api, q)
        self.assertEqual(result["status"], "OK")
        self.assertEqual(result["path_count"], 300)
        ok, report = reconcile_result(result, q, private)
        self.assertTrue(ok, report)
        # 预算不足时不得伪装完整
        api2, _ = make_api(rows)
        q2 = dict(q, budget={"max_calls": 40, "max_returned_edges": 2000})
        r2 = execute_query(api2, q2)
        self.assertIn(r2["status"], ("INCOMPLETE", "BUDGET_EXHAUSTED"))
        self.assertFalse(r2["complete"])
        self.assertEqual(r2["paths"], [])


class ExplainAuditTests(CapRestoringTest):
    def setUp(self):
        restore_standard_caps()

    def test_explain_contains_request_log_and_provenance(self):
        q = q_by_name("forward-selective")
        api = GraphAPI(PRIVATE)
        result = execute_query(api, q)
        text = explain(result)
        self.assertIn("snapshot", text)
        self.assertIn("f-own-1", text)
        self.assertIn("request_bytes", text)
        ok, checks = audit(result, api=api)
        self.assertTrue(ok)
        # 每条路径的边都能回查
        prov = result["provenance"]
        self.assertEqual(len(prov), result["path_count"])
        for key, p in prov.items():
            for ed in p["edges"]:
                self.assertTrue(ed["fetched_in_log_seq"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
