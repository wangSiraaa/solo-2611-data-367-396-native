#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""test_dedup.py — 逐版本与独立成对参考比对；含材料场景与边界用例。"""
import json
import os
import random
import tempfile
import unittest

import probe  # 材料自带的小规模成对 oracle（阈值 1/4 硬编码）
import reference
from dedupstore import DedupStore, UnknownDocError, StoreError, encode_shingle

HERE = os.path.dirname(os.path.abspath(__file__))


def load_materials():
    docs = json.load(open(os.path.join(HERE, "documents.json"), encoding="utf-8"))
    updates = json.load(open(os.path.join(HERE, "updates.json"), encoding="utf-8"))
    profile = json.load(open(os.path.join(HERE, "profile.json"), encoding="utf-8"))
    return docs, updates, profile


class Base(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, "store.sqlite3")

    def tearDown(self):
        self.tmp.cleanup()

    def mk(self, threshold="1/2", docs=None):
        return DedupStore.build(self.path, threshold=threshold, docs=docs)

    def assertState(self, store, docs, p, q):
        """当前发布状态与独立成对参考逐项一致：边、连通分量、各分量代表。"""
        ref = reference.reference(docs, p, q)
        st = store.state()
        self.assertEqual(st["edges"], ref["edges"], "相似边集不一致")
        self.assertEqual(st["components"], ref["components"], "连通分量不一致")
        self.assertEqual(st["representatives"], ref["representatives"], "代表不一致")
        return st


class TestMaterials(Base):
    """材料场景：桥点 b、重复文档 a/e、独立簇 d；阈值取 profile.json 的 1/4。"""

    def test_materials_against_probe(self):
        docs_list, updates, profile = load_materials()
        p, q = profile["jaccard_threshold"]["numerator"], profile["jaccard_threshold"]["denominator"]
        self.assertEqual((p, q), (1, 4))
        docs = {d["id"]: (d["tokens"], d["quality"]) for d in docs_list}
        store = self.mk(f"{p}/{q}")
        # build：逐篇 put，每篇发布后都与 probe.py 的成对 oracle 比对
        running = {}
        for d in docs_list:
            store.put(d["id"], d["tokens"], d["quality"])
            running[d["id"]] = (d["tokens"], d["quality"])
            probe_edges = {tuple(e) for e in probe.exact_edges(
                [{"id": i, "tokens": t[0]} for i, t in running.items()])}
            self.assertEqual(store.state()["edges"],
                             {tuple(sorted(e)) for e in probe_edges})
            self.assertState(store, running, p, q)
        # 材料校验：桥点 b 连接 c 与 {a,e}，d 独立成簇
        st = store.state()
        self.assertIn(frozenset({"a", "b", "c", "e"}), st["components"])
        self.assertIn(frozenset({"d"}), st["components"])
        self.assertEqual(st["representatives"][frozenset({"a", "b", "c", "e"})], "b")
        # 应用 updates.json，逐步比对
        for up in updates:
            if up["op"] == "remove":
                res = store.remove(up["id"])
                del running[up["id"]]
            else:
                doc = up["document"]
                res = store.put(doc["id"], doc["tokens"], doc["quality"])
                running[doc["id"]] = (doc["tokens"], doc["quality"])
            self.assertTrue(res.changed)
            self.assertState(store, running, p, q)
        # 桥点 b 删除后原簇必须拆分：{a,e} 与 {c}，而不是留下错误大簇
        st = store.state()
        self.assertEqual(st["components"],
                         {frozenset({"a", "e"}), frozenset({"c"}), frozenset({"d", "f"})})
        self.assertEqual(st["representatives"][frozenset({"a", "e"})], "a")   # 7 > 6
        self.assertEqual(st["representatives"][frozenset({"d", "f"})], "f")   # 10 > 5
        # 版本数 = 5 次 build put + 2 次 update
        self.assertEqual(store.version, 7)
        self.assertEqual(len(store.versions()), 7)


class TestReferenceCrossCheck(unittest.TestCase):
    """参数化参考与材料 probe.py（t=1/4）在随机输入上交叉验证。"""

    def test_reference_matches_probe(self):
        rng = random.Random(99)
        vocab = [f"w{i}" for i in range(12)]
        for _ in range(30):
            docs = {}
            for j in range(rng.randrange(2, 15)):
                docs[f"d{j}"] = ([rng.choice(vocab) for _ in range(rng.randrange(0, 8))],
                                 rng.randrange(0, 5))
            ref = reference.reference(docs, 1, 4)
            probe_edges = {tuple(sorted(e)) for e in probe.exact_edges(
                [{"id": i, "tokens": t[0]} for i, t in docs.items()])}
            self.assertEqual(ref["edges"], probe_edges)


class TestEmptyAndShort(Base):
    def test_empty_sets_cluster_together(self):
        store = self.mk("1/4")
        store.put("e0", [], 1)                 # 空 token → 空 shingle 集
        store.put("e1", ["solo"], 1)           # 单 token → 空 shingle 集
        store.put("e2", ["other"], 5)          # 同上：两空集相似
        store.put("n1", ["a", "b"], 1)         # 非空：与空集不相似
        st = store.state()
        self.assertIn(frozenset({"e0", "e1", "e2"}), st["components"])
        self.assertIn(frozenset({"n1"}), st["components"])
        # 空桶是团：3 条边，intersection=union=0
        self.assertEqual(len(st["edges"]), 3)
        ex = store.explain("e0")
        self.assertEqual(ex["probe"]["candidate_source"], "empty-bucket")
        self.assertTrue(all(n["candidate_source"]["via"] == "empty-bucket"
                            for n in ex["neighbors"]))
        self.assertTrue(all(n["union"] == 0 for n in ex["neighbors"]))
        self.assertEqual(ex["representative"], "e2")  # quality 5 最高
        # 删除一个空文档，剩余仍成团
        store.remove("e2")
        self.assertEqual(store.state()["components"],
                         {frozenset({"e0", "e1"}), frozenset({"n1"})})

    def test_one_empty_one_nonempty_not_similar(self):
        store = self.mk("1/5")
        store.put("empty", [], 1)
        store.put("full", ["a", "b"], 1)
        self.assertEqual(store.state()["edges"], set())


class TestThresholdBoundary(Base):
    def test_exact_equality_counts_as_similar(self):
        # t=1/5：|∩|=1, |∪|=5 → 1/5 恰等于阈值 → 有边；1/6 则没有
        store = self.mk("1/5")
        store.put("A", ["x", "y"], 1)                            # {xy}
        store.put("B", ["x", "y", "a", "b", "c", "d"], 1)        # 5 shingles, ∩=1, ∪=5
        store.put("C", ["x", "y", "a", "b", "c", "d", "e"], 1)   # 6 shingles, J(A,C)=1/6
        st = store.state()
        self.assertIn(tuple(sorted(("A", "B"))), st["edges"])
        self.assertNotIn(tuple(sorted(("A", "C"))), st["edges"])
        self.assertIn(tuple(sorted(("B", "C"))), st["edges"])    # J=5/6
        self.assertEqual(st["components"], {frozenset({"A", "B", "C"})})

    def test_half_and_one(self):
        store = self.mk("1/2")
        store.put("A", ["a", "b"], 1)          # {ab}
        store.put("B", ["a", "b", "c"], 1)     # {ab,bc}，J=1/2 恰等 → 边
        store.put("C", ["a", "b", "c", "d"], 1)  # {ab,bc,cd}，J(A,C)=1/3 < 1/2
        st = store.state()
        self.assertIn(tuple(sorted(("A", "B"))), st["edges"])
        self.assertNotIn(tuple(sorted(("A", "C"))), st["edges"])
        # t=1：仅完全相同的集合相似
        store1 = DedupStore.build(os.path.join(self.tmp.name, "s1.sqlite3"), threshold="1")
        store1.put("u", ["a", "b", "c"], 1)
        store1.put("v", ["a", "b", "c"], 1)
        store1.put("w", ["a", "b", "x"], 1)
        self.assertEqual(store1.state()["edges"], {("u", "v")})

    def test_threshold_validation(self):
        for bad in ("1/6", "0/1", "2/1", "-1/2"):
            with self.assertRaises(ValueError):
                DedupStore.build(os.path.join(self.tmp.name, f"bad{bad.replace('/', '_')}.sqlite3"),
                                 threshold=bad)
        DedupStore.build(os.path.join(self.tmp.name, "ok.sqlite3"), threshold="1/5")
        DedupStore.build(os.path.join(self.tmp.name, "ok2.sqlite3"), threshold=(2, 4))

    def test_size_filter_prunes_without_exact_compare(self):
        # t=1/2：大小为 2 与 10 的集合 J≤2/10=1/5<1/2，大小过滤直接排除，不做精确比较
        store = self.mk("1/2")
        store.put("S", ["a", "b", "c"], 1)
        store.put("L", ["a", "b", "c"] + [f"u{i}" for i in range(8)], 1)
        st = store.stats()
        self.assertEqual(st["exact_comparisons"], 0)
        self.assertEqual(store.state()["edges"], set())


class TestHotToken(Base):
    def test_hot_shingle_streams_and_exact_filter_decides(self):
        # 25 篇共享一个热门 shingle，其余 shingle 各自唯一 → 候选很多但无边
        store = self.mk("1/5")
        n = 25
        for i in range(n):
            store.put(f"h{i:02d}", ["hot", "topic", f"u{i}a", f"u{i}b", f"u{i}c", f"u{i}d"], 1)
        st = store.state()
        self.assertEqual(st["edges"], set())
        self.assertEqual(len(st["components"]), n)
        stats = store.stats()
        # 第 i 篇与此前 i 篇各做一次精确比较：0+1+...+(n-1)
        self.assertEqual(stats["exact_comparisons"], n * (n - 1) // 2)
        self.assertEqual(stats["candidates_unique"], n * (n - 1) // 2)
        self.assertGreaterEqual(stats["candidate_visits"], stats["candidates_unique"])


class TestQualityTies(Base):
    def test_tie_breaks_by_utf8_byte_order(self):
        store = self.mk("1/4")
        # 全部空 shingle 集 → 同簇；quality 全并列 → 字节序最小 id 为代表
        for did in ["b", "a", "aa", "z", "é", "中"]:
            store.put(did, [], 7)
        self.assertEqual(store.state()["representatives"][frozenset({"b", "a", "aa", "z", "é", "中"})], "a")
        store.remove("a")
        self.assertEqual(store.state()["representatives"][frozenset({"b", "aa", "z", "é", "中"})], "aa")
        # 仅改 quality（相同 tokens 修订）→ 代表易主，边不变
        res = store.put("z", [], 9)
        self.assertTrue(res.changed)
        self.assertEqual(res.payload["revision"], "quality_only")
        self.assertEqual(store.state()["representatives"][frozenset({"b", "aa", "z", "é", "中"})], "z")
        self.assertEqual(len(store.state()["edges"]), 10)  # 5 篇空文档的团


class TestBridgeSplitMerge(Base):
    def test_bridge_remove_and_revise(self):
        store = self.mk("1/2")
        store.put("A", ["a", "b"], 1)        # {ab}
        store.put("B", ["a", "b", "c"], 5)   # {ab,bc}：桥
        store.put("C", ["b", "c"], 1)        # {bc}
        self.assertEqual(store.state()["components"], {frozenset({"A", "B", "C"})})
        # 删除桥点 → 原簇重新判定为两个单例
        res = store.remove("B")
        kinds = sorted(e["kind"] for e in res.payload["component_events"])
        self.assertEqual(kinds, ["split", "split"])
        self.assertEqual(store.state()["components"], {frozenset({"A"}), frozenset({"C"})})
        # 重新放入桥 → 合并
        res = store.put("B", ["a", "b", "c"], 5)
        kinds = [e["kind"] for e in res.payload["component_events"]]
        self.assertIn("merged", kinds)
        self.assertEqual(store.state()["components"], {frozenset({"A", "B", "C"})})
        # 修订桥点内容 → 桥断开，三篇各自独立
        res = store.put("B", ["x", "y", "z"], 5)
        kinds = sorted(e["kind"] for e in res.payload["component_events"])
        self.assertEqual(kinds, ["split", "split", "split"])
        self.assertEqual(store.state()["components"],
                         {frozenset({"A"}), frozenset({"B"}), frozenset({"C"})})
        # 新文档 D 同时相似于 A、C → 合并两簇
        res = store.put("D", ["a", "b", "c"], 9)
        kinds = [e["kind"] for e in res.payload["component_events"]]
        self.assertIn("merged", kinds)
        st = store.state()
        self.assertEqual(st["components"], {frozenset({"A", "C", "D"}), frozenset({"B"})})
        self.assertEqual(st["representatives"][frozenset({"A", "C", "D"})], "D")

    def test_revision_withdraws_old_edges(self):
        store = self.mk("1/2")
        store.put("A", ["a", "b"], 1)
        store.put("B", ["a", "b", "c"], 1)
        self.assertEqual(len(store.state()["edges"]), 1)
        res = store.put("B", ["x", "y", "z"], 1)   # 修订：旧边撤回
        self.assertEqual([tuple(sorted((e["a"], e["b"]))) for e in res.payload["edges_removed"]],
                         [("A", "B")])
        self.assertEqual(store.state()["edges"], set())
        self.assertState(store, {"A": (["a", "b"], 1), "B": (["x", "y", "z"], 1)}, 1, 2)


class TestReplayAndRefusal(Base):
    def test_identical_replay_publishes_nothing(self):
        store = self.mk("1/2")
        store.put("A", ["a", "b"], 1)
        v = store.version
        res = store.put("A", ["a", "b"], 1)
        self.assertFalse(res.changed)
        self.assertEqual(store.version, v)
        self.assertEqual(len(store.versions()), v)

    def test_remove_unknown_refused(self):
        store = self.mk("1/2")
        store.put("A", ["a", "b"], 1)
        with self.assertRaises(UnknownDocError):
            store.remove("nope")
        self.assertEqual(store.version, 1)  # 拒绝不产生版本
        with self.assertRaises(UnknownDocError):
            store.explain("nope")

    def test_invalid_input(self):
        store = self.mk("1/2")
        for bad in [("", ["a"], 1), ("x", "notalist", 1), ("x", ["a", 1], 1), ("x", ["a"], 1.5)]:
            with self.assertRaises(ValueError):
                store.put(*bad)


class TestRandomized(Base):
    def test_random_op_sequences(self):
        for threshold, steps, seed in [("1/5", 200, 101), ("1/3", 200, 202),
                                       ("1/2", 200, 303), ("1/1", 120, 404)]:
            with self.subTest(threshold=threshold):
                self._run_random(threshold, steps, seed)

    def _run_random(self, threshold, steps, seed):
        from fractions import Fraction
        f = Fraction(threshold)
        p, q = f.numerator, f.denominator
        rng = random.Random(seed)
        vocab = [f"w{i}" for i in range(24)]
        pool = [f"id{i:02d}" for i in range(30)] + ["édoc", "中doc"]
        path = os.path.join(self.tmp.name, f"rand{seed}.sqlite3")
        store = DedupStore.build(path, threshold=threshold)
        docs = {}
        prev_edges = set()
        for step in range(steps):
            r = rng.random()
            if r < 0.55 or not docs:
                did = rng.choice(pool)
                tokens = [rng.choice(vocab) for _ in range(rng.randrange(0, 13))]
                quality = rng.randrange(0, 4)
                res = store.put(did, tokens, quality)
                docs[did] = (tokens, quality)
            elif r < 0.80:
                did = rng.choice(sorted(docs))
                res = store.remove(did)
                del docs[did]
            elif r < 0.95:
                did = rng.choice(sorted(docs))
                tokens = [rng.choice(vocab) for _ in range(rng.randrange(0, 13))]
                quality = rng.randrange(0, 4)
                res = store.put(did, tokens, quality)
                docs[did] = (tokens, quality)
            else:
                with self.assertRaises(UnknownDocError):
                    store.remove("ghost-id-never-put")
                continue
            # 每次发布后：边、连通分量、代表与独立成对参考逐项一致
            st = self.assertState(store, docs, p, q)
            # 发布载荷中的边差分必须精确解释状态变化
            added = {(e["a"], e["b"]) for e in res.payload.get("edges_added", [])}
            removed = {(e["a"], e["b"]) for e in res.payload.get("edges_removed", [])}
            self.assertEqual((prev_edges - removed) | added, st["edges"])
            self.assertFalse(added & prev_edges)
            self.assertLessEqual(removed, prev_edges)
            prev_edges = st["edges"]
            # 相同内容重放不多发变化
            if step % 25 == 0 and docs:
                did = rng.choice(sorted(docs))
                v0 = store.version
                res2 = store.put(did, docs[did][0], docs[did][1])
                self.assertFalse(res2.changed)
                self.assertEqual(store.version, v0)
        self.assertEqual(len(store.versions()), store.version)
        # 计数器单调且为实际计数
        stats = store.stats()
        self.assertGreaterEqual(stats["exact_comparisons"], 0)
        self.assertGreaterEqual(stats["candidate_visits"], stats["candidates_unique"])


class TestExportExplain(Base):
    def test_export_standalone_and_paths(self):
        store = self.mk("1/2")
        docs = {"A": (["a", "b"], 9), "B": (["a", "b", "c"], 5), "C": (["b", "c"], 1),
                "E1": ([], 2), "E2": (["z"], 3), "S": (["p", "q"], 4)}
        for did, (t, qq) in docs.items():
            store.put(did, t, qq)
        out = os.path.join(self.tmp.name, "export.json")
        written = store.export(out)
        loaded = json.load(open(out, encoding="utf-8"))  # 脱离数据库读回
        self.assertEqual(loaded, written)
        self.assertEqual(loaded["version"], store.version)
        self.assertEqual(loaded["summary"]["documents"], len(docs))
        self.assertEqual(loaded["summary"]["kept_representatives"],
                         loaded["summary"]["clusters"])
        members = [m for c in loaded["clusters"] for m in c["members"]]
        self.assertEqual(sorted(m["doc_id"] for m in members), sorted(docs))
        sets = {d: reference.shingle_set(t) for d, (t, _q) in docs.items()}
        for c in loaded["clusters"]:
            rep = c["representative"]
            for m in c["members"]:
                path = m["path_to_representative"]
                if m["doc_id"] == rep:
                    self.assertEqual(m["role"], "kept_representative")
                    self.assertEqual(path, [])
                    continue
                self.assertEqual(m["role"], "collapsed")
                # 路径从本篇出发、逐边相连、止于代表；每条都是真实相似边
                self.assertEqual(path[0]["from"], m["doc_id"])
                self.assertEqual(path[-1]["to"], rep)
                for e1, e2 in zip(path, path[1:]):
                    self.assertEqual(e1["to"], e2["from"])
                for e in path:
                    sa, sb = sets[e["from"]], sets[e["to"]]
                    self.assertEqual(e["intersection"], len(sa & sb))
                    self.assertEqual(e["union"], len(sa | sb))
                    self.assertTrue(reference.similar(sa, sb, 1, 2))
        # 间接相似不得伪装成直接边：C 与代表 A 不相似，C→A 的路径必须经由 B（两条真实边）
        cl = next(c for c in loaded["clusters"] if "C" in [m["doc_id"] for m in c["members"]])
        self.assertEqual(cl["representative"], "A")
        pc = next(m for m in cl["members"] if m["doc_id"] == "C")
        hops = [(e["from"], e["to"]) for e in pc["path_to_representative"]]
        self.assertEqual(hops, [("C", "B"), ("B", "A")])
        pa = next(m for m in cl["members"] if m["doc_id"] == "A")
        self.assertEqual(pa["path_to_representative"], [])

    def test_explain_contents(self):
        store = self.mk("1/2")
        store.put("A", ["a", "b"], 1)
        store.put("B", ["a", "b", "c"], 5)
        store.put("C", ["b", "c"], 1)
        ex = store.explain("A")
        self.assertEqual(ex["representative"], "B")
        self.assertEqual(ex["cluster_members"], ["A", "B", "C"])
        self.assertEqual(len(ex["neighbors"]), 1)
        n = ex["neighbors"][0]
        self.assertEqual(n["doc_id"], "B")
        self.assertEqual((n["intersection"], n["union"]), (1, 2))
        # 候选来源 shingle 确实同时属于两篇文档
        via = json.loads(n["candidate_source"]["via"])
        self.assertIn(tuple(via), reference.shingle_set(["a", "b"]))
        self.assertIn(tuple(via), reference.shingle_set(["a", "b", "c"]))
        self.assertEqual(ex["path_to_representative"][0]["to"], "B")
        # 版本日志完整
        self.assertEqual([v["version"] for v in store.versions()], [1, 2, 3])

    def test_reopen_persistence(self):
        store = self.mk("1/2")
        store.put("A", ["a", "b"], 1)
        store.put("B", ["a", "b", "c"], 5)
        store.close()
        store2 = DedupStore.open(self.path)
        self.assertEqual(store2.version, 2)
        self.assertState(store2, {"A": (["a", "b"], 1), "B": (["a", "b", "c"], 5)}, 1, 2)
        res = store2.put("A", ["a", "b"], 1)  # 重开后重放仍不产生变化
        self.assertFalse(res.changed)


if __name__ == "__main__":
    unittest.main(verbosity=2)
