#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
dedupstore.py — 可追溯近重复语料去重存储（精确 Jaccard / 二元 shingle / 版本化发布）。

产物定位：可追溯的去重语料（代表样本 + 折叠轨迹），不训练任何相似模型。

设计要点（论证与取舍见 DESIGN.md）：
- profile 固定二元 shingle（相邻 token 对）的集合；tokens 不做任何隐含规范化。
- 精确判定用整数交叉乘法：inter*q >= uni*p，无浮点；两个空集相似，恰一个空集不相似。
- 候选完备性：对大小为 x 的集合，取其任意 (x - ceil(p*x/q) + 1) 个 shingle 探测完备倒排，
  配合大小过滤 y ∈ [ceil(p*x/q), floor(x*q/p)] 与空桶特判，保证题定阈值下不漏真实边。
  本实现选"当前最稀有"的 shingle 作为探测子集——仅为选择性，完备性不依赖全局序一致。
- 相似边无向；簇 = 边图的连通分量（不宣称簇内两两达阈值）。增删后只对受影响分量
  做 BFS 重算，因此支持桥点删除后的拆分（不使用只支持合并的 union-find）。
- 代表 = quality 最高，并列取 UTF-8 字节序最小 id。
- 每次有效增删发布一个完整版本：边差分 + 分量事件（formed/grown/merged/split/
  dissolved/representative_changed）+ 实际计数器差分。相同内容重放不产生变化。
- 计数器 candidate_visits / candidates_unique / exact_comparisons 均为实际执行计数，
  不以计划估计替代。

内存预算披露（题定：同时展开的 posting/candidate 集合 ≤ 10000 个文档 ID）：
- posting 以游标流式读取，每批 BATCH_SIZE=1000 行，从不把整条 posting 装成列表；
- 候选去重 seen、BFS frontier/visited 都是单分量内的 doc_id 集合，其大小 ≤ 语料
  上限 5000 < 10000，天然在预算内；
- 原始单篇 tokens/shingle 集合驻留内存（题定豁免，在此披露）；export 结果流不计入。
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from dataclasses import dataclass
from fractions import Fraction

PROFILE_SHINGLE_N = 2          # profile：固定二元 shingle（相邻 token 对）
BATCH_SIZE = 1000              # posting/候选流式批大小（ID 数）
EMPTY_BUCKET = "empty-bucket"  # 空 shingle 集合文档的候选来源标记
EXPORT_FORMAT = "near-dedup-export/1"
COUNTERS = ("candidate_visits", "candidates_unique", "exact_comparisons")

SCHEMA = """
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE docs (
  doc_id TEXT PRIMARY KEY,
  quality INTEGER NOT NULL,
  set_size INTEGER NOT NULL
);
CREATE TABLE postings (           -- 完备倒排：每篇文档的全部 shingle
  shingle TEXT NOT NULL,
  doc_id TEXT NOT NULL,
  PRIMARY KEY (shingle, doc_id)
);
CREATE INDEX postings_by_doc ON postings (doc_id, shingle);
CREATE TABLE shingle_df (
  shingle TEXT PRIMARY KEY,
  df INTEGER NOT NULL
);
CREATE TABLE edges (              -- 精确相似边（无向，a<b 按 UTF-8 字节序存储）
  a TEXT NOT NULL,
  b TEXT NOT NULL,
  inter INTEGER NOT NULL,         -- 精确交集大小
  uni INTEGER NOT NULL,           -- 精确并集大小（0/0 表示两空集相似）
  via TEXT NOT NULL,              -- 候选来源：发现该候选的 shingle，或 empty-bucket
  found_by TEXT NOT NULL,         -- 由哪一篇文档的探测建立
  PRIMARY KEY (a, b)
);
CREATE INDEX edges_by_b ON edges (b, a);
CREATE TABLE membership (         -- 当前连通分量归属
  doc_id TEXT PRIMARY KEY,
  cluster_id INTEGER NOT NULL
);
CREATE INDEX membership_by_cluster ON membership (cluster_id);
CREATE TABLE versions (           -- 每次发布的完整版本记录
  version INTEGER PRIMARY KEY,
  op TEXT NOT NULL,
  doc_id TEXT,
  payload TEXT NOT NULL
);
"""


class StoreError(Exception):
    """存储层致命误用（库已存在、阈值非法等）。"""


class UnknownDocError(KeyError):
    """remove/explain 引用了不存在的 id —— 明确拒绝。"""


def _bytes(s: str) -> bytes:
    return s.encode("utf-8")


def encode_shingle(t1: str, t2: str) -> str:
    """二元 shingle 的确定性可逆编码；不做任何文本规范化。"""
    return json.dumps([t1, t2], ensure_ascii=False, separators=(",", ":"))


def shingle_set(tokens) -> set:
    """相邻 token 对的集合（重复出现不重复加权）。"""
    return {encode_shingle(tokens[i], tokens[i + 1]) for i in range(len(tokens) - 1)}


def parse_threshold(t) -> tuple:
    """接受 'p/q'、Fraction、(p, q)；必须为正有理数且落在 [1/5, 1]。"""
    try:
        if isinstance(t, Fraction):
            f = t
        elif isinstance(t, str):
            f = Fraction(t)
        elif isinstance(t, (tuple, list)) and len(t) == 2:
            f = Fraction(int(t[0]), int(t[1]))
        else:
            raise ValueError
    except (ValueError, ZeroDivisionError, TypeError):
        raise ValueError(f"threshold 须为 [1/5, 1] 内的正有理数，得到 {t!r}")
    if not (Fraction(1, 5) <= f <= 1):
        raise ValueError(f"threshold 须为 [1/5, 1] 内的正有理数，得到 {t!r}")
    return f.numerator, f.denominator


@dataclass
class OpResult:
    """一次 put/remove 的返回：是否产生变化、当前版本、完整发布载荷。"""
    changed: bool
    version: int
    payload: dict

    def __getitem__(self, k):
        return self.payload[k]

    def to_dict(self):
        return dict(self.payload)


class DedupStore:
    # ------------------------------------------------------------------ 构造
    @classmethod
    def build(cls, path, threshold="1/5", docs=None, overwrite=False):
        """新建库（可选批量装入初始文档，逐篇发布版本）。"""
        if path != ":memory:" and os.path.exists(path):
            if overwrite:
                os.remove(path)
            else:
                raise StoreError(f"库已存在: {path}（如需重建请 overwrite=True）")
        p, q = parse_threshold(threshold)
        self = cls.__new__(cls)
        self._connect(path)
        self._init_fresh(p, q)
        if docs:
            for d in docs:
                if isinstance(d, dict):
                    self.put(d["id"], d["tokens"], d["quality"])
                else:
                    self.put(d[0], d[1], d[2])
        return self

    @classmethod
    def open(cls, path):
        """重新打开本机已有库。"""
        if path != ":memory:" and not os.path.exists(path):
            raise StoreError(f"库不存在: {path}")
        self = cls.__new__(cls)
        self._connect(path)
        self._load()
        return self

    def _connect(self, path):
        self._path = path
        self._conn = sqlite3.connect(path, isolation_level=None)  # 手动事务
        self._conn.execute("PRAGMA busy_timeout=30000")
        self._conn.execute("PRAGMA synchronous=NORMAL")

    def _init_fresh(self, p, q):
        self._conn.executescript(SCHEMA)
        self._p, self._q = p, q
        self._version = 0
        self._next_cluster = 0
        self._candidate_visits = 0
        self._candidates_unique = 0
        self._exact_comparisons = 0
        self._conn.execute("INSERT INTO meta(key, value) VALUES (?,?)",
                           ("profile", json.dumps({"shingle_size": PROFILE_SHINGLE_N})))
        self._conn.execute("INSERT INTO meta(key, value) VALUES (?,?)", ("threshold_p", str(p)))
        self._conn.execute("INSERT INTO meta(key, value) VALUES (?,?)", ("threshold_q", str(q)))
        self._save_meta()

    def _load(self):
        meta = {k: v for k, v in self._conn.execute("SELECT key, value FROM meta")}
        profile = json.loads(meta["profile"])
        if profile.get("shingle_size") != PROFILE_SHINGLE_N:
            raise StoreError("profile 不匹配：本实现固定二元 shingle")
        self._p = int(meta["threshold_p"])
        self._q = int(meta["threshold_q"])
        self._version = int(meta["version"])
        self._next_cluster = int(meta["next_cluster_id"])
        self._candidate_visits = int(meta["candidate_visits"])
        self._candidates_unique = int(meta["candidates_unique"])
        self._exact_comparisons = int(meta["exact_comparisons"])

    def close(self):
        self._conn.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # ------------------------------------------------------------------ 事务/元数据
    def _begin(self):
        self._conn.execute("BEGIN IMMEDIATE")

    def _commit(self):
        self._conn.commit()

    def _rollback(self):
        self._conn.rollback()

    def _counters(self):
        return {"candidate_visits": self._candidate_visits,
                "candidates_unique": self._candidates_unique,
                "exact_comparisons": self._exact_comparisons}

    def _save_meta(self):
        vals = {"version": self._version, "next_cluster_id": self._next_cluster}
        vals.update(self._counters())
        self._conn.executemany(
            "INSERT INTO meta(key, value) VALUES (?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            [(k, str(v)) for k, v in vals.items()])

    def _next_cid(self):
        self._next_cluster += 1
        return self._next_cluster

    # ------------------------------------------------------------------ 校验
    @staticmethod
    def _validate_doc(doc_id, tokens, quality):
        if not isinstance(doc_id, str) or not doc_id:
            raise ValueError("doc_id 必须是非空字符串")
        if isinstance(tokens, str) or not isinstance(tokens, (list, tuple)) \
                or not all(isinstance(t, str) for t in tokens):
            raise ValueError("tokens 必须是字符串列表")
        if not isinstance(quality, int) or isinstance(quality, bool):
            raise ValueError("quality 必须是整数")

    # ------------------------------------------------------------------ 基本查询
    def _doc_exists(self, doc_id):
        return self._conn.execute("SELECT 1 FROM docs WHERE doc_id=?", (doc_id,)).fetchone() is not None

    def _df_add(self, shingle, delta):
        if delta > 0:
            self._conn.execute(
                "INSERT INTO shingle_df(shingle, df) VALUES (?,1) "
                "ON CONFLICT(shingle) DO UPDATE SET df=df+1", (shingle,))
        else:
            self._conn.execute("UPDATE shingle_df SET df=df-1 WHERE shingle=?", (shingle,))
            self._conn.execute("DELETE FROM shingle_df WHERE shingle=? AND df<=0", (shingle,))

    def _edges_of(self, doc_id):
        out = []
        for a, b, inter, uni, via, fb in self._conn.execute(
                "SELECT a,b,inter,uni,via,found_by FROM edges WHERE a=? OR b=?", (doc_id, doc_id)):
            out.append({"a": a, "b": b, "intersection": inter, "union": uni,
                        "via": via, "found_by": fb, "other": b if a == doc_id else a})
        return out

    def _rep_of(self, cid):
        # SQLite TEXT 的 BINARY 排序即 UTF-8 字节序：quality 降序、id 字节序升序。
        r = self._conn.execute(
            "SELECT m.doc_id FROM membership m JOIN docs d ON d.doc_id=m.doc_id "
            "WHERE m.cluster_id=? ORDER BY d.quality DESC, m.doc_id ASC LIMIT 1", (cid,)).fetchone()
        return r[0] if r else None

    def _bfs_component(self, start):
        """当前边图上从 start 出发的连通分量（frontier/visited ≤ 语料上限 5000 < 预算）。"""
        if not self._doc_exists(start):
            return set()
        seen = {start}
        frontier = [start]
        while frontier:
            nxt = []
            for d in frontier:
                for (n,) in self._conn.execute(
                        "SELECT b FROM edges WHERE a=? UNION SELECT a FROM edges WHERE b=?", (d, d)):
                    if n not in seen:
                        seen.add(n)
                        nxt.append(n)
            frontier = nxt
        return seen

    # ------------------------------------------------------------------ 候选与精确判定
    def _exact_compare(self, shingles, x, cand, y):
        """一次实际精确判定：整数交叉乘法，不用浮点。"""
        self._exact_comparisons += 1
        bset = {r[0] for r in self._conn.execute("SELECT shingle FROM postings WHERE doc_id=?", (cand,))}
        inter = len(shingles & bset)
        uni = x + y - inter
        return (inter * self._q >= uni * self._p), inter, uni

    def _probe(self, doc_id, shingles, x):
        """为 doc_id 生成候选并逐对精确判定，返回新边列表。

        完备性：任意相似的非空 B 必满足大小过滤且与 A 的任意
        (x - ceil(p*x/q) + 1) 元子集相交（见 DESIGN.md 证明），因此必被探测到。
        """
        added = []
        if x == 0:
            # 空桶：两个空集相似；恰一个空集不相似，故候选仅为其他空集文档。
            cur = self._conn.execute(
                "SELECT doc_id FROM docs WHERE set_size=0 AND doc_id != ? ORDER BY doc_id", (doc_id,))
            while True:
                rows = cur.fetchmany(BATCH_SIZE)
                if not rows:
                    break
                for (cand,) in rows:
                    self._candidate_visits += 1
                    self._candidates_unique += 1
                    self._exact_comparisons += 1  # 规则判定：两空集相似
                    added.append(self._mk_edge(doc_id, cand, 0, 0, EMPTY_BUCKET))
            return added
        lo = (self._p * x + self._q - 1) // self._q   # ceil(p*x/q)
        hi = (x * self._q) // self._p                 # floor(x*q/p)
        k = x - lo + 1                                # 探测子集大小
        dfmap = {sh: df for sh, df in self._conn.execute(
            "SELECT shingle, df FROM shingle_df WHERE shingle IN (%s)"
            % ",".join("?" * len(shingles)), tuple(sorted(shingles)))}
        # 选当前最稀有的 k 个 shingle（仅为选择性；完备性不依赖此选择）
        prefix = sorted(shingles, key=lambda s: (dfmap[s], s))[:k]
        seen = {}  # cand -> 首次发现它的 shingle（≤5000 个 ID，在预算内）
        cur = self._conn.execute(
            "SELECT p.shingle, p.doc_id, d.set_size FROM postings p "
            "JOIN docs d ON d.doc_id = p.doc_id "
            "WHERE p.shingle IN (%s) AND d.set_size BETWEEN ? AND ? AND p.doc_id != ?"
            % ",".join("?" * len(prefix)),
            tuple(prefix) + (lo, hi, doc_id))
        while True:
            rows = cur.fetchmany(BATCH_SIZE)   # 流式：热门 posting 不整体装入内存
            if not rows:
                break
            for sh, cand, y in rows:
                self._candidate_visits += 1
                if cand in seen:
                    continue
                seen[cand] = sh
                self._candidates_unique += 1
                ok, inter, uni = self._exact_compare(shingles, x, cand, y)
                if ok:
                    added.append(self._mk_edge(doc_id, cand, inter, uni, sh))
        return added

    def _mk_edge(self, doc_id, cand, inter, uni, via):
        a, b = (doc_id, cand) if _bytes(doc_id) < _bytes(cand) else (cand, doc_id)
        return {"a": a, "b": b, "intersection": inter, "union": uni,
                "via": via, "found_by": doc_id, "other": cand}

    # ------------------------------------------------------------------ 簇重算
    def _recluster(self, seeds, region_old):
        """对受影响区域重算连通分量（支持合并与拆分），发布分量事件。

        seeds: 需要重新判定的 doc_id（旧分量全体成员 + 新邻居 + 目标文档）。
        region_old: {cluster_id: {"members": set, "rep": id}}，变更前的受影响分量。
        """
        events, rep_changes = [], []
        visited, new_comps = set(), []
        for s in sorted(seeds, key=_bytes):
            if s in visited:
                continue
            comp = self._bfs_component(s)
            if not comp:
                continue
            new_comps.append(comp)
            visited |= comp
        old_by_members = {}
        member_old_cid = {}
        for cid, info in region_old.items():
            old_by_members.setdefault(frozenset(info["members"]), cid)
            for m in info["members"]:
                member_old_cid[m] = cid
        for cid in region_old:
            self._conn.execute("DELETE FROM membership WHERE cluster_id=?", (cid,))
        used_old, referenced, assigned = set(), set(), []
        new_comps.sort(key=lambda c: _bytes(min(c, key=_bytes)))
        for comp in new_comps:
            fs = frozenset(comp)
            cid = old_by_members.get(fs)
            if cid is not None and cid not in used_old:
                used_old.add(cid)          # 成员集合未变：保留原 cluster_id
                kind = "unchanged"
            else:
                cid = self._next_cid()     # 成员集合变化：分配新 cluster_id
                kind = "fresh"
            self._conn.executemany("INSERT INTO membership(doc_id, cluster_id) VALUES (?,?)",
                                   [(m, cid) for m in sorted(comp, key=_bytes)])
            rep = self._rep_of(cid)
            from_clusters = sorted({member_old_cid[m] for m in comp if m in member_old_cid})
            assigned.append((cid, fs, rep, kind, from_clusters))
            referenced.update(from_clusters)
        for cid, fs, rep, kind, from_clusters in assigned:
            members_sorted = sorted(fs, key=_bytes)
            if kind == "unchanged":
                old_rep = region_old[cid]["rep"]
                if old_rep != rep:
                    rep_changes.append({"cluster_id": cid, "old": old_rep, "new": rep})
                    events.append({"kind": "representative_changed", "cluster_id": cid,
                                   "members": members_sorted, "representative": rep,
                                   "previous_representative": old_rep})
                continue
            if not from_clusters:
                ekind = "formed"
            elif len(from_clusters) > 1:
                ekind = "merged"
            else:
                old_members = region_old[from_clusters[0]]["members"]
                ekind = "split" if fs < old_members else "grown" if fs > old_members else "reformed"
            events.append({"kind": ekind, "cluster_id": cid, "members": members_sorted,
                           "representative": rep, "from_clusters": from_clusters})
        for cid, info in region_old.items():
            if cid not in used_old and cid not in referenced:
                events.append({"kind": "dissolved", "cluster_id": cid,
                               "members": sorted(info["members"], key=_bytes),
                               "representative": info["rep"]})
        return events, rep_changes

    # ------------------------------------------------------------------ 发布
    def _publish(self, op, doc_id, c0, extra):
        self._version += 1
        payload = {"version": self._version, "op": op, "doc_id": doc_id, "changed": True}
        payload.update(extra)
        now = self._counters()
        payload["counters_delta"] = {k: now[k] - c0[k] for k in COUNTERS}
        payload["counters_total"] = now
        self._conn.execute("INSERT INTO versions(version, op, doc_id, payload) VALUES (?,?,?,?)",
                           (self._version, op, doc_id, json.dumps(payload, ensure_ascii=False)))
        self._save_meta()
        return payload

    # ------------------------------------------------------------------ 对外操作
    def put(self, doc_id, tokens, quality):
        """新增或修订文档。相同内容重放不产生变化；修订先撤回旧 shingle/旧边再建新边。"""
        self._validate_doc(doc_id, tokens, quality)
        tokens = list(tokens)
        shingles = shingle_set(tokens)
        x = len(shingles)
        self._begin()
        try:
            row = self._conn.execute("SELECT quality, set_size FROM docs WHERE doc_id=?",
                                     (doc_id,)).fetchone()
            if row is not None:
                old_shingles = {r[0] for r in self._conn.execute(
                    "SELECT shingle FROM postings WHERE doc_id=?", (doc_id,))}
                if old_shingles == shingles:
                    if row[0] == quality:
                        # 相同内容重放：不多发变化
                        self._commit()
                        return OpResult(False, self._version,
                                        {"op": "put", "doc_id": doc_id, "changed": False,
                                         "reason": "identical replay", "version": self._version})
                    # 仅 quality 变化：边与分量不变，只需重判代表
                    c0 = self._counters()
                    cid = self._conn.execute("SELECT cluster_id FROM membership WHERE doc_id=?",
                                             (doc_id,)).fetchone()[0]
                    old_rep = self._rep_of(cid)
                    self._conn.execute("UPDATE docs SET quality=? WHERE doc_id=?", (quality, doc_id))
                    new_rep = self._rep_of(cid)
                    events, rep_changes = [], []
                    if new_rep != old_rep:
                        rep_changes.append({"cluster_id": cid, "old": old_rep, "new": new_rep})
                        members = sorted((r[0] for r in self._conn.execute(
                            "SELECT doc_id FROM membership WHERE cluster_id=?", (cid,))), key=_bytes)
                        events.append({"kind": "representative_changed", "cluster_id": cid,
                                       "members": members, "representative": new_rep,
                                       "previous_representative": old_rep})
                    payload = self._publish("put", doc_id, c0,
                                            {"revision": "quality_only", "edges_added": [],
                                             "edges_removed": [], "component_events": events,
                                             "representative_changes": rep_changes})
                    self._commit()
                    return OpResult(True, self._version, payload)
            c0 = self._counters()
            old_edges, old_neighbors, old_cid, old_members, old_rep = [], set(), None, set(), None
            if row is not None:
                # 修订：先撤回旧边、旧 shingle、旧归属
                old_edges = self._edges_of(doc_id)
                old_neighbors = {e["other"] for e in old_edges}
                old_cid = self._conn.execute("SELECT cluster_id FROM membership WHERE doc_id=?",
                                             (doc_id,)).fetchone()[0]
                old_members = {r[0] for r in self._conn.execute(
                    "SELECT doc_id FROM membership WHERE cluster_id=?", (old_cid,))}
                old_rep = self._rep_of(old_cid)
                self._conn.execute("DELETE FROM edges WHERE a=? OR b=?", (doc_id, doc_id))
                self._conn.execute("DELETE FROM postings WHERE doc_id=?", (doc_id,))
                for sh in old_shingles:
                    self._df_add(sh, -1)
                self._conn.execute("DELETE FROM docs WHERE doc_id=?", (doc_id,))
                self._conn.execute("DELETE FROM membership WHERE doc_id=?", (doc_id,))
            # 建立新 shingle
            self._conn.execute("INSERT INTO docs(doc_id, quality, set_size) VALUES (?,?,?)",
                               (doc_id, quality, x))
            self._conn.executemany("INSERT INTO postings(shingle, doc_id) VALUES (?,?)",
                                   [(sh, doc_id) for sh in sorted(shingles)])
            for sh in shingles:
                self._df_add(sh, +1)
            # 候选探测 + 精确判定 → 新边
            added = self._probe(doc_id, shingles, x)
            for e in added:
                self._conn.execute(
                    "INSERT INTO edges(a,b,inter,uni,via,found_by) VALUES (?,?,?,?,?,?)",
                    (e["a"], e["b"], e["intersection"], e["union"], e["via"], e["found_by"]))
            # 受影响区域重算
            new_neighbors = {e["other"] for e in added}
            region_old = {}
            if old_cid is not None:
                region_old[old_cid] = {"members": old_members, "rep": old_rep}
            for n in new_neighbors:
                cid2 = self._conn.execute("SELECT cluster_id FROM membership WHERE doc_id=?",
                                          (n,)).fetchone()[0]
                if cid2 not in region_old:
                    members2 = {r[0] for r in self._conn.execute(
                        "SELECT doc_id FROM membership WHERE cluster_id=?", (cid2,))}
                    region_old[cid2] = {"members": members2, "rep": self._rep_of(cid2)}
            seeds = {doc_id} | old_members | old_neighbors | new_neighbors
            events, rep_changes = self._recluster(seeds, region_old)
            payload = self._publish("put", doc_id, c0, {
                "revision": "content" if row is not None else "new",
                "edges_added": [{k: e[k] for k in ("a", "b", "intersection", "union", "via", "found_by")}
                                for e in added],
                "edges_removed": [{k: e[k] for k in ("a", "b", "intersection", "union")}
                                  for e in old_edges],
                "component_events": events,
                "representative_changes": rep_changes})
            self._commit()
            return OpResult(True, self._version, payload)
        except BaseException:
            self._rollback()
            raise

    def remove(self, doc_id):
        """删除文档；未知 id 明确拒绝。桥点删除会触发原分量的重新判定。"""
        if not isinstance(doc_id, str) or not doc_id:
            raise ValueError("doc_id 必须是非空字符串")
        self._begin()
        try:
            row = self._conn.execute("SELECT quality, set_size FROM docs WHERE doc_id=?",
                                     (doc_id,)).fetchone()
            if row is None:
                self._rollback()
                raise UnknownDocError(doc_id)
            c0 = self._counters()
            old_edges = self._edges_of(doc_id)
            old_cid = self._conn.execute("SELECT cluster_id FROM membership WHERE doc_id=?",
                                         (doc_id,)).fetchone()[0]
            old_members = {r[0] for r in self._conn.execute(
                "SELECT doc_id FROM membership WHERE cluster_id=?", (old_cid,))}
            old_rep = self._rep_of(old_cid)
            old_shingles = {r[0] for r in self._conn.execute(
                "SELECT shingle FROM postings WHERE doc_id=?", (doc_id,))}
            self._conn.execute("DELETE FROM edges WHERE a=? OR b=?", (doc_id, doc_id))
            self._conn.execute("DELETE FROM postings WHERE doc_id=?", (doc_id,))
            for sh in old_shingles:
                self._df_add(sh, -1)
            self._conn.execute("DELETE FROM docs WHERE doc_id=?", (doc_id,))
            self._conn.execute("DELETE FROM membership WHERE doc_id=?", (doc_id,))
            region_old = {old_cid: {"members": old_members, "rep": old_rep}}
            seeds = old_members - {doc_id}
            events, rep_changes = self._recluster(seeds, region_old)
            payload = self._publish("remove", doc_id, c0, {
                "edges_added": [],
                "edges_removed": [{k: e[k] for k in ("a", "b", "intersection", "union")}
                                  for e in old_edges],
                "component_events": events,
                "representative_changes": rep_changes})
            self._commit()
            return OpResult(True, self._version, payload)
        except BaseException:
            self._rollback()
            raise

    # ------------------------------------------------------------------ 读侧
    @property
    def version(self):
        return self._version

    def stats(self):
        return {"version": self._version,
                "documents": self._conn.execute("SELECT COUNT(*) FROM docs").fetchone()[0],
                "edges": self._conn.execute("SELECT COUNT(*) FROM edges").fetchone()[0],
                "clusters": self._conn.execute("SELECT COUNT(DISTINCT cluster_id) FROM membership").fetchone()[0],
                **self._counters()}

    def versions(self, limit=None):
        sql = "SELECT payload FROM versions ORDER BY version"
        if limit:
            sql = "SELECT payload FROM versions ORDER BY version DESC LIMIT %d" % int(limit)
        return [json.loads(r[0]) for r in self._conn.execute(sql)]

    def state(self):
        """当前发布状态：边集、连通分量集合、各分量代表（供测试逐项比对）。"""
        edges = {(a, b) for a, b in self._conn.execute("SELECT a, b FROM edges")}
        comps = {}
        for cid, did in self._conn.execute("SELECT cluster_id, doc_id FROM membership"):
            comps.setdefault(cid, set()).add(did)
        reps = {frozenset(m): self._rep_of(cid) for cid, m in comps.items()}
        return {"version": self._version, "edges": edges,
                "components": {frozenset(m) for m in comps.values()},
                "representatives": reps}

    def _paths_to_rep(self, rep):
        """从代表出发的 BFS 树：每篇文档到代表的相似边路径（只引用真实边及其计数）。"""
        parent = {rep: None}
        pedge = {}
        frontier = [rep]
        while frontier:
            nxt = []
            for d in sorted(frontier, key=_bytes):
                for a, b, inter, uni in self._conn.execute(
                        "SELECT a,b,inter,uni FROM edges WHERE a=? OR b=?", (d, d)):
                    nb = b if a == d else a
                    if nb not in parent:
                        parent[nb] = d
                        edge = {"from": nb, "to": d, "intersection": inter, "union": uni,
                                "jaccard": f"{inter}/{uni}" if uni else "0/0"}
                        if uni == 0:
                            edge["note"] = "两个 shingle 集合均为空，按规则相似"
                        pedge[nb] = edge
                        nxt.append(nb)
            frontier = nxt
        paths = {}
        for d in parent:
            path, cur = [], d
            while parent[cur] is not None:
                path.append(pedge[cur])
                cur = parent[cur]
            paths[d] = path
        return paths

    def export(self, path=None):
        """读取一个一致版本，导出可脱离内部数据库的完整追溯 JSON。"""
        self._begin()
        try:
            clusters = []
            cids = [r[0] for r in self._conn.execute(
                "SELECT DISTINCT cluster_id FROM membership ORDER BY cluster_id")]
            ndocs = 0
            for cid in cids:
                members = [(r[0], r[1]) for r in self._conn.execute(
                    "SELECT m.doc_id, d.quality FROM membership m "
                    "JOIN docs d ON d.doc_id=m.doc_id WHERE m.cluster_id=? ORDER BY m.doc_id",
                    (cid,))]
                rep = self._rep_of(cid)
                paths = self._paths_to_rep(rep)
                mlist = [{"doc_id": did, "quality": q,
                          "role": "kept_representative" if did == rep else "collapsed",
                          "path_to_representative": paths[did]}
                         for did, q in members]
                clusters.append({"cluster_id": cid, "representative": rep,
                                 "size": len(members), "members": mlist})
                ndocs += len(members)
            nedges = self._conn.execute("SELECT COUNT(*) FROM edges").fetchone()[0]
            doc = {"format": EXPORT_FORMAT,
                   "version": self._version,
                   "profile": {"shingle_size": PROFILE_SHINGLE_N,
                               "similarity": "exact-jaccard",
                               "threshold": f"{self._p}/{self._q}",
                               "threshold_p": self._p, "threshold_q": self._q},
                   "summary": {"documents": ndocs, "clusters": len(clusters),
                               "kept_representatives": len(clusters),
                               "collapsed": ndocs - len(clusters),
                               "similarity_edges": nedges},
                   "clusters": clusters,
                   "statistics": self._counters()}
            self._commit()
        except BaseException:
            self._rollback()
            raise
        if path:
            with open(path, "w", encoding="utf-8") as fh:
                json.dump(doc, fh, ensure_ascii=False, indent=2)
        return doc

    def explain(self, doc_id):
        """给出候选来源、精确交/并集计数、所属簇与到代表的路径。"""
        row = self._conn.execute("SELECT quality, set_size FROM docs WHERE doc_id=?",
                                 (doc_id,)).fetchone()
        if row is None:
            raise UnknownDocError(doc_id)
        quality, x = row
        neighbors = []
        for e in self._edges_of(doc_id):
            neighbors.append({
                "doc_id": e["other"],
                "intersection": e["intersection"],
                "union": e["union"],
                "jaccard": f"{e['intersection']}/{e['union']}" if e["union"] else "0/0",
                "threshold": f"{self._p}/{self._q}",
                "meets_threshold": True,
                "candidate_source": {"via": e["via"], "discovered_by": e["found_by"]}})
        neighbors.sort(key=lambda d: _bytes(d["doc_id"]))
        cid = self._conn.execute("SELECT cluster_id FROM membership WHERE doc_id=?",
                                 (doc_id,)).fetchone()[0]
        members = sorted((r[0] for r in self._conn.execute(
            "SELECT doc_id FROM membership WHERE cluster_id=?", (cid,))), key=_bytes)
        rep = self._rep_of(cid)
        if x == 0:
            probe_info = {"candidate_source": EMPTY_BUCKET,
                          "note": "空 shingle 集合：候选为其他空集文档（两空集相似）"}
        else:
            lo = (self._p * x + self._q - 1) // self._q
            hi = (x * self._q) // self._p
            k = x - lo + 1
            own = sorted(r[0] for r in self._conn.execute(
                "SELECT shingle FROM postings WHERE doc_id=?", (doc_id,)))
            dfmap = {sh: df for sh, df in self._conn.execute(
                "SELECT shingle, df FROM shingle_df WHERE shingle IN (%s)"
                % ",".join("?" * len(own)), tuple(own))} if own else {}
            probe_info = {"threshold": f"{self._p}/{self._q}",
                          "set_size": x,
                          "size_window": [lo, hi],
                          "prefix_size": k,
                          "prefix_shingles_current": sorted(own, key=lambda s: (dfmap[s], s))[:k],
                          "note": "探测键按当前 df 稀有度选取；完备性不依赖该顺序"}
        return {"doc_id": doc_id, "quality": quality, "shingle_count": x,
                "probe": probe_info,
                "neighbors": neighbors,
                "cluster_id": cid, "cluster_size": len(members), "cluster_members": members,
                "representative": rep, "is_representative": doc_id == rep,
                "path_to_representative": self._paths_to_rep(rep)[doc_id],
                "counters": self._counters(), "version": self._version}


# ---------------------------------------------------------------------- CLI
def _tokens_arg(s):
    if s.startswith("@"):
        with open(s[1:], encoding="utf-8") as fh:
            return json.load(fh)
    return json.loads(s)


def _cli(argv=None):
    ap = argparse.ArgumentParser(prog="dedupstore", description="可追溯近重复语料去重存储")
    sub = ap.add_subparsers(dest="cmd", required=True)
    b = sub.add_parser("build", help="新建库")
    b.add_argument("db"); b.add_argument("--threshold", default="1/5")
    b.add_argument("--docs", help="JSONL：每行 {\"id\",\"tokens\",\"quality\"}")
    p = sub.add_parser("put", help="新增/修订文档")
    p.add_argument("db"); p.add_argument("doc_id"); p.add_argument("quality", type=int)
    p.add_argument("tokens", help="JSON 数组，或 @文件")
    r = sub.add_parser("remove", help="删除文档")
    r.add_argument("db"); r.add_argument("doc_id")
    e = sub.add_parser("export", help="导出一致版本")
    e.add_argument("db"); e.add_argument("--out")
    x = sub.add_parser("explain", help="解释单篇文档")
    x.add_argument("db"); x.add_argument("doc_id")
    s = sub.add_parser("stats", help="计数器与规模")
    s.add_argument("db")
    v = sub.add_parser("versions", help="版本日志")
    v.add_argument("db"); v.add_argument("--limit", type=int)
    args = ap.parse_args(argv)
    try:
        if args.cmd == "build":
            docs = None
            if args.docs:
                docs = [json.loads(line) for line in open(args.docs, encoding="utf-8") if line.strip()]
            store = DedupStore.build(args.db, threshold=args.threshold, docs=docs)
            print(json.dumps(store.stats(), ensure_ascii=False, indent=2))
        elif args.cmd == "put":
            store = DedupStore.open(args.db)
            print(json.dumps(store.put(args.doc_id, _tokens_arg(args.tokens), args.quality).payload,
                             ensure_ascii=False, indent=2))
        elif args.cmd == "remove":
            store = DedupStore.open(args.db)
            print(json.dumps(store.remove(args.doc_id).payload, ensure_ascii=False, indent=2))
        elif args.cmd == "export":
            store = DedupStore.open(args.db)
            doc = store.export(args.out)
            if not args.out:
                print(json.dumps(doc, ensure_ascii=False, indent=2))
            else:
                print(f"已导出版本 {doc['version']} → {args.out}")
        elif args.cmd == "explain":
            store = DedupStore.open(args.db)
            print(json.dumps(store.explain(args.doc_id), ensure_ascii=False, indent=2))
        elif args.cmd == "stats":
            store = DedupStore.open(args.db)
            print(json.dumps(store.stats(), ensure_ascii=False, indent=2))
        elif args.cmd == "versions":
            store = DedupStore.open(args.db)
            print(json.dumps(store.versions(args.limit), ensure_ascii=False, indent=2))
    except UnknownDocError as e:
        print(f"拒绝：未知文档 id {e.args[0]!r}", file=sys.stderr)
        return 2
    except (StoreError, ValueError) as e:
        print(f"错误：{e}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(_cli())
