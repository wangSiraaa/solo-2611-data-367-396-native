"""索引引擎：不可变段 + 显式提交清单 + 固定读视图。

并发模型（题面只要求单写者、一并发读者）：
- 写操作（put/delete/compact）持进程内 threading.Lock，并对 root/.write.lock
  加 fcntl 排他锁，兼容同机多进程的单写者约定；
- 读路径不加写锁：snapshot() 先登记租约再复制清单并把段整体读入内存，
  compact 的段回收必须避让在册租约，因此旧读视图在合并期间可正常读完；
- 清单与段均以临时文件 + os.replace 原子发布，读者要么看到完整旧清单，
  要么看到完整新清单，不存在半合并状态。
"""

from __future__ import annotations

import fcntl
import os
import threading
from dataclasses import dataclass

from .analyzer import Analyzer
from .storage import (
    LEASE_DIR,
    P_BEND,
    P_BSTART,
    P_DOC,
    P_FIELD,
    P_POS,
    P_REV,
    P_VALUE,
    Manifest,
    Segment,
    create_lease,
    ensure_layout,
    live_lease_pids,
    load_manifest,
    load_segment,
    release_lease,
    write_manifest,
    write_segment,
)

DEFAULT_FIELDS = ("title", "body")

# 本进程仍然在册的租约（compact 与读视图同进程并发时据此保护旧段）
_LOCAL_LEASES: set[str] = set()


class QueryError(ValueError):
    """非法查询（未知 field、停词、非法 max_gap 等），CLI 据此明确拒绝。"""


@dataclass(frozen=True)
class Occ:
    """一个可回到原文的词出现。"""

    doc_id: str
    revision: int
    field: str
    value_no: int
    position: int
    byte_start: int
    byte_end: int


# ---------------------------------------------------------------- 引擎


class IndexEngine:
    def __init__(self, root: str, analyzer: Analyzer | None = None,
                 fields: tuple[str, ...] = DEFAULT_FIELDS):
        self.root = root
        self.fields = tuple(fields)
        ensure_layout(root)
        self._lock_path = os.path.join(root, ".write.lock")
        self._thread_lock = threading.Lock()
        manifest = load_manifest(root)
        if manifest is None:
            if analyzer is None:
                raise FileNotFoundError(f"{root} 下没有清单，build 之后才能打开")
            self.analyzer = analyzer
            self._manifest = Manifest(stopwords=list(analyzer.stopwords))
            write_manifest(root, self._manifest)
        else:
            self.analyzer = analyzer or Analyzer(manifest.stopwords)
            self._manifest = manifest

    # ---- 内部工具 ----

    def _reload(self) -> Manifest:
        # 单写者前提下重新读盘，保证与同机其他进程的提交一致
        m = load_manifest(self.root)
        assert m is not None
        self._manifest = m
        return m

    class _FileLock:
        def __init__(self, path: str):
            self.path = path
            self.fd = os.open(path, os.O_CREAT | os.O_RDWR)

        def __enter__(self):
            fcntl.flock(self.fd, fcntl.LOCK_EX)
            return self

        def __exit__(self, *exc):
            fcntl.flock(self.fd, fcntl.LOCK_UN)
            os.close(self.fd)

    def _validate_doc(self, doc: dict) -> None:
        if not isinstance(doc, dict) or not isinstance(doc.get("doc_id"), str) \
                or not doc["doc_id"]:
            raise ValueError("put 文档必须带非空字符串 doc_id")
        for field in self.fields:
            vals = doc.get(field, [])
            if not isinstance(vals, list) or not all(isinstance(v, str) for v in vals):
                raise ValueError(f"字段 {field} 必须是字符串数组")

    def _build_segment(self, doc_id: str, revision: int, doc: dict | None) -> Segment:
        m = self._manifest
        seg = Segment(seg_id=m.next_seg_id, postings={}, docs={})
        if doc is not None:  # put：重新索引该文档全部字段
            for field in self.fields:
                for value_no, value in enumerate(doc.get(field, [])):
                    for tok in self.analyzer.indexable(value):
                        seg.postings.setdefault(tok.term, []).append([
                            doc_id, revision, field, value_no,
                            tok.position, tok.byte_start, tok.byte_end,
                        ])
        # delete 段 postings 为空，仅用 docs 里的 null 记录墓碑 revision
        seg.docs[doc_id] = {"revision": revision, "doc": doc}
        return seg

    # ---- 写路径 ----

    def put(self, doc: dict) -> int:
        self._validate_doc(doc)
        with self._thread_lock, self._FileLock(self._lock_path):
            m = self._reload()
            doc_id = doc["doc_id"]
            revision = m.next_rev.get(doc_id, 0) + 1
            m.next_rev[doc_id] = revision
            seg = self._build_segment(doc_id, revision, doc)
            write_segment(self.root, seg)
            m.segments.append(seg.seg_id)
            m.active[doc_id] = {
                "revision": revision, "op": "put", "seg": seg.seg_id,
            }
            m.next_seg_id += 1
            m.generation += 1
            write_manifest(self.root, m)
            return revision

    def delete(self, doc_id: str) -> int:
        if not isinstance(doc_id, str) or not doc_id:
            raise ValueError("delete 需要非空 doc_id")
        with self._thread_lock, self._FileLock(self._lock_path):
            m = self._reload()
            revision = m.next_rev.get(doc_id, 0) + 1
            m.next_rev[doc_id] = revision
            seg = self._build_segment(doc_id, revision, None)
            write_segment(self.root, seg)
            m.segments.append(seg.seg_id)
            m.active[doc_id] = {
                "revision": revision, "op": "delete", "seg": seg.seg_id,
            }
            m.next_seg_id += 1
            m.generation += 1
            write_manifest(self.root, m)
            return revision

    # ---- 读视图 ----

    def snapshot(self) -> "Snapshot":
        # 先登记租约再读清单：任何随后的 compact 都不能删掉本视图需要的段
        lease = create_lease(self.root)
        _LOCAL_LEASES.add(lease)
        try:
            m = load_manifest(self.root)
            assert m is not None
            segments = [load_segment(self.root, sid) for sid in m.segments]
        except BaseException:
            _LOCAL_LEASES.discard(lease)
            release_lease(lease)
            raise
        return Snapshot(self.root, m, segments, Analyzer(m.stopwords),
                        self.fields, lease)

    # ---- 段合并 ----

    def compact(self) -> dict:
        """把清单内全部段合并成一个新段，清除被覆盖/删除的旧 posting。"""
        with self._thread_lock, self._FileLock(self._lock_path):
            m = self._reload()
            old_ids = list(m.segments)
            if len(old_ids) < 2:
                return {"merged": [], "new_segment": None,
                        "reason": "少于两个段，无需合并"}
            old_segments = [load_segment(self.root, sid) for sid in old_ids]
            merged = Segment(seg_id=m.next_seg_id, postings={}, docs={})

            # 只保留清单中当前活跃版本（put）的 posting；旧 revision 与
            # delete 墓碑对应的数据在此被物理清除，合并后不可能复活。
            for seg in old_segments:
                for term, plist in seg.postings.items():
                    kept = merged.postings.setdefault(term, [])
                    for p in plist:
                        rec = m.active.get(p[P_DOC])
                        if (rec is not None and rec["op"] == "put"
                                and rec["revision"] == p[P_REV]):
                            kept.append(list(p))
            for doc_id, rec in m.active.items():
                if rec["op"] != "put":
                    continue
                win = next(s for s in old_segments if s.seg_id == rec["seg"])
                merged.docs[doc_id] = win.docs[doc_id]

            write_segment(self.root, merged)
            new_active = {
                doc_id: {**rec, "seg": merged.seg_id}
                for doc_id, rec in m.active.items()
            }
            new_manifest = Manifest(
                segments=[merged.seg_id],
                active=new_active,
                next_seg_id=m.next_seg_id + 1,
                next_rev=dict(m.next_rev),
                stopwords=list(m.stopwords),
                generation=m.generation + 1,
            )
            # 新段落盘之后才切换清单：这一步之前没有任何查询能看到半合并结果
            write_manifest(self.root, new_manifest)
            self._manifest = new_manifest

            removed, retained = self._reap_locked(new_manifest)
            return {
                "merged": old_ids,
                "new_segment": merged.seg_id,
                "removed_files": removed,
                "retained_for_readers": retained,
            }

    def reap(self) -> dict:
        """显式回收清单之外、且无在读视图保护的孤儿段文件。"""
        with self._thread_lock, self._FileLock(self._lock_path):
            m = self._reload()
            removed, retained = self._reap_locked(m)
            return {"removed_files": removed, "retained_for_readers": retained}

    def _reap_locked(self, manifest: Manifest):
        live_pids = live_lease_pids(self.root)
        local_leases = {os.path.join(self.root, LEASE_DIR, os.path.basename(p))
                        for p in _LOCAL_LEASES}
        # 同进程内未关闭的 snapshot 也登记在案：按租约路径精确保护
        for lease in local_leases:
            if os.path.exists(lease):
                try:
                    live_pids.add(int(os.path.basename(lease).split("_")[1]))
                except (IndexError, ValueError):
                    pass
        wanted = set(manifest.segments)
        seg_dir = os.path.join(self.root, "segments")
        removed, retained = [], []
        if not os.path.isdir(seg_dir):
            return removed, retained
        for name in os.listdir(seg_dir):
            if not (name.startswith("seg_") and name.endswith(".json")):
                continue
            sid = int(name[len("seg_"):-len(".json")])
            if sid in wanted:
                continue
            # 存在在册读视图时保留旧段，等视图关闭后由下一次 reap 回收
            if live_pids:
                retained.append(name)
                continue
            os.unlink(os.path.join(seg_dir, name))
            removed.append(name)
        return removed, retained


# ---------------------------------------------------------------- 读视图


class Snapshot:
    """固定读视图：清单快照 + 段内存副本，生命周期内不受后续合并影响。"""

    def __init__(self, root: str, manifest: Manifest, segments: list[Segment],
                 analyzer: Analyzer, fields: tuple[str, ...], lease: str):
        self.root = root
        self.manifest = manifest
        self.segments = segments
        self.analyzer = analyzer
        self.fields = tuple(fields)
        self._lease = lease
        self._doc_cache: dict[str, dict | None] = {}
        self._closed = False

    # ---- 生命周期 ----

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        _LOCAL_LEASES.discard(self._lease)
        release_lease(self._lease)

    def __enter__(self) -> "Snapshot":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    # ---- 校验与公共工具 ----

    def _check_field(self, field: str) -> None:
        if field not in self.fields:
            raise QueryError(
                f"未知 field {field!r}，本索引只接受 {list(self.fields)}")

    def _check_term(self, term) -> str:
        if not isinstance(term, str) or not term:
            raise QueryError(f"非法 term: {term!r}")
        norm = term.casefold()
        if self.analyzer.is_stopword(norm):
            raise QueryError(f"停词 {term!r} 不允许作为查询词")
        return norm

    def active_doc_ids(self) -> list[str]:
        return sorted(d for d, r in self.manifest.active.items() if r["op"] == "put")

    def get_doc(self, doc_id: str) -> dict | None:
        """取清单指定版本的完整文档（原文，不经过任何改写）。"""
        if doc_id not in self._doc_cache:
            rec = self.manifest.active.get(doc_id)
            if rec is None or rec["op"] != "put":
                self._doc_cache[doc_id] = None
            else:
                seg = next(s for s in self.segments if s.seg_id == rec["seg"])
                self._doc_cache[doc_id] = seg.docs[doc_id]["doc"]
        return self._doc_cache[doc_id]

    def evidence(self, doc_id: str, field: str, value_no: int,
                 byte_start: int, byte_end: int) -> str:
        """按 posting 上的 UTF-8 半开字节区间切回原文字符串。"""
        doc = self.get_doc(doc_id)
        if doc is None:
            raise QueryError(f"文档 {doc_id} 不在当前活跃集合中")
        value = doc[field][value_no]
        return value.encode("utf-8")[byte_start:byte_end].decode("utf-8")

    def _live_occurrences(self, term: str, field: str) -> list[Occ]:
        """跨段收集某 term 的出现，并按清单做版本筛选与去重。"""
        out: list[Occ] = []
        seen: set[tuple] = set()
        for seg in self.segments:
            for p in seg.postings.get(term, ()):
                if p[P_FIELD] != field:
                    continue
                rec = self.manifest.active.get(p[P_DOC])
                if rec is None or rec["op"] != "put" \
                        or rec["revision"] != p[P_REV]:
                    continue  # 旧 revision / 墓碑：显式过滤，绝不参与命中
                key = (p[P_DOC], p[P_VALUE], p[P_POS], p[P_BSTART], p[P_BEND])
                if key in seen:
                    continue
                seen.add(key)
                out.append(Occ(p[P_DOC], p[P_REV], p[P_FIELD], p[P_VALUE],
                               p[P_POS], p[P_BSTART], p[P_BEND]))
        out.sort(key=lambda o: (o.doc_id, o.value_no, o.position, o.byte_start))
        return out

    # ---- 短语查询（词序/距离，同值边界，重复 term 不共用出现）----

    def phrase(self, field: str, terms: list[str], max_gap: int) -> dict:
        self._check_field(field)
        if isinstance(max_gap, bool) or not isinstance(max_gap, int) \
                or max_gap < 0:
            raise QueryError(f"max_gap 必须是非负整数，收到 {max_gap!r}")
        if not isinstance(terms, list) or len(terms) < 2:
            raise QueryError("短语查询至少需要两个 term")
        norm_terms = [self._check_term(t) for t in terms]

        per_term = [self._live_occurrences(t, field) for t in norm_terms]
        # 按 (doc_id, value_no) 分组：不同数组元素绝不进入同一组，
        # 因此即使两个值里都有 position=0 也拼不出跨值短语。
        grouped: list[dict[tuple, list[Occ]]] = []
        common: set[tuple] | None = None
        for occs in per_term:
            g: dict[tuple, list[Occ]] = {}
            for o in occs:
                g.setdefault((o.doc_id, o.value_no), []).append(o)
            grouped.append(g)
            keys = set(g)
            common = keys if common is None else (common & keys)

        hits: list[dict] = []
        limit = max_gap + 1
        for key in sorted(common or ()):
            doc_id, value_no = key
            chain: list[Occ] = []
            used_pos: set[int] = set()
            used_span: set[tuple] = set()

            def dfs(ti: int) -> None:
                if ti == len(norm_terms):
                    hits.append(self._phrase_hit(field, key, list(chain)))
                    return
                prev = chain[-1] if chain else None
                for occ in grouped[ti][key]:
                    if prev is not None:
                        gap = occ.position - prev.position
                        if gap < 1:
                            continue  # 同位置或倒退：不满足词序
                        if gap > limit:
                            break  # 已按 position 排序，后面的距离只会更大
                    if occ.position in used_pos or \
                            (occ.byte_start, occ.byte_end) in used_span:
                        continue  # 同一出现不能匹配查询里两个（重复）词
                    chain.append(occ)
                    used_pos.add(occ.position)
                    used_span.add((occ.byte_start, occ.byte_end))
                    dfs(ti + 1)
                    used_span.discard((occ.byte_start, occ.byte_end))
                    used_pos.discard(occ.position)
                    chain.pop()

            dfs(0)

        hits.sort(key=lambda h: (h["doc_id"], h["value_no"],
                                 tuple(h["positions"]), tuple(h["byte_spans"])))
        return {
            "type": "phrase", "field": field, "terms": norm_terms,
            "max_gap": max_gap, "hit_count": len(hits), "hits": hits,
        }

    def _phrase_hit(self, field: str, key: tuple, chain: list[Occ]) -> dict:
        doc_id, value_no = key
        return {
            "doc_id": doc_id,
            "revision": chain[0].revision,
            "field": field,
            "value_no": value_no,
            "positions": [o.position for o in chain],
            "byte_spans": [[o.byte_start, o.byte_end] for o in chain],
            "evidence": [
                self.evidence(doc_id, field, value_no, o.byte_start, o.byte_end)
                for o in chain
            ],
        }

    # ---- 单词与 AND/OR/NOT 文档集合查询 ----

    def word(self, field: str, term: str) -> dict:
        self._check_field(field)
        norm = self._check_term(term)
        occs = self._live_occurrences(norm, field)
        return {
            "type": "word", "field": field, "term": norm,
            "docs": sorted({o.doc_id for o in occs}),
            "occurrences": [self._occ_obj(o) for o in occs],
        }

    def _occ_obj(self, o: Occ) -> dict:
        return {
            "doc_id": o.doc_id, "revision": o.revision, "value_no": o.value_no,
            "position": o.position,
            "byte_span": [o.byte_start, o.byte_end],
            "evidence": self.evidence(
                o.doc_id, o.field, o.value_no, o.byte_start, o.byte_end),
        }

    def set_query(self, field: str, node: dict) -> dict:
        self._check_field(field)

        def eval_node(n: dict) -> set[str]:
            if not isinstance(n, dict):
                raise QueryError(f"非法集合查询节点: {n!r}")
            if "word" in n or "term" in n:
                term = n.get("word", n.get("term"))
                return set(self.word(field, term)["docs"])
            if "and" in n:
                sets = [eval_node(c) for c in n["and"]]
                if not sets:
                    raise QueryError("AND 至少需要一个子查询")
                return set.intersection(*sets)
            if "or" in n:
                sets = [eval_node(c) for c in n["or"]]
                if not sets:
                    raise QueryError("OR 至少需要一个子查询")
                return set.union(*sets)
            if "not" in n:
                # NOT 的全集是当前活跃文档（put），不是历史 posting 里出现过的 doc
                return set(self.active_doc_ids()) - eval_node(n["not"])
            raise QueryError(f"无法识别的集合查询节点: {n!r}")

        docs = eval_node(node)
        return {"type": "set", "field": field, "node": node,
                "docs": sorted(docs)}

    def run(self, query: dict) -> dict:
        if not isinstance(query, dict):
            raise QueryError("查询必须是 JSON 对象")
        if "terms" in query:
            return self.phrase(query["field"], query["terms"],
                               query.get("max_gap", 0))
        if "word" in query or "term" in query:
            return self.word(query["field"],
                             query.get("word", query.get("term")))
        if any(k in query for k in ("and", "or", "not")):
            return self.set_query(query["field"], query)
        raise QueryError(f"无法识别的查询形态: {query!r}")
