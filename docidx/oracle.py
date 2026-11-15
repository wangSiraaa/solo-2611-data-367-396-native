"""Oracle：独立的小型逐文档扫描器，完全不查倒排索引。

与引擎共用的只有最底层“词 = 连续 isalpha/isdigit 码点、casefold、
UTF-8 字节坐标”这条题面规则；短语匹配、值边界、距离、重复词、
集合运算全部在这里独立重写一遍，用于与索引结果逐条对账，
比较到每个位置组合与字节证据，而不是只比命中总数。
"""

from __future__ import annotations

from dataclasses import dataclass

from .analyzer import tokenize
from .engine import QueryError


@dataclass(frozen=True)
class Cell:
    """一个 (文档, 字段, 数组元素) 单元的独立扫描结果。"""

    doc_id: str
    field: str
    value_no: int
    indexed: list  # 去掉停词后的 token（position 不重排）
    raw_value: str


class OracleScanner:
    def __init__(self, analyzer, fields):
        self.analyzer = analyzer
        self.fields = tuple(fields)

    def check(self, field: str, terms, phrase: bool):
        if field not in self.fields:
            raise QueryError(f"未知 field {field!r}")
        if not isinstance(terms, list) or not terms:
            raise QueryError("查询 term 列表为空")
        if phrase and len(terms) < 2:
            raise QueryError("短语查询至少两个 term")
        norm = [t.casefold() for t in terms]
        for t in norm:
            if self.analyzer.is_stopword(t):
                raise QueryError(f"停词 {t!r} 不允许作为查询词")
        return norm

    def cells(self, active_docs: dict[str, dict], field: str):
        """逐文档、逐数组元素展开——oracle 的入口就是原始文档集合。"""
        for doc_id in sorted(active_docs):
            doc = active_docs[doc_id]
            if doc is None:
                continue
            for value_no, value in enumerate(doc.get(field, [])):
                toks = [t for t in tokenize(value)
                        if t.term not in self.analyzer.stopwords]
                yield Cell(doc_id, field, value_no, toks, value)


def _byte_evidence(cell: Cell, token) -> str:
    raw = cell.raw_value.encode("utf-8")
    return raw[token.byte_start:token.byte_end].decode("utf-8")


def oracle_phrase(scanner: OracleScanner, active_docs: dict[str, dict],
                  field: str, terms: list[str], max_gap: int) -> list[dict]:
    """独立短语实现：在每个值单元上做有序回溯。"""
    if isinstance(max_gap, bool) or not isinstance(max_gap, int) or max_gap < 0:
        raise QueryError(f"max_gap 必须是非负整数，收到 {max_gap!r}")
    want = scanner.check(field, terms, phrase=True)
    limit = max_gap + 1
    result: list[dict] = []

    for cell in scanner.cells(active_docs, field):
        toks = cell.indexed
        chosen: list = []
        occupied: set[int] = set()  # token 序号去重，保证同一出现不被复用

        def backtrack(qi: int, start: int) -> None:
            if qi == len(want):
                result.append({
                    "doc_id": cell.doc_id,
                    "value_no": cell.value_no,
                    "positions": [t.position for t in chosen],
                    "byte_spans": [[t.byte_start, t.byte_end] for t in chosen],
                    "evidence": [_byte_evidence(cell, t) for t in chosen],
                })
                return
            for idx in range(start, len(toks)):
                if idx in occupied or toks[idx].term != want[qi]:
                    continue
                if chosen:
                    gap = toks[idx].position - chosen[-1].position
                    if gap < 1:
                        continue
                    if gap > limit:
                        break  # 位置单调，后面更不可能
                chosen.append(toks[idx])
                occupied.add(idx)
                backtrack(qi + 1, idx + 1)
                occupied.discard(idx)
                chosen.pop()

        backtrack(0, 0)
    return result


def oracle_word_docs(scanner: OracleScanner,
                     active_docs: dict[str, dict], field: str, term: str) -> set[str]:
    want = scanner.check(field, [term], phrase=False)[0]
    return {cell.doc_id for cell in scanner.cells(active_docs, field)
            if any(t.term == want for t in cell.indexed)}


def oracle_set_query(scanner: OracleScanner,
                     active_docs: dict[str, dict], query: dict) -> set[str]:
    field = query["field"]
    universe = set(active_docs.keys())

    def ev(node: dict) -> set[str]:
        if "word" in node or "term" in node:
            term = node.get("word", node.get("term"))
            return oracle_word_docs(scanner, active_docs, field, term)
        if "and" in node:
            subs = [ev(c) for c in node["and"]]
            if not subs:
                raise QueryError("AND 至少需要一个子查询")
            return set.intersection(*subs)
        if "or" in node:
            subs = [ev(c) for c in node["or"]]
            if not subs:
                raise QueryError("OR 至少需要一个子查询")
            return set.union(*subs)
        if "not" in node:
            # 全集 = 当前活跃文档，与历史 posting 无关
            return universe - ev(node["not"])
        raise QueryError(f"无法识别的集合查询节点: {node!r}")

    if field not in scanner.fields:
        raise QueryError(f"未知 field {field!r}")
    return ev(query)
