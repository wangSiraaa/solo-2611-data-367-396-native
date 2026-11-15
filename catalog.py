"""查询目录：只接受 queries.json 中登记的参数化只读 SQL，不接受任意 SQL。

规范查询键 = 源快照 SHA256 + query_id + 带类型参数。
INTEGER 的 1 与 TEXT 的 '1' 使用不同类型标记，不会误合并。
"""
from __future__ import annotations

import json
from pathlib import Path

# 目录允许的 SQLite 声明类型 -> Python 校验类型
TYPE_MAP = {"INTEGER": int, "TEXT": str}

# 只读守卫：SQL 归一化后禁止出现这些写/附加类关键字
_FORBIDDEN = (
    "INSERT", "UPDATE", "DELETE", "REPLACE", "DROP", "CREATE", "ALTER",
    "ATTACH", "DETACH", "PRAGMA", "VACUUM", "REINDEX", "SAVEPOINT",
    "BEGIN", "COMMIT", "ROLLBACK",
)


class CatalogError(ValueError):
    """query_id 未登记、参数个数/类型不符或 SQL 非只读时抛出。"""


class Catalog:
    def __init__(self, path: str | Path):
        data = json.loads(Path(path).read_text(encoding="utf-8"))
        self.queries: dict[str, dict] = {}
        for qid, spec in data.items():
            sql = spec["sql"]
            norm = " ".join(sql.upper().split())
            if not norm.startswith("SELECT"):
                raise CatalogError(f"{qid}: 仅允许只读 SELECT")
            if any(kw in norm.split() for kw in _FORBIDDEN) or ";" in norm.rstrip(";"):
                raise CatalogError(f"{qid}: SQL 含禁止的写操作或多语句")
            types = tuple(spec["parameter_types"])
            if not types or any(t not in TYPE_MAP for t in types):
                raise CatalogError(f"{qid}: parameter_types 非法")
            self.queries[qid] = {
                "sql": sql,
                "types": types,
                "max_rows": int(spec["max_rows"]),
                "max_encoded_bytes": int(spec["max_encoded_bytes"]),
            }

    def validate(self, qid: str, params) -> dict:
        """校验 query_id 与每个实参的声明类型，返回该查询规格。"""
        if qid not in self.queries:
            raise CatalogError(f"未登记的 query_id: {qid!r}（不接受任意 SQL）")
        spec = self.queries[qid]
        params = tuple(params)
        if len(params) != len(spec["types"]):
            raise CatalogError(
                f"{qid}: 参数个数 {len(params)} 与声明 {len(spec['types'])} 不一致"
            )
        for value, declared in zip(params, spec["types"]):
            if declared == "INTEGER":
                # bool 是 int 的子类，显式拒绝，避免类型语义被绕过
                if not isinstance(value, int) or isinstance(value, bool):
                    raise CatalogError(f"{qid}: 参数 {value!r} 声明 INTEGER，实参不是 int")
            elif not isinstance(value, str):
                raise CatalogError(f"{qid}: 参数 {value!r} 声明 TEXT，实参不是 str")
        return spec

    @staticmethod
    def canonical_key(snapshot_digest: str, qid: str, params, types) -> str:
        """类型化规范键：长度前缀编码，整数 1 与文本 '1' 必然不同。"""
        tokens = [f"snap:{len(snapshot_digest)}:{snapshot_digest}", f"q:{len(qid)}:{qid}"]
        for value, declared in zip(tuple(params), types):
            if declared == "INTEGER":
                # 不做 int(value) 隐式转换：validate 已保证这里是 int；
                # 类型标记 i:/s: 本身就是 1 与 '1' 不被合并的一部分。
                payload = str(value)
                tokens.append(f"i:{len(payload)}:{payload}")
            else:
                payload = str(value)
                tokens.append(f"s:{len(payload)}:{payload}")
        return "|".join(tokens)
