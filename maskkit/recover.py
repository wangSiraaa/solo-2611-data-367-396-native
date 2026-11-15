"""recover：依据 vault 状态机与磁盘文件恢复中断发布。

规则：
  - reserved：release 记录已建但未开始写成品 -> 直接 abandoned；
  - intermediate 且磁盘成品缺失/哈希不符 -> abandoned（失败包不得标 complete）；
  - intermediate 且成品存在且哈希匹配、私有清单齐全 -> 补标 complete；
  - complete：核对成品哈希，发现成品丢失则报告但不自动改状态。
预留但未被任何发布使用的 token 允许保留在 token_mapping 中，不做回收。
"""

from dataclasses import dataclass
from pathlib import Path

from .util import sha256_file
from .vault import (
    TokenVault,
    STATE_RESERVED,
    STATE_INTERMEDIATE,
    STATE_COMPLETE,
    STATE_ABANDONED,
)


@dataclass
class RecoveryAction:
    release_id: str
    old_state: str
    new_state: str
    reason: str


def recover(vault: TokenVault):
    actions = []
    with vault.locked() as conn:
        conn.execute("BEGIN IMMEDIATE")
        try:
            for row in vault.list_releases(conn):
                rid = row["release_id"]
                state = row["state"]
                manifest_path = vault.manifest_dir / f"{rid}.json"
                out = Path(row["output_db_path"]) if row["output_db_path"] else None

                if state == STATE_RESERVED:
                    # 即便成品已被原子放到最终路径，记录仍停在 reserved，
                    # 说明提交未完成，按失败包清理（映射允许保留）。
                    if out is not None and out.exists():
                        out.unlink()
                    vault.mark_abandoned(conn, rid)
                    actions.append(RecoveryAction(
                        rid, state, STATE_ABANDONED,
                        "R1: 预留后未完成提交，清理孤儿成品",
                    ))
                elif state == STATE_INTERMEDIATE:
                    healthy = (
                        out is not None
                        and out.exists()
                        and sha256_file(out) == row["output_sha256"]
                        and manifest_path.exists()
                    )
                    if healthy:
                        # 崩溃发生在最后一次状态更新附近，成品其实完整。
                        actions.append(RecoveryAction(
                            rid, state, STATE_COMPLETE,
                            "R2: 成品与清单完整，补登完成态",
                        ))
                        vault.mark_complete(conn, rid, row["output_sha256"])
                    else:
                        vault.mark_abandoned(conn, rid)
                        if out is not None and out.exists():
                            out.unlink()
                        actions.append(RecoveryAction(
                            rid, state, STATE_ABANDONED,
                            "R3: 成品缺失或哈希不符，废弃失败包",
                        ))
                elif state == STATE_COMPLETE:
                    if out is None or not out.exists():
                        actions.append(RecoveryAction(
                            rid, state, state,
                            "R4: 完整发布的成品文件已在外部丢失，映射保留",
                        ))
            conn.execute("COMMIT")
        except BaseException:
            conn.execute("ROLLBACK")
            raise
    return actions
