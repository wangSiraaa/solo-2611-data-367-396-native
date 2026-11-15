"""release 阶段：把计划执行成可独立打开的 release.sqlite。

发布决定（release decision）在一个持锁窗口内完成：
  1. 只读打开源并取内容指纹 -> 源快照被固定；
  2. 载入 policy 并取其规范指纹 -> policy 被固定；
  3. plan 全量静态校验；
  4. vault IMMEDIATE 事务：建 release 记录、按需分配/复用令牌；
  5. 生成临时成品库，切换为 intermediate，原子 rename 到输出路径；
  6. 写私有清单（含原值，0600）与公开回执（不含原值）；
  7. 标记 complete 并提交。
任一步失败，事务回滚；崩溃后由 recover 把遗留 intermediate 标 abandoned，
已被其他完整发布使用的映射不受影响，预留但未用的令牌允许保留。
"""

from dataclasses import dataclass
import os
import re
import sqlite3
import tempfile
import uuid
from pathlib import Path

from .catalog import open_source, load_catalog
from .errors import CapacityExhausted, ReleaseInterrupted
from .planner import build_plan
from .policy import load_policy, ACTION_DROP, ACTION_KEEP
from .util import (
    atomic_write,
    canonical_json,
    sha256_file,
    source_snapshot_fingerprint,
)
from .vault import (
    TokenVault,
    STATE_RESERVED,
    STATE_INTERMEDIATE,
    STATE_COMPLETE,
)


@dataclass
class ReleaseResult:
    release_id: str
    output_db: Path
    receipt_path: Path
    manifest_path: Path
    receipt: dict
    new_allocations: int
    reused_allocations: int


def token_regex(spec):
    return re.compile(
        "^" + re.escape(spec.prefix)
        + "[" + re.escape(spec.alphabet) + "]{" + str(spec.width) + "}$"
    )


def _build_output(conn_out, plan):
    for tname in plan.creation_order:
        tp = plan.table_plans[tname]
        conn_out.execute(tp.output_ddl())
    # 非主键的 UNIQUE 约束以唯一索引重建（主键索引随建表生成）。
    for tname in plan.creation_order:
        tp = plan.table_plans[tname]
        surviving = {c.name for c in tp.surviving_columns}
        for idx, seq in zip(tp.unique_indexes, range(len(tp.unique_indexes))):
            if idx.origin == "pk":
                continue
            if not all(c in surviving for c in idx.columns):
                continue  # R012 已保证不会发生，防御性跳过
            conn_out.execute(
                f'CREATE UNIQUE INDEX "idx_{tname}_{seq}" '
                f'ON "{tname}" (' + ",".join(f'"{c}"' for c in idx.columns) + ")"
            )


def _row_values(conn_src, table, columns):
    col_sql = ",".join(f'"{c}"' for c in columns)
    return conn_src.execute(
        f'SELECT {col_sql} FROM "{table}"'
    ).fetchall()


def run_release(source_path, policy_path, vault: TokenVault,
                output_dir, release_id=None):
    source_path = Path(source_path)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    release_id = release_id or uuid.uuid4().hex

    # --- 固定源快照与 policy（同一发布决定） ---
    conn_src = open_source(source_path)
    try:
        source_fp = source_snapshot_fingerprint(conn_src)
        catalog = load_catalog(conn_src)
        policy = load_policy(policy_path)
        policy_fp = canonical_json(policy.raw).hex()
        plan = build_plan(catalog, policy, conn_src)
    finally:
        conn_src.close()

    result = _execute(
        source_path, output_dir, release_id, source_fp,
        policy, policy_fp, plan, vault,
    )
    return result


def _execute(source_path, output_dir, release_id, source_fp,
             policy, policy_fp, plan, vault):
    output_db = output_dir / "release.sqlite"
    receipt_path = output_dir / "receipt.json"

    # 私有清单数据在锁内收集，落盘到 vault 专用目录。
    manifest = {
        "release_id": release_id,
        "policy_id": policy.policy_id,
        "policy_fingerprint": policy_fp,
        "source_fingerprint": source_fp,
        "domains": {},   # domain -> [{"value": ..., "token": ..., "reused": bool}]
    }
    counters = {"new": 0, "reused": 0}

    with vault.locked() as conn_v:
        conn_v.execute("BEGIN IMMEDIATE")
        tmp_name = None
        try:
            # reserved 阶段即持久化最终输出路径，崩溃后 recover 可据此清理孤儿。
            vault.create_release(
                conn_v, release_id, policy.policy_id, policy_fp,
                source_fp, output_db_path=output_db,
            )

            # 先在临时库完整建库、灌数；映射在此事务内分配。
            tmp_fd, tmp_name = tempfile.mkstemp(
                prefix="release.", suffix=".sqlite", dir=str(output_dir)
            )
            os.close(tmp_fd)
            os.unlink(tmp_name)
            conn_out = sqlite3.connect(tmp_name)
            conn_out.execute("PRAGMA foreign_keys=ON")
            try:
                _build_output(conn_out, plan)
                cache = {}  # (domain, value) -> token，进程内同值只查一次

                def resolve(domain, value, spec):
                    if value is None:
                        return None
                    key = (domain, value)
                    if key in cache:
                        return cache[key]
                    alloc = vault.allocate(
                        conn_v, domain, value, spec, release_id
                    )
                    cache[key] = alloc.token
                    # 同一发布内每个去重值只登记一次（含复用历史映射的值）。
                    counters["new" if not alloc.reused else "reused"] += 1
                    manifest["domains"].setdefault(domain, []).append({
                        "value": value,
                        "token": alloc.token,
                        "reused": alloc.reused,
                    })
                    return alloc.token

                conn_src = open_source(source_path)
                try:
                    for tname in plan.creation_order:
                        tp = plan.table_plans[tname]
                        survivors = tp.surviving_columns
                        src_cols = [c.name for c in survivors]
                        rows = _row_values(conn_src, tname, src_cols)
                        out_rows = []
                        for row in rows:
                            out = []
                            for cp, val in zip(survivors, row):
                                if cp.action == ACTION_DROP:
                                    continue
                                if cp.domain is not None:
                                    out.append(
                                        resolve(cp.domain, val,
                                                plan.domain_specs[cp.domain])
                                    )
                                else:  # keep
                                    out.append(val)
                            out_rows.append(tuple(out))
                        placeholders = ",".join("?" for _ in survivors)
                        cols_sql = ",".join(f'"{c.name}"' for c in survivors)
                        conn_out.executemany(
                            f'INSERT INTO "{tname}" ({cols_sql}) '
                            f"VALUES ({placeholders})",
                            out_rows,
                        )
                finally:
                    conn_src.close()

                # 成品侧自检：任何约束失败立即中止，外部永远看不到半成品。
                failures = list(conn_out.execute("PRAGMA foreign_key_check"))
                if failures:
                    raise ReleaseInterrupted(
                        f"成品外键检查失败: {failures[0][:2]}"
                    )
                conn_out.commit()
                integrity = conn_out.execute(
                    "PRAGMA integrity_check"
                ).fetchone()[0]
                if integrity != "ok":
                    raise ReleaseInterrupted(f"成品完整性检查失败: {integrity}")
            finally:
                conn_out.close()

            vault.mark_intermediate(conn_v, release_id, output_db)

            # 原子替换：读取方只看到旧文件或完整新文件。
            os.replace(tmp_name, output_db)
            os.chmod(output_db, 0o644)
            output_sha = sha256_file(output_db)

            # 私有清单（含原值，仅 vault 目录，0600）。
            manifest_path = vault.write_manifest(release_id, manifest)

            # 公开回执：只有指纹、计数、令牌格式与代号，绝无原值。
            receipt = {
                "release_id": release_id,
                "policy_id": policy.policy_id,
                "policy_fingerprint": policy_fp,
                "source_fingerprint": source_fp,
                "output_file": output_db.name,
                "output_sha256": output_sha,
                "tables": {
                    tname: {
                        "columns": [
                            c.name for c in
                            plan.table_plans[tname].surviving_columns
                        ],
                        "tokenize_columns": [
                            {"name": c.name, "domain": c.domain}
                            for c in plan.table_plans[tname].surviving_columns
                            if c.domain is not None
                        ],
                    }
                    for tname in plan.creation_order
                },
                "domains": {
                    d: {
                        "prefix": spec.prefix,
                        "alphabet": spec.alphabet,
                        "width": spec.width,
                        "token_length": spec.token_length,
                        "capacity": spec.capacity,
                        "mappings_in_release": len(
                            manifest["domains"].get(d, [])
                        ),
                    }
                    for d, spec in sorted(plan.domain_specs.items())
                },
                "allocations": {
                    "new": counters["new"],
                    "reused": counters["reused"],
                },
            }
            atomic_write(receipt_path, canonical_json(receipt))

            vault.mark_complete(conn_v, release_id, output_sha)
            conn_v.execute("COMMIT")
        except BaseException:
            conn_v.execute("ROLLBACK")
            try:
                if os.path.exists(tmp_name):
                    os.unlink(tmp_name)
            except OSError:
                pass
            raise

    return ReleaseResult(
        release_id=release_id,
        output_db=output_db,
        receipt_path=receipt_path,
        manifest_path=manifest_path,
        receipt=receipt,
        new_allocations=counters["new"],
        reused_allocations=counters["reused"],
    )
