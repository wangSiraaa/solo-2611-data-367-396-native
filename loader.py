#!/usr/bin/env python3
"""单机 CDC 装载器：只解析题定 JSONL 协议（control / p0 / p1 + end 帧）。

子命令：
  ingest   读入一路 JSONL（文件或 stdin），逐帧落盘到状态库，片段去重/损坏检测
  work     调度器：组装完整事务、解依赖、查写冲突、在单 SQLite 事务内物化
  query    导出目标表 customers / orders 当前可见行
  inspect  导出流结束位、事务状态机、违例、审计事件、回执、故障开关
  arm-fault 注入两类故障：提交前失败 / 提交后丢确认

设计要点见 DESIGN.md。所有状态均在 SQLite 内，进程重开不丢已收片段，
已提交事务不会重放业务写入，重放返回原回执。
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------- 常量

SOURCES = ("p0", "p1", "control")
PARTITIONS = ("p0", "p1")
OPS = ("put_customer", "put_order", "delete_order")
TERMINALS = ("commit", "abort")
INT64_MAX = 2**63 - 1
MAX_PARTS_PER_PARTITION = 200

# 终态：不再参与调度
TERMINAL_STATES = {
    "committed", "aborted", "failed", "rejected", "corrupt",
    "blocked", "incomplete", "missing_dependency",
}


class LostAck(Exception):
    """COMMIT 已持久化，但确认在返回前丢失（故障注入）。"""

    def __init__(self, receipt: dict):
        self.receipt = receipt
        super().__init__("ack_lost_after_commit")

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS customers(
    id   TEXT PRIMARY KEY,
    name TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS orders(
    id          TEXT PRIMARY KEY,
    customer_id TEXT NOT NULL,
    amount      INTEGER NOT NULL CHECK(amount >= 0),
    FOREIGN KEY(customer_id) REFERENCES customers(id)
);

CREATE TABLE IF NOT EXISTS cdc_meta(
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS cdc_stream(
    source  TEXT PRIMARY KEY,
    ended   INTEGER NOT NULL DEFAULT 0,
    records INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS cdc_txn(
    txn          TEXT PRIMARY KEY,
    parts_json   TEXT,
    deps_json    TEXT,
    terminal     TEXT,
    state        TEXT NOT NULL DEFAULT 'receiving',
    reason       TEXT,
    control_seen INTEGER NOT NULL DEFAULT 0,
    corrupt      INTEGER NOT NULL DEFAULT 0,
    updated_at   TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS cdc_fragment(
    txn          TEXT NOT NULL,
    partition    TEXT NOT NULL,
    part_no      INTEGER NOT NULL,
    op_no        INTEGER NOT NULL,
    payload_hash TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    seq          INTEGER NOT NULL,
    PRIMARY KEY(txn, partition, part_no)
) WITHOUT ROWID;
-- op_no 全事务唯一的兜底约束；ingest 会先显式诊断 duplicate_op_no
CREATE UNIQUE INDEX IF NOT EXISTS ux_frag_opno ON cdc_fragment(txn, op_no);

CREATE TABLE IF NOT EXISTS cdc_write_set(
    txn  TEXT NOT NULL,
    kind TEXT NOT NULL,          -- customer / order
    id   TEXT NOT NULL,
    PRIMARY KEY(txn, kind, id)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS ix_ws_key ON cdc_write_set(kind, id);

CREATE TABLE IF NOT EXISTS cdc_receipt(
    txn            TEXT PRIMARY KEY,
    receipt        TEXT NOT NULL,
    committed_at   TEXT NOT NULL,
    ops            INTEGER NOT NULL,
    receipt_json   TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS cdc_violation(
    id     INTEGER PRIMARY KEY AUTOINCREMENT,
    at     TEXT NOT NULL,
    txn    TEXT,
    code   TEXT NOT NULL,
    detail TEXT NOT NULL,
    source TEXT,
    seq    INTEGER
);
CREATE TABLE IF NOT EXISTS cdc_event(
    id     INTEGER PRIMARY KEY AUTOINCREMENT,
    at     TEXT NOT NULL,
    txn    TEXT,
    event  TEXT NOT NULL,
    detail TEXT
);
CREATE TABLE IF NOT EXISTS cdc_fault(
    name  TEXT PRIMARY KEY,      -- fail_before_commit / lose_ack_after_commit
    txn   TEXT,
    armed INTEGER NOT NULL DEFAULT 1
);
"""


# ---------------------------------------------------------------- 基础工具

def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def connect(db_path: str) -> sqlite3.Connection:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(SCHEMA)
    for s in SOURCES:
        conn.execute(
            "INSERT OR IGNORE INTO cdc_stream(source, ended, records) VALUES(?,0,0)",
            (s,),
        )
    conn.commit()
    return conn


def close(conn: sqlite3.Connection) -> None:
    # 干净关闭前把 WAL 合并回主库，交付的 .db 文件自身即完整产物
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    conn.close()


def payload_hash(rec: dict) -> str:
    blob = json.dumps(rec, sort_keys=True, ensure_ascii=False).encode()
    return hashlib.sha256(blob).hexdigest()


def log_violation(conn, code, detail, txn=None, source=None, seq=None) -> None:
    conn.execute(
        "INSERT INTO cdc_violation(at,txn,code,detail,source,seq) "
        "VALUES(?,?,?,?,?,?)",
        (now(), txn, code, detail, source, seq),
    )


def log_event(conn, event, txn=None, detail=None) -> None:
    conn.execute(
        "INSERT INTO cdc_event(at,txn,event,detail) VALUES(?,?,?,?)",
        (now(), txn, event, detail),
    )


def ensure_txn(conn, txn: str) -> None:
    conn.execute(
        "INSERT OR IGNORE INTO cdc_txn(txn,state,updated_at) VALUES(?, 'receiving', ?)",
        (txn, now()),
    )


def set_state(conn, txn: str, state: str, reason: str | None = None) -> None:
    conn.execute(
        "UPDATE cdc_txn SET state=?, reason=?, updated_at=? WHERE txn=?",
        (state, reason, now(), txn),
    )


def is_plain_int(v) -> bool:
    return isinstance(v, int) and not isinstance(v, bool)


# ---------------------------------------------------------------- ingest

def validate_fragment_shape(rec: dict) -> str | None:
    """结构/类型级校验；语义约束（非负、int64、外键）留到提交前。"""
    if not isinstance(rec.get("txn"), str) or not rec["txn"]:
        return "bad_txn"
    if rec.get("partition") not in PARTITIONS:
        return "bad_partition"
    if not is_plain_int(rec.get("part_no")) or rec["part_no"] < 0:
        return "bad_part_no"
    if not is_plain_int(rec.get("op_no")) or rec["op_no"] < 0:
        return "bad_op_no"
    op = rec.get("op")
    if op not in OPS:
        return "unknown_op"
    if not isinstance(rec.get("id"), str) or not rec["id"]:
        return "bad_id"
    if op == "put_customer":
        if not isinstance(rec.get("name"), str):
            return "bad_name"
    elif op == "put_order":
        if not isinstance(rec.get("customer"), str) or not rec["customer"]:
            return "bad_customer"
        if not is_plain_int(rec.get("amount")):
            return "bad_amount_type"
    # delete_order 只需 id
    return None


def validate_control_shape(rec: dict) -> str | None:
    if not isinstance(rec.get("txn"), str) or not rec["txn"]:
        return "bad_txn"
    parts = rec.get("parts")
    if not isinstance(parts, dict) or not parts:
        return "bad_parts"
    for p, n in parts.items():
        if p not in PARTITIONS or not is_plain_int(n) or not (1 <= n <= MAX_PARTS_PER_PARTITION):
            return "bad_parts"
    deps = rec.get("dependencies")
    if not isinstance(deps, list) or not all(isinstance(d, str) and d for d in deps):
        return "bad_dependencies"
    if not isinstance(rec.get("terminal"), str):
        return "bad_terminal"
    return None


def apply_control(conn, rec: dict, source: str, seq: int) -> str:
    """返回 applied / folded / violation。"""
    txn = rec["txn"]
    bad = validate_control_shape(rec)
    if bad:
        ensure_txn(conn, txn)
        log_violation(conn, bad, json.dumps(rec, ensure_ascii=False), txn, source, seq)
        conn.execute("UPDATE cdc_txn SET corrupt=1 WHERE txn=?", (txn,))
        return "violation"

    ensure_txn(conn, txn)
    row = conn.execute(
        "SELECT parts_json,deps_json,terminal,control_seen FROM cdc_txn WHERE txn=?",
        (txn,),
    ).fetchone()

    new_parts = json.dumps(rec["parts"], sort_keys=True)
    new_deps = json.dumps(rec["dependencies"])
    new_terminal = rec["terminal"]

    if row["control_seen"]:
        # 控制帧重放：全同折叠，任何差异都是损坏
        same = (row["parts_json"] == new_parts and row["deps_json"] == new_deps
                and row["terminal"] == new_terminal)
        if same:
            log_event(conn, "control_replay_folded", txn)
            return "folded"
        log_violation(conn, "control_conflict",
                      f"stored={row['parts_json']}/{row['deps_json']}/{row['terminal']} "
                      f"incoming={new_parts}/{new_deps}/{new_terminal}",
                      txn, source, seq)
        conn.execute("UPDATE cdc_txn SET corrupt=1 WHERE txn=?", (txn,))
        return "violation"

    if new_terminal not in TERMINALS:
        log_violation(conn, "unknown_terminal", f"terminal={new_terminal}", txn, source, seq)
        conn.execute("UPDATE cdc_txn SET corrupt=1 WHERE txn=?", (txn,))

    conn.execute(
        "UPDATE cdc_txn SET parts_json=?, deps_json=?, terminal=?, control_seen=1, "
        "updated_at=? WHERE txn=?",
        (new_parts, new_deps, new_terminal, now(), txn),
    )
    log_event(conn, "control_received", txn,
              f"parts={new_parts} deps={new_deps} terminal={new_terminal}")

    # 控制迟到：对已落盘片段回溯核对分区声明与 part_no 边界
    for f in conn.execute(
            "SELECT partition,part_no,payload_json FROM cdc_fragment WHERE txn=?",
            (txn,)):
        decl = rec["parts"]
        if f["partition"] not in decl:
            log_violation(conn, "undeclared_partition",
                          f"partition={f['partition']} not in {sorted(decl)}",
                          txn, source, seq)
            conn.execute("UPDATE cdc_txn SET corrupt=1 WHERE txn=?", (txn,))
        elif f["part_no"] >= decl[f["partition"]]:
            log_violation(conn, "part_no_out_of_range",
                          f"partition={f['partition']} part_no={f['part_no']} "
                          f"declared={decl[f['partition']]}",
                          txn, source, seq)
            conn.execute("UPDATE cdc_txn SET corrupt=1 WHERE txn=?", (txn,))
    return "applied"


def apply_fragment(conn: sqlite3.Connection, rec: dict, source: str, seq: int) -> str:
    """返回 applied / folded / violation。"""
    bad = validate_fragment_shape(rec)
    if bad:
        txn = rec.get("txn") if isinstance(rec.get("txn"), str) else None
        log_violation(conn, bad, json.dumps(rec, ensure_ascii=False), txn, source, seq)
        if txn:
            ensure_txn(conn, txn)
            conn.execute("UPDATE cdc_txn SET corrupt=1 WHERE txn=?", (txn,))
        return "violation"

    txn, partition, part_no, op_no = rec["txn"], rec["partition"], rec["part_no"], rec["op_no"]
    ensure_txn(conn, txn)
    h = payload_hash(rec)

    trow = conn.execute(
        "SELECT parts_json,corrupt FROM cdc_txn WHERE txn=?", (txn,)
    ).fetchone()
    if trow["parts_json"] is not None:
        decl = json.loads(trow["parts_json"])
        if partition not in decl:
            log_violation(conn, "undeclared_partition",
                          f"partition={partition} not in {sorted(decl)}",
                          txn, source, seq)
            conn.execute("UPDATE cdc_txn SET corrupt=1 WHERE txn=?", (txn,))
            return "violation"
        if part_no >= decl[partition]:
            log_violation(conn, "part_no_out_of_range",
                          f"partition={partition} part_no={part_no} declared={decl[partition]}",
                          txn, source, seq)
            conn.execute("UPDATE cdc_txn SET corrupt=1 WHERE txn=?", (txn,))
            return "violation"

    existing = conn.execute(
        "SELECT payload_hash FROM cdc_fragment WHERE txn=? AND partition=? AND part_no=?",
        (txn, partition, part_no),
    ).fetchone()
    if existing is not None:
        if existing["payload_hash"] == h:
            log_event(conn, "fragment_replay_folded", txn,
                      f"{partition}/{part_no}")
            return "folded"
        # 同身份不同内容：损坏，保留此前好片段（输入只读）
        log_violation(conn, "content_conflict",
                      f"identity=({txn},{partition},{part_no}) "
                      f"stored={existing['payload_hash'][:12]} incoming={h[:12]}",
                      txn, source, seq)
        conn.execute("UPDATE cdc_txn SET corrupt=1 WHERE txn=?", (txn,))
        return "violation"

    dup_op = conn.execute(
        "SELECT partition,part_no FROM cdc_fragment WHERE txn=? AND op_no=?",
        (txn, op_no),
    ).fetchone()
    if dup_op is not None:
        log_violation(conn, "duplicate_op_no",
                      f"op_no={op_no} already at {dup_op['partition']}/{dup_op['part_no']}, "
                      f"new fragment {partition}/{part_no}",
                      txn, source, seq)
        conn.execute("UPDATE cdc_txn SET corrupt=1 WHERE txn=?", (txn,))
        return "violation"

    conn.execute(
        "INSERT INTO cdc_fragment(txn,partition,part_no,op_no,payload_hash,payload_json,seq) "
        "VALUES(?,?,?,?,?,?,?)",
        (txn, partition, part_no, op_no, h, json.dumps(rec, ensure_ascii=False), seq),
    )
    kind = "customer" if rec["op"] == "put_customer" else "order"
    # 写集只记录“写入映像”（put_*）；delete_order 不产生写入，不参与同 key 冲突合同
    if rec["op"] != "delete_order":
        conn.execute(
            "INSERT OR IGNORE INTO cdc_write_set(txn,kind,id) VALUES(?,?,?)",
            (txn, kind, rec["id"]),
        )
    log_event(conn, "fragment_received", txn, f"{partition}/{part_no} op_no={op_no}")
    return "applied"


def cmd_ingest(args) -> int:
    conn = connect(args.db)
    try:
        if args.source not in SOURCES:
            raise SystemExit(f"unknown source {args.source!r}; expected one of {SOURCES}")
        raw = sys.stdin if args.file == "-" else open(args.file, encoding="utf-8")
        lines = [ln for ln in raw.read().splitlines() if ln.strip()]
        if args.file != "-":
            raw.close()

        # 解析可并行；写 SQLite 仍在主线程串行，每帧独立事务
        with ThreadPoolExecutor(max_workers=4) as pool:
            parsed = list(pool.map(lambda ln: (ln, json.loads(ln)), lines))

        stream = conn.execute(
            "SELECT ended,records FROM cdc_stream WHERE source=?", (args.source,)
        ).fetchone()
        applied = folded = violations = 0
        for offset, (ln, rec) in enumerate(parsed):
            seq = stream["records"] + offset
            if not isinstance(rec, dict):
                log_violation(conn, "bad_json", ln, source=args.source, seq=seq)
                violations += 1
                conn.commit()
                continue

            if rec.get("kind") == "end":
                if rec.get("source") != args.source:
                    log_violation(conn, "end_source_mismatch",
                                  f"end={rec.get('source')} stream={args.source}",
                                  source=args.source, seq=seq)
                    violations += 1
                elif stream["ended"]:
                    log_event(conn, "end_replay_folded", args.source)
                    folded += 1
                else:
                    conn.execute(
                        "UPDATE cdc_stream SET ended=1 WHERE source=?", (args.source,)
                    )
                    log_event(conn, "stream_ended", args.source)
                    stream = {"ended": 1, "records": stream["records"]}
                conn.commit()
                continue

            if stream["ended"]:
                log_violation(conn, "stream_closed",
                              "data frame after explicit end",
                              rec.get("txn") if isinstance(rec.get("txn"), str) else None,
                              args.source, seq)
                violations += 1
                conn.commit()
                continue

            # 帧形状必须与流匹配
            is_control_shape = "parts" in rec and "terminal" in rec
            is_fragment_shape = "partition" in rec and "part_no" in rec and "op" in rec

            if args.source == "control":
                if not is_control_shape or is_fragment_shape:
                    log_violation(conn, "stream_record_mismatch",
                                  "non-control frame on control stream",
                                  rec.get("txn"), args.source, seq)
                    status = "violation"
                else:
                    status = apply_control(conn, rec, args.source, seq)
            else:
                if not is_fragment_shape or is_control_shape:
                    log_violation(conn, "stream_record_mismatch",
                                  "non-fragment frame on data stream",
                                  rec.get("txn"), args.source, seq)
                    status = "violation"
                else:
                    status = apply_fragment(conn, rec, args.source, seq)

            if status == "violation":
                violations += 1
            elif status == "folded":
                folded += 1
            else:
                applied += 1
            conn.commit()

        conn.execute(
            "UPDATE cdc_stream SET records=records+? WHERE source=?",
            (len(parsed), args.source),
        )
        conn.commit()
        out = {
            "source": args.source,
            "frames": len(parsed),
            "applied": applied,
            "folded": folded,
            "violations": violations,
            "ended": bool(conn.execute(
                "SELECT ended FROM cdc_stream WHERE source=?", (args.source,)).fetchone()["ended"]),
        }
        print(json.dumps(out, ensure_ascii=False))
        return 0
    except json.JSONDecodeError as e:
        print(json.dumps({"fatal": "invalid JSON", "detail": str(e)}, ensure_ascii=False),
              file=sys.stderr)
        return 2
    finally:
        close(conn)


# ---------------------------------------------------------------- work：调度与物化

def all_streams_ended(conn) -> bool:
    row = conn.execute("SELECT MIN(ended) m FROM cdc_stream").fetchone()
    return row["m"] == 1


def load_txn_map(conn) -> dict:
    out = {}
    for r in conn.execute("SELECT * FROM cdc_txn"):
        d = dict(r)
        d["parts"] = json.loads(d["parts_json"]) if d["parts_json"] else None
        d["deps"] = json.loads(d["deps_json"]) if d["deps_json"] else None
        out[d["txn"]] = d
    return out


def fragments_complete(conn, txn: str, parts: dict) -> tuple[bool, str | None]:
    """返回 (是否齐全且无多余, 违例原因)。part_no 越界在 ingest 已按控制声明拦截，
    这里兜底处理“控制迟到/控制缺失”下的多余片段。"""
    have = conn.execute(
        "SELECT partition, COUNT(*) c, MIN(part_no) lo, MAX(part_no) hi "
        "FROM cdc_fragment WHERE txn=? GROUP BY partition",
        (txn,),
    ).fetchall()
    counts = {r["partition"]: r["c"] for r in have}
    for p, n in parts.items():
        if counts.get(p, 0) < n:
            return False, None
    for r in have:
        decl = parts.get(r["partition"])
        if decl is None:
            return False, f"unexpected_partition:{r['partition']}"
        if r["c"] > decl or r["lo"] < 0 or r["hi"] >= decl:
            return False, (f"fragment_out_of_bounds:{r['partition']} "
                           f"count={r['c']} declared={decl}")
    return True, None


def reachable_ancestors(txns: dict, txn: str) -> set[str]:
    """沿 dependencies 传递闭包（祖先 = 必须先提交的事务）。"""
    seen, stack = set(), list(txns[txn]["deps"] or [])
    while stack:
        d = stack.pop()
        if d in seen or d not in txns:
            continue
        seen.add(d)
        stack.extend(txns[d]["deps"] or [])
    return seen


def find_cycles(txns: dict) -> set[str]:
    """对控制已到的依赖图做 DFS，返回处在环上的全部 txn。"""
    WHITE, GRAY, BLACK = 0, 1, 2
    color = {t: WHITE for t in txns}
    in_cycle: set[str] = set()

    def dfs(node: str, stack: list[str]) -> None:
        color[node] = GRAY
        stack.append(node)
        for dep in txns[node]["deps"] or []:
            if dep not in txns:
                continue
            if color[dep] == GRAY:
                in_cycle.update(stack[stack.index(dep):])
            elif color[dep] == WHITE:
                dfs(dep, stack)
        stack.pop()
        color[node] = BLACK

    for t in txns:
        if color[t] == WHITE:
            dfs(t, [])
    return in_cycle


def semantic_check(conn, txns: dict, txn: str) -> str | None:
    """提交前语义校验：int64/非负、外键父对象来源。返回违例原因或 None。"""
    txns_deps = {t: (meta["deps"] or []) for t, meta in txns.items()}
    frags = [json.loads(r["payload_json"]) for r in conn.execute(
        "SELECT payload_json FROM cdc_fragment WHERE txn=? ORDER BY op_no", (txn,))]
    local_customers: set[str] = set()
    for rec in frags:
        op = rec["op"]
        if op == "put_customer":
            local_customers.add(rec["id"])
        elif op == "put_order":
            amount = rec["amount"]
            if amount < 0 or amount > INT64_MAX:
                return f"amount_out_of_range: order={rec['id']} amount={amount}"
            if rec["customer"] in local_customers:
                continue
            # 父客户必须由显式依赖中已提交的事务建立，禁止推测隐藏依赖
            declared_deps = txns_deps[txn]
            placeholders = ",".join("?" * len(declared_deps)) if declared_deps else ""
            if not declared_deps:
                return (f"fk_parent_missing: order={rec['id']} customer={rec['customer']} "
                        f"not created earlier in txn and no declared dependency")
            parent = conn.execute(
                f"SELECT 1 FROM cdc_write_set ws JOIN cdc_txn t ON t.txn=ws.txn "
                f"WHERE ws.kind='customer' AND ws.id=? AND t.state='committed' "
                f"AND t.txn IN ({placeholders}) LIMIT 1",
                [rec["customer"], *declared_deps],
            ).fetchone()
            if parent is None:
                return (f"fk_parent_missing: order={rec['id']} customer={rec['customer']} "
                        f"not created earlier in txn nor by declared committed dependency")
    return None


def write_conflict_verdict(conn, txns: dict, txn: str) -> str | None:
    """写集合同 key 冲突检查。返回拒绝原因，或 None 表示合同满足。"""
    mine = {(r["kind"], r["id"]) for r in conn.execute(
        "SELECT kind,id FROM cdc_write_set WHERE txn=?", (txn,))}
    ancestors = reachable_ancestors(txns, txn)
    for kind, wid in mine:
        for r in conn.execute(
                "SELECT txn FROM cdc_write_set WHERE kind=? AND id=? AND txn<>?",
                (kind, wid, txn)):
            other = r["txn"]
            if other not in txns:
                continue
            o = txns[other]
            # 只对已知完整的事务核合同；不完整的一方后续 pass 再判
            if not o["control_seen"] or not o["parts"]:
                continue
            complete, _ = fragments_complete(conn, other, o["parts"])
            if not complete:
                continue
            if o["state"] == "committed":
                # 历史已提交：无依赖路径即新事务违规，历史不动
                if other not in ancestors:
                    return (f"unsorted_write_conflict: committed {other} and {txn} both "
                            f"write {kind}:{wid} without dependency path")
            else:
                other_ancestors = reachable_ancestors(txns, other)
                if txn not in other_ancestors and other not in ancestors:
                    return (f"unsorted_write_conflict: {txn} and {other} both write "
                            f"{kind}:{wid} with no dependency path either way")
    return None


def apply_ops(conn, frags: list[dict]) -> None:
    for rec in frags:
        op, rid = rec["op"], rec["id"]
        if op == "put_customer":
            conn.execute(
                "INSERT INTO customers(id,name) VALUES(?,?) "
                "ON CONFLICT(id) DO UPDATE SET name=excluded.name",
                (rid, rec["name"]),
            )
        elif op == "put_order":
            conn.execute(
                "INSERT INTO orders(id,customer_id,amount) VALUES(?,?,?) "
                "ON CONFLICT(id) DO UPDATE SET customer_id=excluded.customer_id, "
                "amount=excluded.amount",
                (rid, rec["customer"], rec["amount"]),
            )
        else:  # delete_order：缺失行即 no-op
            conn.execute("DELETE FROM orders WHERE id=?", (rid,))


def commit_txn(conn, txn: str) -> dict:
    """全部业务操作 + 提交标记 + 回执在同一个 SQLite 事务内可见。

    返回 {"state": ..., "receipt": ...}；不在此直接打印，由 work 统一汇总。
    """
    frag_rows = conn.execute(
        "SELECT payload_json,payload_hash FROM cdc_fragment WHERE txn=? ORDER BY op_no",
        (txn,)).fetchall()
    frags = [json.loads(r["payload_json"]) for r in frag_rows]

    fault = conn.execute(
        "SELECT name,armed FROM cdc_fault WHERE txn=? AND armed=1", (txn,)
    ).fetchall()
    fail_before = any(f["name"] == "fail_before_commit" for f in fault)
    lose_ack = any(f["name"] == "lose_ack_after_commit" for f in fault)

    conn.execute("BEGIN IMMEDIATE")
    try:
        apply_ops(conn, frags)
        if fail_before:
            # 模拟业务操作已执行、提交标记写入前崩溃：回滚，目标无痕迹
            conn.execute("ROLLBACK")
            conn.execute("UPDATE cdc_fault SET armed=0 WHERE txn=? AND name='fail_before_commit'",
                         (txn,))
            log_violation(conn, "injected_failure", "fail_before_commit", txn)
            log_event(conn, "commit_failed_injected", txn)
            set_state(conn, txn, "failed", "injected fail_before_commit")
            conn.commit()
            return {"state": "failed", "receipt": None,
                    "reason": "injected fail_before_commit"}

        digest = hashlib.sha256(
            txn.encode() + b"|" + b",".join(r["payload_hash"].encode() for r in frag_rows)
        ).hexdigest()[:16]
        receipt_id = f"rcpt_{digest}"
        ts = now()
        writes = [{"kind": ("customer" if f["op"] == "put_customer" else "order"),
                   "id": f["id"], "action": f["op"]} for f in frags]
        receipt = {
            "txn": txn, "receipt": receipt_id, "committed_at": ts,
            "ops": len(frags), "writes": writes,
        }
        conn.execute(
            "INSERT INTO cdc_receipt(txn,receipt,committed_at,ops,receipt_json) "
            "VALUES(?,?,?,?,?)",
            (txn, receipt_id, ts, len(frags), json.dumps(receipt, ensure_ascii=False)),
        )
        set_state(conn, txn, "committed")
        log_event(conn, "committed", txn, receipt_id)
        conn.execute("COMMIT")
    except Exception as exc:
        conn.execute("ROLLBACK")
        log_violation(conn, "commit_error", str(exc), txn)
        set_state(conn, txn, "failed", f"commit_error: {exc}")
        conn.commit()
        return {"state": "failed", "receipt": None, "reason": str(exc)}

    if lose_ack:
        # 提交已落盘但确认丢失：解除武装后以非零码退出，模拟进程在返回前崩溃
        conn.execute("UPDATE cdc_fault SET armed=0 WHERE txn=? AND name='lose_ack_after_commit'",
                     (txn,))
        log_event(conn, "ack_lost_after_commit", txn, receipt_id)
        conn.commit()
        raise LostAck(receipt)

    return {"state": "committed", "receipt": receipt}


def evaluate(conn, txns: dict, txn: str, ended: bool, cycle_members: set[str]) -> str:
    """计算单个事务的新状态；只返回决定，落盘由调用方统一处理。"""
    t = txns[txn]
    if t["state"] in TERMINAL_STATES:
        return t["state"]
    if t["corrupt"]:
        # 损坏优先于 abort：损坏的事务即便声明 abort 也必须留下可核查的 corrupt 记录
        return "corrupt"
    # abort 从不触碰目标，片段是否齐全不影响其终态
    if t["control_seen"] and t["terminal"] == "abort":
        return "aborted"
    if not t["control_seen"] or not t["parts"]:
        return "incomplete" if ended else "receiving"
    complete, bound_violation = fragments_complete(conn, txn, t["parts"])
    if not complete:
        if bound_violation:
            # 多余/越界片段属于协议损坏而非“缺片”
            if not t["corrupt"]:
                log_violation(conn, "fragment_out_of_bounds", bound_violation, txn)
                conn.execute("UPDATE cdc_txn SET corrupt=1 WHERE txn=?", (txn,))
                conn.commit()
            return "corrupt"
        return "incomplete" if ended else "receiving"

    # 完整的 commit 事务：解依赖
    if txn in cycle_members:
        return "rejected"
    for dep in t["deps"]:
        if dep not in txns or not txns[dep]["control_seen"]:
            return "missing_dependency" if ended else "waiting_dependency"
        dstate = txns[dep]["state"]
        if dstate in {"aborted", "failed", "rejected", "corrupt",
                      "blocked", "incomplete", "missing_dependency"}:
            return "blocked"
        if dstate != "committed":
            return "waiting_dependency"

    reason = semantic_check(conn, txns, txn)
    if reason:
        return "failed"
    reason = write_conflict_verdict(conn, txns, txn)
    if reason:
        return "rejected"
    return "ready"


def cmd_work(args) -> int:
    conn = connect(args.db)
    ack_lost = False
    try:
        max_passes = args.max_passes
        committed, changed_total = [], 0
        for _ in range(max_passes):
            txns = load_txn_map(conn)
            ended = all_streams_ended(conn)
            cycle_members = find_cycles(txns)
            changed = False
            for txn in sorted(txns):
                t = txns[txn]
                if t["state"] in TERMINAL_STATES:
                    continue
                new_state = evaluate(conn, txns, txn, ended, cycle_members)
                if new_state == t["state"] and new_state != "ready":
                    continue
                if new_state == "ready":
                    # ready 不持久化为停留态：直接在本 pass 物化
                    result = commit_txn(conn, txn)
                    if result["state"] == "committed":
                        committed.append(result["receipt"])
                    changed = True
                    # 重新加载内存视图，后续判定基于最新状态
                    txns = load_txn_map(conn)
                else:
                    reason = None
                    if new_state in {"failed", "rejected", "corrupt", "blocked",
                                     "incomplete", "missing_dependency"}:
                        reason = evaluate_reason(conn, txns, txn, new_state, cycle_members)
                    set_state(conn, txn, new_state, reason)
                    log_event(conn, f"state_{new_state}", txn, reason)
                    conn.commit()
                    # 同步内存视图：同 pass 后续事务（尤其后继依赖）必须看到新状态
                    fresh_row = conn.execute(
                        "SELECT * FROM cdc_txn WHERE txn=?", (txn,)).fetchone()
                    d = dict(fresh_row)
                    d["parts"] = json.loads(d["parts_json"]) if d["parts_json"] else None
                    d["deps"] = json.loads(d["deps_json"]) if d["deps_json"] else None
                    txns[txn] = d
                    changed = True
            if not changed:
                break
            changed_total += 1
            if args.once:
                break

        summary = {
            "passes_with_changes": changed_total,
            "committed": [r["txn"] for r in committed],
            "receipts": committed,
            "states": {r["txn"]: r["state"] for r in conn.execute(
                "SELECT txn,state FROM cdc_txn ORDER BY txn")},
            "all_streams_ended": all_streams_ended(conn),
        }
        print(json.dumps(summary, ensure_ascii=False))
        return 0
    except LostAck as e:
        # lose_ack_after_commit：提交已落盘，进程在返回确认前终止
        ack_lost = True
        close(conn)
        sys.stderr.write(json.dumps(
            {"fatal": "ack_lost_after_commit", "receipt": e.receipt,
             "detail": "commit durable but acknowledgement lost; rerun work to recover"},
            ensure_ascii=False) + "\n")
        return 3
    finally:
        if not ack_lost:
            close(conn)


def evaluate_reason(conn, txns: dict, txn: str, state: str, cycle_members: set[str]) -> str | None:
    t = txns[txn]
    if state == "failed":
        return semantic_check(conn, txns, txn)
    if state == "rejected":
        if txn in cycle_members:
            return "dependency_cycle"
        return write_conflict_verdict(conn, txns, txn)
    if state == "blocked":
        for dep in t["deps"] or []:
            if dep in txns and txns[dep]["state"] in {
                "aborted", "failed", "rejected", "corrupt",
                "blocked", "incomplete", "missing_dependency"}:
                return f"dependency {dep} in terminal state {txns[dep]['state']}"
        return "blocked by dependency"
    if state == "incomplete":
        return "declared fragments not all received after source end"
    if state == "missing_dependency":
        return "declared dependency unknown after source end"
    if state == "corrupt":
        v = conn.execute(
            "SELECT code FROM cdc_violation WHERE txn=? ORDER BY id LIMIT 1", (txn,))\
            .fetchone()
        return v["code"] if v else "corrupt"
    return None


# ---------------------------------------------------------------- query / inspect / fault

def cmd_query(args) -> int:
    conn = connect(args.db)
    try:
        out = {}
        if args.table in (None, "customers"):
            out["customers"] = [dict(r) for r in conn.execute(
                "SELECT id,name FROM customers ORDER BY id")]
        if args.table in (None, "orders"):
            out["orders"] = [dict(r) for r in conn.execute(
                "SELECT id,customer_id,amount FROM orders ORDER BY id")]
        print(json.dumps(out, ensure_ascii=False, indent=2 if args.pretty else None))
        return 0
    finally:
        close(conn)


def cmd_inspect(args) -> int:
    conn = connect(args.db)
    try:
        streams = {r["source"]: dict(r) for r in conn.execute(
            "SELECT source,ended,records FROM cdc_stream ORDER BY source")}
        txns = []
        for r in conn.execute("SELECT * FROM cdc_txn ORDER BY txn"):
            d = dict(r)
            d["parts_declared"] = json.loads(d.pop("parts_json")) if d["parts_json"] else None
            d["dependencies"] = json.loads(d.pop("deps_json")) if d["deps_json"] else None
            if d["parts_declared"]:
                cnt = {p: conn.execute(
                    "SELECT COUNT(*) c FROM cdc_fragment WHERE txn=? AND partition=?",
                    (d["txn"], p)).fetchone()["c"] for p in d["parts_declared"]}
                d["fragments"] = {"declared": d["parts_declared"], "received": cnt}
            if args.txn and d["txn"] != args.txn:
                continue
            rcpt = conn.execute(
                "SELECT receipt_json FROM cdc_receipt WHERE txn=?", (d["txn"],)).fetchone()
            if rcpt:
                d["receipt"] = json.loads(rcpt["receipt_json"])
            txns.append(d)
        violations = [dict(r) for r in conn.execute(
            "SELECT * FROM cdc_violation ORDER BY id")]
        events = [dict(r) for r in conn.execute(
            "SELECT * FROM cdc_event ORDER BY id")]
        faults = [dict(r) for r in conn.execute("SELECT * FROM cdc_fault ORDER BY name")]
        out = {"streams": streams, "txns": txns, "violations": violations,
               "events": events, "faults": faults}
        print(json.dumps(out, ensure_ascii=False, indent=2 if args.pretty else None))
        return 0
    finally:
        close(conn)


def cmd_arm(args) -> int:
    if args.name not in ("fail_before_commit", "lose_ack_after_commit"):
        raise SystemExit("unknown fault")
    conn = connect(args.db)
    try:
        conn.execute(
            "INSERT INTO cdc_fault(name,txn,armed) VALUES(?,?,1) "
            "ON CONFLICT(name) DO UPDATE SET txn=excluded.txn, armed=1",
            (args.name, args.txn),
        )
        log_event(conn, "fault_armed", args.txn, args.name)
        conn.commit()
        print(json.dumps({"armed": args.name, "txn": args.txn}, ensure_ascii=False))
        return 0
    finally:
        close(conn)


# ---------------------------------------------------------------- CLI

def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="single-machine CDC loader (JSONL protocol only)")
    sub = p.add_subparsers(dest="cmd", required=True)

    pi = sub.add_parser("ingest", help="ingest one JSONL source stream")
    pi.add_argument("db")
    pi.add_argument("--source", required=True, choices=SOURCES)
    pi.add_argument("--file", default="-", help="JSONL path or - for stdin")
    pi.set_defaults(func=cmd_ingest)

    pw = sub.add_parser("work", help="run scheduler/materialization until stable")
    pw.add_argument("db")
    pw.add_argument("--once", action="store_true", help="single evaluation pass")
    pw.add_argument("--max-passes", type=int, default=1000)
    pw.set_defaults(func=cmd_work)

    pq = sub.add_parser("query", help="dump visible target rows")
    pq.add_argument("db")
    pq.add_argument("--table", choices=["customers", "orders"])
    pq.add_argument("--pretty", action="store_true")
    pq.set_defaults(func=cmd_query)

    pn = sub.add_parser("inspect", help="dump loader state, violations, events, receipts")
    pn.add_argument("db")
    pn.add_argument("--txn")
    pn.add_argument("--pretty", action="store_true")
    pn.set_defaults(func=cmd_inspect)

    pf = sub.add_parser("arm-fault", help="arm an injected failure")
    pf.add_argument("db")
    pf.add_argument("name", choices=["fail_before_commit", "lose_ack_after_commit"])
    pf.add_argument("--txn", required=True)
    pf.set_defaults(func=cmd_arm)

    args = p.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
