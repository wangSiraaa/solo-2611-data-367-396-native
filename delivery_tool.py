#!/usr/bin/env python3.11
"""
delivery_tool.py — 本地 CSV 分段上传与发布工具。

只通过 multipart_store.Store 的公开方法与对象服务模拟器交互，从不直接
读写服务的 state.json。五个子命令：plan / upload / resume / publish / verify。

状态目录（本地持久日志，原子写入）：
  plan.json    交付计划：源文件 SHA256、长度、分段策略、key、delivery、expected_generation
  upload.json  上传日志：upload_id、每段确认的 etag/size、对象版本、发送字节计数

故障注入（仅用于演练，环境变量，单次进程内生效）：
  DELIVERY_KILL_AFTER_PART=N            上传完第 N 段并落盘后硬退出（模拟进程被 kill -9）
  DELIVERY_FAULT_COMPLETE=before|after  complete 提交前失败 / 提交后响应丢失
  DELIVERY_CRASH_BEFORE_CATALOG_RENAME=1  catalog 临时文件写好、rename 前硬退出
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import importlib.util
import io
import json
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path

# 退出码
OK = 0
USAGE = 2
UNCERTAIN = 3           # complete 结果未知，需 resume 收敛
CONTENT_CONFLICT = 4    # delivery_id 被不同内容重用
GENERATION_CONFLICT = 10  # expected_generation 与服务当前代不符
FOREIGN_OBJECT = 11     # key 上已是别的 delivery / 别的内容，拒绝覆盖
STATE_DRIFT = 12        # 本地计划与源文件/现状不一致
VERIFY_FAILED = 1

PART_MIN, PART_MAX = 64, 256
MAX_FILE = 16 * 1024
MAX_PARTS = 256


# --------------------------- 通用工具 ---------------------------

def die(code: int, message: str, **extra) -> "typing.NoReturn":
    payload = {"status": "error", "exit_code": code, "message": message, **extra}
    print(json.dumps(payload, indent=2, ensure_ascii=False))
    sys.exit(code)


def ok(message: str, **extra) -> int:
    payload = {"status": "ok", "message": message, **extra}
    print(json.dumps(payload, indent=2, ensure_ascii=False))
    return OK


def atomic_write_json(path: Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, sort_keys=True, ensure_ascii=False)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def read_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def sha256_file(path: Path) -> tuple[str, int]:
    h = hashlib.sha256()
    size = 0
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            h.update(chunk)
            size += len(chunk)
    return h.hexdigest(), size


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def planned_parts(size: int, part_bytes: int) -> list[dict]:
    """编号从 1 连续，末段可短。"""
    parts, offset, no = [], 0, 1
    while offset < size:
        length = min(part_bytes, size - offset)
        parts.append({"no": no, "offset": offset, "length": length})
        offset += length
        no += 1
    return parts


def load_store(store_root: str, lib_path: str):
    spec = importlib.util.spec_from_file_location("multipart_store", lib_path)
    if spec is None or spec.loader is None:
        die(USAGE, f"无法加载服务模拟器: {lib_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.Store(store_root)


def generation_of(version: str) -> int:
    return int(version.rsplit("@", 1)[1])


@dataclass
class Context:
    state_dir: Path
    store_root: str
    store_lib: str
    store: object
    plan_path: Path
    upload_path: Path

    @classmethod
    def build(cls, args) -> "Context":
        state_dir = Path(args.state_dir)
        state_dir.mkdir(parents=True, exist_ok=True)
        lib = args.store_lib or str(Path(__file__).with_name("multipart_store.py"))
        store = load_store(args.store_root, lib)
        return cls(
            state_dir=state_dir,
            store_root=args.store_root,
            store_lib=lib,
            store=store,
            plan_path=state_dir / "plan.json",
            upload_path=state_dir / "upload.json",
        )

    def load_plan(self) -> dict:
        if not self.plan_path.exists():
            die(USAGE, "尚无交付计划，请先执行 plan")
        return read_json(self.plan_path)

    def load_upload(self) -> dict | None:
        return read_json(self.upload_path) if self.upload_path.exists() else None

    def save_upload(self, upload: dict) -> None:
        atomic_write_json(self.upload_path, upload)

    def source_path(self, plan: dict, override: str | None) -> Path:
        path = Path(override) if override else Path(plan["source_path"])
        if not path.exists():
            die(STATE_DRIFT, f"源文件不可读: {path}")
        return path

    def revalidate_source(self, plan: dict, override: str | None) -> Path:
        """重读源文件并核对身份；内容漂移直接拒绝，绝不沿用旧分段位置。"""
        path = self.source_path(plan, override)
        digest, size = sha256_file(path)
        if digest != plan["source_sha256"] or size != plan["size"]:
            die(
                STATE_DRIFT,
                "源文件身份与计划不符，拒绝在旧 delivery 上套用新内容；请使用新 delivery_id 重新 plan",
                plan_digest=plan["source_sha256"],
                current_digest=digest,
                plan_size=plan["size"],
                current_size=size,
            )
        return path


# --------------------------- plan ---------------------------

def cmd_plan(args) -> int:
    ctx = Context.build(args)
    csv_path = Path(args.csv).resolve()
    if not csv_path.is_file():
        die(USAGE, f"CSV 不存在: {csv_path}")
    if not (PART_MIN <= args.part_bytes <= PART_MAX):
        die(USAGE, f"part_bytes 必须在 {PART_MIN}..{PART_MAX} 字节之间")

    digest, size = sha256_file(csv_path)
    if size > MAX_FILE:
        die(USAGE, f"文件 {size} 字节超过单文件上限 {MAX_FILE}")
    parts = planned_parts(size, args.part_bytes)
    if len(parts) > MAX_PARTS:
        die(USAGE, f"分段数 {len(parts)} 超过上限 {MAX_PARTS}，请调大 part_bytes")

    # 预检 generation，避免在必然冲突的计划上白传分段（服务端 complete 仍是权威 CAS）
    head = ctx.store.head(args.key)
    current_gen = generation_of(head["version"]) if head else 0
    if args.expected_generation != current_gen:
        die(
            GENERATION_CONFLICT,
            "expected_generation 预检失败",
            key=args.key,
            current_generation=current_gen,
            expected_generation=args.expected_generation,
        )

    if ctx.plan_path.exists():
        old = read_json(ctx.plan_path)
        if old["delivery_id"] != args.delivery or old["key"] != args.key:
            die(USAGE, "状态目录已绑定另一 delivery/key，请使用独立状态目录")
        identity_changed = (
            old["source_sha256"] != digest
            or old["size"] != size
            or old["key"] != args.key
        )
        if identity_changed:
            # 不同内容重用 delivery_id：明确拒绝（服务端 complete 也会挡 DELIVERY_CONTENT_CONFLICT）
            die(
                CONTENT_CONFLICT,
                "delivery_id 已绑定不同源内容/目标 key，拒绝复用；新交付请使用新 delivery_id",
                delivery_id=args.delivery,
                old_digest=old["source_sha256"],
                new_digest=digest,
            )
        if old["part_bytes"] != args.part_bytes:
            # 同一原件、分段策略变化：明确的新计划。旧暂存分段边界不同，一律不得沿用。
            old_upload = ctx.load_upload()
            if old_upload and ctx.store.receipt(args.delivery) is not None:
                die(
                    STATE_DRIFT,
                    "该 delivery 已有成品回执，分段策略不可再变；直接 publish/verify 即可",
                )
            aborted = None
            if old_upload and old_upload.get("upload_id"):
                # 只清理自己状态里记录、且确定没有成品的暂存 upload；别人的 upload 绝不碰
                ctx.store.abort(old_upload["upload_id"], args.delivery)
                aborted = old_upload["upload_id"]
            ctx.save_upload(
                {
                    "delivery_id": args.delivery,
                    "upload_id": None,
                    "status": "planning",
                    "parts_confirmed": {},
                    "bytes_sent_total": 0,
                    "object_version": None,
                    "supersedes": aborted,
                }
            )
            strategy = "changed:old_upload_aborted"
        else:
            return ok(
                "计划已存在且身份一致，无需重建",
                plan=old,
                upload=ctx.load_upload(),
            )
    else:
        strategy = "new"

    plan = {
        "delivery_id": args.delivery,
        "key": args.key,
        "source_path": str(csv_path),
        "source_sha256": digest,
        "size": size,
        "part_bytes": args.part_bytes,
        "expected_generation": args.expected_generation,
        "parts": parts,
        "created_at": time.time(),
    }
    atomic_write_json(ctx.plan_path, plan)
    if not ctx.upload_path.exists():
        ctx.save_upload(
            {
                "delivery_id": args.delivery,
                "upload_id": None,
                "status": "planning",
                "parts_confirmed": {},
                "bytes_sent_total": 0,
                "object_version": None,
            }
        )
    return ok(
        f"交付计划已持久化（{strategy}）",
        plan={k: v for k, v in plan.items() if k != "parts"},
        part_count=len(parts),
        last_part_length=parts[-1]["length"] if parts else 0,
    )


# --------------------------- 分段上传与核对 ---------------------------

def ensure_session(ctx: Context, plan: dict, upload: dict) -> str:
    """返回可用 upload_id；服务端若已无此会话则新建并清空本地确认。"""
    uid = upload.get("upload_id")
    live = {u["upload_id"] for u in ctx.store.list_uploads()}
    if uid and uid in live:
        return uid
    if uid:
        # 会话被外部清理过：旧确认全部失效，重新 begin
        upload["parts_confirmed"] = {}
    new_uid = ctx.store.begin(
        plan["key"],
        plan["delivery_id"],
        plan["source_sha256"],
        plan["size"],
        plan["expected_generation"],
    )
    upload["upload_id"] = new_uid
    upload["status"] = "uploading"
    ctx.save_upload(upload)
    return new_uid


def reconcile_parts(ctx: Context, plan: dict, upload: dict, source: Path) -> dict:
    """
    逐段收敛。确认一段的唯一依据是三段一致：
    本地计划段字节算出的 etag == 本地日志 etag == 服务 list_parts 的 etag，且 size 一致。
    同 part_no 不同内容绝不沿用旧日志。返回本次运行统计。
    """
    uid = ensure_session(ctx, plan, upload)
    remote = ctx.store.list_parts(uid)  # 只说明暂存状态，不证明成品
    confirmed = upload.setdefault("parts_confirmed", {})
    run = {"parts_reused": 0, "parts_sent": 0, "bytes_sent_run": 0}
    kill_after = os.environ.get("DELIVERY_KILL_AFTER_PART")
    kill_after = int(kill_after) if kill_after else None

    with open(source, "rb") as f:
        for part in plan["parts"]:
            no = str(part["no"])
            f.seek(part["offset"])
            data = f.read(part["length"])
            local_etag = sha256_bytes(data)
            r = remote.get(no)
            if (
                r
                and r.get("etag") == local_etag
                and r.get("size") == len(data)
                and confirmed.get(no, {}).get("etag") == local_etag
            ):
                # 重启复用：编号、大小、实际字节摘要全部核对通过
                run["parts_reused"] += 1
                continue
            # 缺失或同编号内容不一致：重新上传该段
            etag = ctx.store.part(uid, part["no"], data)
            if etag != local_etag:
                die(STATE_DRIFT, "服务返回 etag 与本地字节摘要不一致", part_no=no)
            confirmed[no] = {"etag": etag, "size": len(data)}
            upload["bytes_sent_total"] = upload.get("bytes_sent_total", 0) + len(data)
            run["parts_sent"] += 1
            run["bytes_sent_run"] += len(data)
            ctx.save_upload(upload)  # 每段持久，进程中断后可列举复用
            if kill_after == part["no"]:
                sys.stderr.write(
                    f"[fault] 第 {part['no']} 段已确认并落盘，模拟硬中断（exit 137）\n"
                )
                os._exit(137)

    nos = sorted(int(n) for n in confirmed)
    expected = [p["no"] for p in plan["parts"]]
    if nos != expected:
        die(STATE_DRIFT, "已确认分段不连续或不完整", confirmed=nos, expected=expected)
    upload["status"] = "parts_complete"
    ctx.save_upload(upload)
    return run


def build_manifest(plan: dict, upload: dict) -> list[dict]:
    return [
        {"no": p["no"], "etag": upload["parts_confirmed"][str(p["no"])]["etag"]}
        for p in plan["parts"]
    ]


def preflight_generation(ctx: Context, plan: dict) -> None:
    head = ctx.store.head(plan["key"])
    current_gen = generation_of(head["version"]) if head else 0
    if current_gen != plan["expected_generation"]:
        die(
            GENERATION_CONFLICT,
            "目标 key 的 generation 已变化，拒绝按旧条件提交",
            key=plan["key"],
            current_generation=current_gen,
            expected_generation=plan["expected_generation"],
        )


def attempt_complete(ctx: Context, plan: dict, upload: dict) -> int:
    """
    调用 complete。异常本身不能用来判断对象是否已存在：
    任何 OSError（含 BEFORE_COMMIT / RESPONSE_LOST）都只标记 unknown，交给 resume 收敛。
    """
    preflight_generation(ctx, plan)
    manifest = build_manifest(plan, upload)
    fault = os.environ.get("DELIVERY_FAULT_COMPLETE")  # before / after
    upload["status"] = "complete_uncertain"
    upload["complete_attempts"] = upload.get("complete_attempts", 0) + 1
    ctx.save_upload(upload)
    try:
        receipt = ctx.store.complete(upload["upload_id"], manifest, fail=fault)
    except ValueError as e:
        code = str(e)
        if code == "GENERATION_CONFLICT":
            die(GENERATION_CONFLICT, "服务拒绝：generation 不匹配，未覆盖任何对象",
                key=plan["key"], expected_generation=plan["expected_generation"])
        if code == "DELIVERY_CONTENT_CONFLICT":
            die(CONTENT_CONFLICT, "服务拒绝：同一 delivery_id 已绑定不同内容/key",
                delivery_id=plan["delivery_id"])
        die(STATE_DRIFT, f"服务拒绝 complete: {code}")
    except OSError as e:
        # 可能是提交前失败，也可能已提交但响应丢失——不做猜测，保持 unknown
        return UNCERTAIN if not _adopt_if_committed(ctx, plan, upload, reason=str(e)) else OK

    adopt_receipt(ctx, plan, upload, receipt, origin="complete_response")
    ctx.save_upload(upload)
    return OK


def adopt_receipt(ctx: Context, plan: dict, upload: dict, receipt: dict, origin: str) -> None:
    for field in ("key", "delivery", "digest", "size", "version"):
        if field not in receipt:
            die(STATE_DRIFT, f"回执缺少字段 {field}")
    if receipt["key"] != plan["key"] or receipt["delivery"] != plan["delivery_id"]:
        die(FOREIGN_OBJECT, "回执不属于本次交付的 key/delivery", receipt=receipt)
    if receipt["digest"] != plan["source_sha256"] or receipt["size"] != plan["size"]:
        die(FOREIGN_OBJECT, "回执绑定的内容身份与本次交付不匹配", receipt=receipt)
    upload["object_version"] = receipt["version"]
    upload["receipt"] = receipt
    upload["status"] = "completed"
    upload["adopted_via"] = origin


def _adopt_if_committed(ctx: Context, plan: dict, upload: dict, reason: str) -> bool:
    """unknown 后查证：只在服务回执/成品身份与本次交付完全匹配时认领。"""
    receipt = ctx.store.receipt(plan["delivery_id"])
    if receipt is not None:
        adopt_receipt(ctx, plan, upload, receipt, origin=f"receipt_after_error:{reason}")
        ctx.save_upload(upload)
        return True
    head = ctx.store.head(plan["key"])
    if head is not None:
        same = (
            head.get("delivery") == plan["delivery_id"]
            and head.get("digest") == plan["source_sha256"]
            and head.get("size") == plan["size"]
        )
        if not same:
            die(
                FOREIGN_OBJECT,
                "complete 异常后发现 key 上是其他 delivery/内容的对象，拒绝认领或覆盖",
                head=head,
                delivery_id=plan["delivery_id"],
                source_sha256=plan["source_sha256"],
            )
        adopt_receipt(ctx, plan, upload, head, origin=f"head_identity_after_error:{reason}")
        ctx.save_upload(upload)
        return True
    # 回执与成品都不存在：保持 uncertain，由 resume 重新提交
    ctx.save_upload(upload)
    return False


# --------------------------- upload / resume ---------------------------

def cmd_upload(args) -> int:
    ctx = Context.build(args)
    plan = ctx.load_plan()
    upload = ctx.load_upload() or {"status": "planning", "parts_confirmed": {}}
    source = ctx.revalidate_source(plan, args.csv)

    if upload.get("status") == "completed" or upload.get("object_version"):
        # 已完成：只做只读核验，绝不重复提交产生第二对象
        receipt = ctx.store.receipt(plan["delivery_id"])
        if receipt and receipt["version"] == upload["object_version"]:
            return ok("该交付已有成品版本，upload 幂等返回", **_completed_view(upload))
        die(STATE_DRIFT, "本地记录的版本在服务侧无法核对，请执行 resume")

    run = reconcile_parts(ctx, plan, upload, source)
    rc = attempt_complete(ctx, plan, upload)
    if rc == UNCERTAIN:
        return _uncertain_output(plan, upload, run)
    return ok("分段上传并 complete 成功", run=run, **_completed_view(upload))


def cmd_resume(args) -> int:
    ctx = Context.build(args)
    plan = ctx.load_plan()
    upload = ctx.load_upload() or die(USAGE, "无上传日志，无法 resume")
    source = ctx.revalidate_source(plan, args.csv)

    # 1) 已完成：重新核对不可变版本仍然可读且身份一致
    if upload.get("object_version"):
        receipt = ctx.store.receipt(plan["delivery_id"])
        head = ctx.store.head(plan["key"])
        if receipt and receipt["version"] == upload["object_version"]:
            data = ctx.store.read(upload["object_version"])
            if sha256_bytes(data) == plan["source_sha256"] and len(data) == plan["size"]:
                return ok(
                    "交付早已完成，版本身份复核一致（幂等 resume，零额外对象字节）",
                    run={"parts_reused": len(plan["parts"]), "parts_sent": 0,
                         "bytes_sent_run": 0},
                    head_version=head["version"] if head else None,
                    **_completed_view(upload),
                )
        upload["object_version"] = None  # 本地记录不可信，落入下面的重新收敛

    # 2) 分段收敛：list_parts 核对编号/大小/etag，缺啥补啥
    run = reconcile_parts(ctx, plan, upload, source)

    # 3) 不确定完成的收敛：先查交付回执，再查成品绑定身份
    receipt = ctx.store.receipt(plan["delivery_id"])
    if receipt is not None:
        adopt_receipt(ctx, plan, upload, receipt, origin="receipt_on_resume")
        ctx.save_upload(upload)
        return ok(
            "complete 结果未知后经交付回执确认：同一 delivery、同一原件，认领既有版本，未新建对象",
            run=run, **_completed_view(upload),
        )

    head = ctx.store.head(plan["key"])
    if head is not None:
        same = (
            head.get("delivery") == plan["delivery_id"]
            and head.get("digest") == plan["source_sha256"]
            and head.get("size") == plan["size"]
        )
        if same:
            adopt_receipt(ctx, plan, upload, head, origin="head_identity_on_resume")
            ctx.save_upload(upload)
            return ok(
                "经成品绑定身份确认属于本次交付，认领既有不可变版本",
                run=run, **_completed_view(upload),
            )
        die(
            FOREIGN_OBJECT,
            "key 已被其他 delivery/内容占用且 generation 已变化，报告冲突而非覆盖",
            head=head, delivery_id=plan["delivery_id"],
        )

    # 4) 回执与成品都不存在：之前从未提交成功，按原 expected_generation 重新提交
    rc = attempt_complete(ctx, plan, upload)
    if rc == UNCERTAIN:
        return _uncertain_output(plan, upload, run)
    return ok("恢复后重新 complete 成功", run=run, **_completed_view(upload))


def _uncertain_output(plan: dict, upload: dict, run: dict) -> int:
    payload = {
        "status": "uncertain",
        "exit_code": UNCERTAIN,
        "message": "complete 提交后状态未知（异常不能证明对象有无）；请执行 resume，"
                   "工具将查交付回执/成品身份后收敛，不会盲目新建对象",
        "delivery_id": plan["delivery_id"],
        "key": plan["key"],
        "upload_id": upload["upload_id"],
        "run": run,
    }
    print(json.dumps(payload, indent=2, ensure_ascii=False))
    return UNCERTAIN


def _completed_view(upload: dict) -> dict:
    return {
        "object_version": upload["object_version"],
        "delivery_id": upload["delivery_id"],
        "status": upload["status"],
        "adopted_via": upload.get("adopted_via"),
        "bytes_sent_total": upload.get("bytes_sent_total", 0),
    }


# --------------------------- publish / verify ---------------------------

def load_catalog(path: Path) -> dict:
    if path.exists():
        return read_json(path)
    return {"entries": []}


def cmd_publish(args) -> int:
    ctx = Context.build(args)
    plan = ctx.load_plan()
    upload = ctx.load_upload() or die(USAGE, "无上传日志")
    catalog_path = Path(args.catalog).resolve()

    if not upload.get("object_version"):
        die(STATE_DRIFT, "尚无已完成的对象版本，先 upload/resume")

    # 发布前独立强校验：按不可变 version 读回整对象字节并核对身份
    version = upload["object_version"]
    data = ctx.store.read(version)
    digest = sha256_bytes(data)
    if digest != plan["source_sha256"] or len(data) != plan["size"]:
        die(VERIFY_FAILED, "对象版本字节身份校验失败，拒绝发布", version=version)
    receipt = ctx.store.receipt(plan["delivery_id"])
    if receipt is None or receipt["version"] != version:
        die(FOREIGN_OBJECT, "交付回执与待发布版本不一致，拒绝发布")

    catalog = load_catalog(catalog_path)
    for entry in catalog["entries"]:
        if entry["delivery_id"] == plan["delivery_id"]:
            if entry["object_version"] == version and entry["source_sha256"] == digest:
                return ok("catalog 已含同一交付的同一版本，幂等返回，未重传任何对象字节",
                          catalog=str(catalog_path), entry=entry)
            die(STATE_DRIFT, "catalog 中该 delivery 已绑定其他版本，不可改写",
                existing=entry)

    # 同 key 允许新交付替换：追加新条目，旧条目及其引用的旧 version 原样保留
    entry = {
        "delivery_id": plan["delivery_id"],
        "key": plan["key"],
        "object_version": version,            # 读者按不可变 version 读，不按可变 key 猜
        "source_sha256": digest,
        "size": len(data),
        "published_at": time.time(),
    }
    catalog["entries"].append(entry)

    catalog_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = catalog_path.with_name(catalog_path.name + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(catalog, f, indent=2, ensure_ascii=False)
        f.flush()
        os.fsync(f.fileno())
    if os.environ.get("DELIVERY_CRASH_BEFORE_CATALOG_RENAME"):
        sys.stderr.write("[fault] catalog 临时文件已落盘，rename 前模拟硬中断（exit 137）\n")
        os._exit(137)
    os.replace(tmp, catalog_path)  # 原子发布：成功前旧 catalog 一直可读
    dir_fd = os.open(catalog_path.parent, os.O_RDONLY)
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)

    upload["status"] = "published"
    ctx.save_upload(upload)
    return ok("catalog 已发布（追加条目，旧版本引用保持不动）",
              catalog=str(catalog_path), entry=entry,
              catalog_entries=len(catalog["entries"]))


def parse_csv_rows(data: bytes) -> list[list[str]]:
    return list(csv.reader(io.StringIO(data.decode("utf-8"))))


def cmd_verify(args) -> int:
    ctx = Context.build(args)
    plan = ctx.load_plan()
    catalog_path = Path(args.catalog).resolve()
    if not catalog_path.exists():
        die(USAGE, f"catalog 不存在: {catalog_path}")
    catalog = load_catalog(catalog_path)
    entries = [e for e in catalog["entries"] if e["delivery_id"] == plan["delivery_id"]]
    if not entries:
        die(VERIFY_FAILED, "catalog 中找不到该 delivery 的发布条目")
    entry = entries[-1]

    # 独立从不可变 version 读回全部字节——不看 etag 字符串，不按 key 读当前代
    data = ctx.store.read(entry["object_version"])
    actual_digest = sha256_bytes(data)
    checks = {
        "version_exists": True,
        "length_ok": len(data) == entry["size"] == plan["size"],
        "sha256_ok": actual_digest == entry["source_sha256"] == plan["source_sha256"],
    }
    source = ctx.revalidate_source(plan, args.csv)
    source_rows = parse_csv_rows(source.read_bytes())
    version_rows = parse_csv_rows(data)
    checks["csv_parse_ok"] = version_rows == source_rows
    checks["csv_rows"] = len(version_rows)

    head = ctx.store.head(plan["key"])
    checks["head_version"] = head["version"] if head else None
    checks["catalog_version"] = entry["object_version"]
    # head 反映可变 key 的当前代；读者始终按 catalog 中的不可变 version 读取，
    # 即使当前代已被别的交付替换，本版本字节依旧可读、不可变（S6 由演练断言）。

    if all(v for k, v in checks.items() if k.endswith("_ok")):
        return ok("独立读回校验全部通过：长度、整对象 SHA256、CSV 解析均一致",
                  checks=checks, entry=entry)
    die(VERIFY_FAILED, "校验失败", checks=checks, entry=entry)


# --------------------------- CLI ---------------------------

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="本地 CSV 分段上传与发布工具")
    p.add_argument("--state-dir", required=True, help="本地持久日志目录")
    p.add_argument("--store-root", required=True, help="对象服务模拟器数据根目录")
    p.add_argument("--store-lib", help="multipart_store.py 路径（默认同目录）")
    sub = p.add_subparsers(dest="cmd", required=True)

    sp = sub.add_parser("plan", help="计算并持久化交付计划")
    sp.add_argument("--csv", required=True)
    sp.add_argument("--delivery", required=True)
    sp.add_argument("--key", required=True)
    sp.add_argument("--part-bytes", type=int, required=True)
    sp.add_argument("--expected-generation", type=int, default=0)
    sp.set_defaults(func=cmd_plan)

    su = sub.add_parser("upload", help="分段上传并 complete（可重复执行）")
    su.add_argument("--csv", help="源文件路径变更时可覆盖，但字节身份必须一致")
    su.set_defaults(func=cmd_upload)

    sr = sub.add_parser("resume", help="中断/不确定状态收敛，不盲目新建对象")
    sr.add_argument("--csv")
    sr.set_defaults(func=cmd_resume)

    spub = sub.add_parser("publish", help="把交付绑定到不可变 object_version 并写入 catalog")
    spub.add_argument("--catalog", required=True)
    spub.set_defaults(func=cmd_publish)

    sv = sub.add_parser("verify", help="按 catalog 的 version 独立读回并校验字节与 CSV")
    sv.add_argument("--catalog", required=True)
    sv.add_argument("--csv")
    sv.set_defaults(func=cmd_verify)
    return p


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return args.func(args)
    except BrokenPipeError:
        return UNCERTAIN


if __name__ == "__main__":
    sys.exit(main())
