"""命令行入口：submit / read / cancel / status / evict。

服务以长驻进程方式运行，命令通过本地 JSON 行协议在 Unix 域套接字上交互
（仅本机、无 HTTP、无账号）。

用法：
  python3.11 cli.py serve [--spill DIR]            # 启动服务（前台）
  python3.11 cli.py submit QUERY_ID JSON_PARAMS    # 提交，打印 consumer_id
  python3.11 cli.py read CONSUMER_ID MAX_ROWS      # 顺序读取片段
  python3.11 cli.py cancel CONSUMER_ID             # 取消自身订阅
  python3.11 cli.py status                         # 物理查询/消费者/资源状态
  python3.11 cli.py evict                          # 确定淘汰一个空闲完成查询
  python3.11 cli.py audit                          # 资源不变量审计

参数按 queries.json 的声明类型从 JSON 解析（JSON 数字→INTEGER，字符串→TEXT）。
也可在代码中直接 import service.QueryService 使用可调用 API（见 demo.py）。
"""
from __future__ import annotations

import argparse
import json
import socket
import sys
from pathlib import Path

from catalog import Catalog
from service import QueryService, file_sha256

ROOT = Path(__file__).resolve().parent
SOCK = ROOT / "service.sock"
CONFIG = json.loads((ROOT / "config.json").read_text())


class SocketServer:
    """单连接串行处理的本机控制面；数据面 API 本身是线程安全的。"""

    def __init__(self, svc: QueryService, sock_path: Path):
        self.svc = svc
        self.sock_path = sock_path

    def serve_forever(self) -> None:
        self.sock_path.unlink(missing_ok=True)
        srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        srv.bind(str(self.sock_path))
        srv.listen(8)
        print(f"服务已启动: {self.sock_path} (快照 {self.svc.snapshot_digest[:12]}…)")
        try:
            while True:
                conn, _ = srv.accept()
                with conn:
                    self._handle(conn)
        except KeyboardInterrupt:
            pass
        finally:
            srv.close()
            self.sock_path.unlink(missing_ok=True)
            self.svc.close()

    def _handle(self, conn: socket.socket) -> None:
        buf = b""
        while not buf.endswith(b"\n"):
            chunk = conn.recv(65536)
            if not chunk:
                return
            buf += chunk
        try:
            cmd = json.loads(buf.decode("utf-8"))
            resp = self.dispatch(cmd)
        except Exception as exc:  # noqa: BLE001
            resp = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
        conn.sendall((json.dumps(resp, ensure_ascii=False, default=str) + "\n")
                     .encode("utf-8"))

    def dispatch(self, cmd: dict) -> dict:
        op = cmd["op"]
        if op == "submit":
            r = self.svc.submit(cmd["query_id"], cmd["params"])
            return {"ok": r["accepted"], **r}
        if op == "read":
            rr = self.svc.read(cmd["consumer_id"], int(cmd["max_rows"]),
                               timeout=float(cmd.get("timeout", 30)))
            return {"ok": True, "rows": rr.rows, "state": rr.state,
                    "row_offset": rr.row_offset, "reason": rr.reason,
                    "terminal": rr.terminal}
        if op == "cancel":
            return {"ok": True, **self.svc.cancel(cmd["consumer_id"])}
        if op == "status":
            return {"ok": True, **self.svc.status()}
        if op == "evict":
            return {"ok": True, **self.svc.evict()}
        if op == "audit":
            return {"ok": True, **self.svc.audit()}
        return {"ok": False, "error": f"未知命令 {op!r}"}


def client_call(payload: dict) -> dict:
    if not SOCK.exists():
        raise SystemExit(f"找不到服务套接字 {SOCK}，请先运行: python3.11 cli.py serve")
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as cli:
        cli.connect(str(SOCK))
        cli.sendall((json.dumps(payload, ensure_ascii=False) + "\n").encode("utf-8"))
        buf = b""
        while True:
            chunk = cli.recv(65536)
            if not chunk:
                break
            buf += chunk
            if buf.endswith(b"\n"):
                break
    return json.loads(buf.decode("utf-8"))


def main() -> int:
    ap = argparse.ArgumentParser(description="本地共享查询结果服务 CLI")
    sub = ap.add_subparsers(dest="op", required=True)

    p_serve = sub.add_parser("serve")
    p_serve.add_argument("--spill", default=str(ROOT / "spill"))

    p_sub = sub.add_parser("submit")
    p_sub.add_argument("query_id")
    p_sub.add_argument("params", help="JSON 数组，如 [\"g1\"] 或 [100]")

    p_read = sub.add_parser("read")
    p_read.add_argument("consumer_id")
    p_read.add_argument("max_rows", type=int)

    p_cancel = sub.add_parser("cancel")
    p_cancel.add_argument("consumer_id")

    sub.add_parser("status")
    sub.add_parser("evict")
    sub.add_parser("audit")

    args = ap.parse_args()

    if args.op == "serve":
        cat = Catalog(ROOT / "queries.json")
        svc = QueryService(
            ROOT / "snapshot.sqlite", cat,
            pool_bytes=CONFIG["pool_bytes"],
            resident_rows=CONFIG["resident_rows"],
            max_consumers=CONFIG["max_consumers"],
            spill_dir=Path(args.spill))
        SocketServer(svc, SOCK).serve_forever()
        return 0

    if args.op == "submit":
        params = json.loads(args.params)
        if not isinstance(params, list):
            raise SystemExit("params 必须是 JSON 数组")
        resp = client_call({"op": "submit", "query_id": args.query_id,
                            "params": params})
    elif args.op == "read":
        resp = client_call({"op": "read", "consumer_id": args.consumer_id,
                            "max_rows": args.max_rows})
    elif args.op == "cancel":
        resp = client_call({"op": "cancel", "consumer_id": args.consumer_id})
    else:
        resp = client_call({"op": args.op})

    print(json.dumps(resp, ensure_ascii=False, indent=2, default=str))
    return 0 if resp.get("ok") else 2


if __name__ == "__main__":
    sys.exit(main())
