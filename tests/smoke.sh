#!/usr/bin/env bash
# 可复跑冒烟检查：从零构建 vault -> plan -> 两个快照发布 -> verify -> recover。
# 仅使用合成数据；运行结束打印每一步退出码。
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WORK="${ROOT}/work_smoke"
rm -rf "$WORK"
mkdir -p "$WORK/vault" "$WORK/s2" "$WORK/out1" "$WORK/out2"

echo "[1/6] init-vault"
python3 -m maskkit init-vault --vault "$WORK/vault"

echo "[2/6] plan（静态校验 + 容量预判）"
python3 -m maskkit plan --source "$ROOT/source.sqlite" \
  --policy "$ROOT/policy.json" --vault "$WORK/vault" \
  --out "$WORK/plan.json"

echo "[3/6] release 快照 A"
python3 -m maskkit release --source "$ROOT/source.sqlite" \
  --policy "$ROOT/policy.json" --vault "$WORK/vault" \
  --out "$WORK/out1"

echo "[4/6] 构造快照 B（新增一个合成客户）并发布"
python3 - "$ROOT" "$WORK" <<'PY'
import sqlite3, shutil, os, sys
root, work = sys.argv[1], sys.argv[2]
dst = os.path.join(work, "s2", "source.sqlite")
shutil.copy(os.path.join(root, "source.sqlite"), dst)
os.chmod(dst, 0o644)
c = sqlite3.connect(dst)
c.execute("INSERT INTO customers VALUES('SRC_C9','NEW_EMAIL_9')")
c.execute("INSERT INTO accounts VALUES('SRC_A9','SRC_C9')")
c.execute("INSERT INTO invoices VALUES('I9','SRC_A9','NEW_EMAIL_9')")
c.commit(); c.close()
os.chmod(dst, 0o444)
PY
python3 -m maskkit release --source "$WORK/s2/source.sqlite" \
  --policy "$ROOT/policy.json" --vault "$WORK/vault" \
  --out "$WORK/out2"

echo "[5/6] verify 两个成品"
for d in out1 out2; do
  python3 -m maskkit verify --db "$WORK/$d/release.sqlite" \
    --receipt "$WORK/$d/receipt.json" --vault "$WORK/vault" \
    | python3 -c "import json,sys;r=json.load(sys.stdin);assert r['ok'],r;print('  $d verify ok')"
done

echo "[6/6] recover（无中断时应为空动作）"
python3 -m maskkit recover --vault "$WORK/vault" \
  | python3 -c "import json,sys;r=json.load(sys.stdin);assert r['count']==0,r;print('  no stale releases')"

# 公开产物泄漏扫描
if grep -rql "SRC_\|NEW_EMAIL" "$WORK/out1" "$WORK/out2"; then
  echo "FAIL: 公开产物出现原身份值"; exit 1
fi
echo "SMOKE OK: 公开产物无原身份值，全部检查通过"
