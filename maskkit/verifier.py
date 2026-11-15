"""verify 阶段：独立重开 release.sqlite 全面核查。

不把“PRAGMA foreign_key_check 为空”当作令牌一一性的替代：
  - 逐令牌核对格式（前缀/字母表/长度）；
  - 每个域内令牌集合与 vault 权威映射做双向集合比对（一一性）；
  - 结合本 release 私有清单核对原值->令牌仍指向同一令牌（跨批稳定性）；
  - 真实执行 NOT NULL / UNIQUE / FK / 行数 / 回执哈希检查。
"""

import json
import re
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path

from .errors import MaskKitError
from .vault import TokenVault, STATE_COMPLETE
from .util import sha256_file


class VerificationError(MaskKitError):
    exit_code = 5


@dataclass
class VerificationReport:
    ok: bool
    release_id: str
    checks: list[str] = field(default_factory=list)
    violations: list[str] = field(default_factory=list)
    domain_stats: dict = field(default_factory=dict)

    def add(self, ok, code, detail):
        line = f"{'PASS' if ok else 'FAIL'} {code}: {detail}"
        self.checks.append(line)
        if not ok:
            self.violations.append(line)
        return ok


def token_regex(spec):
    return re.compile(
        "^" + re.escape(spec.prefix)
        + "[" + re.escape(spec.alphabet) + "]{" + str(spec.width) + "}$"
    )


def verify_release(release_db, receipt_path, vault: TokenVault, source_for_crosscheck=None):
    release_db = Path(release_db)
    receipt_path = Path(receipt_path)
    report = VerificationReport(ok=True, release_id="")

    with open(receipt_path, "r", encoding="utf-8") as fh:
        receipt = json.load(fh)
    report.release_id = receipt["release_id"]

    # V01 成品文件哈希与公开回执一致（回执本身不含原值）。
    actual_sha = sha256_file(release_db)
    report.add(
        actual_sha == receipt["output_sha256"],
        "V01",
        f"output_sha256 release={receipt['release_id']}",
    )

    conn = sqlite3.connect(f"file:{release_db}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        _verify_schema_and_constraints(conn, receipt, report)
        _verify_tokens(conn, receipt, vault, report)
        _verify_vault_state(receipt, vault, report)
    finally:
        conn.close()

    report.ok = not report.violations
    return report


def _verify_schema_and_constraints(conn, receipt, report):
    # V02 integrity_check。
    integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
    report.add(integrity == "ok", "V02", f"integrity_check={integrity}")

    # V03 外键违反（真实遍历，而不是仅看声明）。
    fk_violations = list(conn.execute("PRAGMA foreign_key_check"))
    report.add(
        not fk_violations,
        "V03",
        f"foreign_key_check violations={len(fk_violations)}",
    )

    # V04 声明列集合与回执一致，且不存在未登记的额外列。
    expected_tables = set(receipt["tables"])
    actual_tables = {
        r[0]
        for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name NOT LIKE 'sqlite_%'"
        )
    }
    report.add(
        expected_tables == actual_tables,
        "V04",
        f"table_set expected={sorted(expected_tables)} actual={sorted(actual_tables)}",
    )

    for tname, spec in receipt["tables"].items():
        actual_cols = [r["name"] for r in conn.execute(f'PRAGMA table_info("{tname}")')]
        report.add(
            actual_cols == spec["columns"],
            "V04",
            f"column_set table={tname}",
        )

        # V05 NOT NULL：直接尝试统计空值（含空串以外的 NULL）。
        notnull_cols = [
            r["name"] for r in conn.execute(f'PRAGMA table_info("{tname}")')
            if r["notnull"]
        ]
        for col in notnull_cols:
            n = conn.execute(
                f'SELECT COUNT(*) FROM "{tname}" WHERE "{col}" IS NULL'
            ).fetchone()[0]
            report.add(n == 0, "V05", f"NOT NULL table={tname} column={col}")

        # V06 UNIQUE：对所有唯一索引列做去重计数比对。
        for ir in conn.execute(f'PRAGMA index_list("{tname}")'):
            if not ir["unique"]:
                continue
            idx_cols = [r["name"] for r in conn.execute(
                f'PRAGMA index_info("{ir["name"]}")'
            )]
            col_sql = ",".join(f'"{c}"' for c in idx_cols)
            total = conn.execute(f'SELECT COUNT(*) FROM "{tname}"').fetchone()[0]
            distinct = conn.execute(
                f'SELECT COUNT(*) FROM (SELECT DISTINCT {col_sql} FROM "{tname}")'
            ).fetchone()[0]
            report.add(
                total == distinct,
                "V06",
                f"UNIQUE table={tname} index={ir['name']} columns={idx_cols}",
            )

        # V07 主键非空且唯一。
        pk_cols = [
            r["name"] for r in sorted(
                conn.execute(f'PRAGMA table_info("{tname}")'),
                key=lambda r: r["pk"],
            ) if r["pk"] > 0
        ]
        if pk_cols:
            col_sql = ",".join(f'"{c}"' for c in pk_cols)
            total = conn.execute(f'SELECT COUNT(*) FROM "{tname}"').fetchone()[0]
            distinct = conn.execute(
                f'SELECT COUNT(*) FROM (SELECT DISTINCT {col_sql} FROM "{tname}")'
            ).fetchone()[0]
            nulls = conn.execute(
                f'SELECT COUNT(*) FROM "{tname}" WHERE '
                + " OR ".join(f'"{c}" IS NULL' for c in pk_cols)
            ).fetchone()[0]
            report.add(
                total == distinct and nulls == 0,
                "V07",
                f"PRIMARY KEY table={tname} columns={pk_cols}",
            )


def _verify_tokens(conn, receipt, vault: TokenVault, report):
    with vault.locked() as conn_v:
        # V08 release 记录确实为 complete。
        row = vault.get_release(conn_v, receipt["release_id"])
        report.add(
            row is not None and row["state"] == STATE_COMPLETE,
            "V08",
            f"release={receipt['release_id']} state="
            f"{row['state'] if row else 'missing'}",
        )

        manifest = vault.read_manifest(receipt["release_id"])

        for domain, dspec in receipt["domains"].items():
            pattern = token_regex(type("S", (), dspec)())
            # 从私有清单反推出本 release 在该域上出现过的所有令牌。
            expected_tokens = sorted(
                m["token"] for m in manifest["domains"].get(domain, [])
            )

            # 按回执显式声明的“令牌化列 -> 域”采集，不靠前缀猜测。
            found_tokens = set()
            for tname, tspec in receipt["tables"].items():
                for col_decl in tspec.get("tokenize_columns", []):
                    if col_decl["domain"] != domain:
                        continue
                    col = col_decl["name"]
                    for (tok,) in conn.execute(
                        f'SELECT DISTINCT "{col}" FROM "{tname}" '
                        f'WHERE "{col}" IS NOT NULL'
                    ):
                        # V09 格式：前缀/字母表/长度。
                        report.add(
                            isinstance(tok, str) and bool(pattern.match(tok)),
                            "V09",
                            f"token_format domain={domain} table={tname} column={col}",
                        )
                        found_tokens.add(tok)

            # V10 一一性：成品出现的令牌集合 == 本 release 清单集合。
            report.add(
                found_tokens == set(expected_tokens),
                "V10",
                f"token_set_equality domain={domain} "
                f"release_tokens={len(expected_tokens)} db_tokens={len(found_tokens)}",
            )

            # V11 域内唯一：vault 中同域令牌不得重复（DB UNIQUE 已保证，
            # 这里显式计数，不以 FK 检查代替）。
            rows = vault.mappings_for_domain(conn_v, domain)
            tokens = [r["token"] for r in rows]
            report.add(
                len(tokens) == len(set(tokens)),
                "V11",
                f"vault_token_uniqueness domain={domain} count={len(tokens)}",
            )

            # V12 跨批稳定性：清单中每个原值当前仍映射到同一令牌。
            stable = True
            for m in manifest["domains"].get(domain, []):
                current = vault.token_for(conn_v, domain, m["value"])
                if current != m["token"]:
                    stable = False
                    break
            report.add(
                stable,
                "V12",
                f"cross_batch_stability domain={domain} "
                f"checked={len(manifest['domains'].get(domain, []))}",
            )

            # V13 成品令牌必须同时：属于本 release 清单、且在 vault 同域有背书。
            # 用本 release 清单而非全量 vault，避免跨 release 令牌被误判归属。
            manifest_tokens = set(expected_tokens)
            vault_tokens = {r["token"] for r in rows}
            report.add(
                found_tokens <= manifest_tokens <= vault_tokens,
                "V13",
                f"tokens_backed_by_release_and_vault domain={domain}",
            )

            report.domain_stats[domain] = {
                "release_tokens": len(expected_tokens),
                "vault_total": len(tokens),
            }


def _verify_vault_state(receipt, vault, report):
    # V14 指纹绑定：release 记录里的 policy/源指纹与回执一致。
    with vault.locked() as conn_v:
        row = vault.get_release(conn_v, receipt["release_id"])
        if row is None:
            report.add(False, "V14", "release record missing")
            return
        report.add(
            row["policy_fingerprint"] == receipt["policy_fingerprint"]
            and row["source_fingerprint"] == receipt["source_fingerprint"],
            "V14",
            f"fingerprint_binding release={receipt['release_id']}",
        )
        report.add(
            row["output_sha256"] == receipt["output_sha256"],
            "V14",
            f"output_hash_binding release={receipt['release_id']}",
        )
