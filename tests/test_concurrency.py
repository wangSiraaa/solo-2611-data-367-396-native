"""两个独立进程共享同一 vault 时的分配一致性测试。"""

import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from tests.helpers import BASE_POLICY, write_policy, write_source
from maskkit.vault import TokenVault


def _run_release(args):
    subprocess.run([sys.executable, "-m", "maskkit", "release",
                    "--source", args["source"],
                    "--policy", args["policy"],
                    "--vault", args["vault"],
                    "--out", args["out"]],
                   check=True, capture_output=True, text=True)


class ConcurrencyTests(unittest.TestCase):
    def test_two_processes_first_encounter_same_value(self):
        tmp = tempfile.mkdtemp()
        root = Path(tmp)
        source_a = root / "source_a.sqlite"
        source_b = root / "source_b.sqlite"
        policy = root / "policy.json"
        vault = root / "vault"
        write_policy(policy, BASE_POLICY)
        TokenVault(vault).initialize()

        # 两个源包含完全相同的合成身份集合。
        rows = (
            [("SRC_C0", "E0"), ("SRC_C1", "E1")],
            [("SRC_A0", "SRC_C0"), ("SRC_A1", "SRC_C1")],
            [("I0", "SRC_A0", "E0"), ("I1", "SRC_A1", "E1")],
        )
        write_source(source_a, *rows)
        write_source(source_b, *rows)

        out_a = root / "out_a"
        out_b = root / "out_b"
        env = dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[1]))
        procs = [
            subprocess.Popen(
                [sys.executable, "-m", "maskkit", "release",
                 "--source", str(source_a), "--policy", str(policy),
                 "--vault", str(vault), "--out", str(out_a)],
                env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            ),
            subprocess.Popen(
                [sys.executable, "-m", "maskkit", "release",
                 "--source", str(source_b), "--policy", str(policy),
                 "--vault", str(vault), "--out", str(out_b)],
                env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            ),
        ]
        outputs = [p.communicate() for p in procs]
        for p, (out, err) in zip(procs, outputs):
            self.assertEqual(p.returncode, 0, err.decode())

        # 两个成品的令牌必须完全一致（同值绝不分配两次）。
        db_a = sqlite3.connect(out_a / "release.sqlite")
        db_b = sqlite3.connect(out_b / "release.sqlite")
        for t in ("customers", "accounts", "invoices"):
            rows_a = db_a.execute(f"SELECT * FROM {t} ORDER BY 1").fetchall()
            rows_b = db_b.execute(f"SELECT * FROM {t} ORDER BY 1").fetchall()
            self.assertEqual(rows_a, rows_b)
        db_a.close()
        db_b.close()

        # vault 内每个域映射数 = 不同原值数，没有重复分配。
        v = TokenVault(vault)
        with v.locked() as conn:
            for domain in ("customer", "account", "email"):
                n = conn.execute(
                    "SELECT COUNT(*) c, COUNT(DISTINCT token) d "
                    "FROM token_mapping WHERE domain=?", (domain,)
                ).fetchone()
                self.assertEqual(n["c"], n["d"])
            self.assertEqual(
                conn.execute("SELECT COUNT(*) c FROM release_record "
                             "WHERE state='complete'").fetchone()["c"],
                2,
            )


if __name__ == "__main__":
    unittest.main()
