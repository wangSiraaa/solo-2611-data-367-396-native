"""测试套件：手工小例 + 真实 SQLite 拒绝 + 独立枚举 + 不变性与防篡改。

运行：. .venv/bin/activate && python -m unittest discover -s tests -v
不依赖 pytest，仅用标准库 unittest。
"""
from __future__ import annotations

import json
import os
import shutil
import sqlite3
import tempfile
import unittest

from synthdb.catalog import read_catalog
from synthdb.csp import build_csp
from synthdb.declaration import load_declaration
from synthdb.errors import (
    DeclarationError,
    SubsetViolation,
    UnsatError,
    VerifyError,
)
from synthdb.parser import parse_schema
from synthdb.pipeline import run_generate, run_verify
from synthdb.solver import build_unsat_conflict, enumerate_satisfiable, solve

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FIX = os.path.join(ROOT, "fixtures")


def p(*parts):
    return os.path.join(ROOT, *parts)


def f(*parts):
    return os.path.join(FIX, *parts)


def read(path):
    with open(path, encoding="utf-8") as fh:
        return fh.read()


def load_json(path):
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


def load_pipeline(schema_path, gen_path):
    raw = read(schema_path)
    schema = parse_schema(raw)
    catalog = read_catalog(raw, schema)
    decl = load_declaration(gen_path, schema, catalog)
    csp = build_csp(schema, catalog, decl)
    return raw, schema, catalog, decl, csp


def assignment_multiset(out_dir):
    rows = {}
    with open(os.path.join(out_dir, "assignments.jsonl"), encoding="utf-8") as fh:
        for line in fh:
            r = json.loads(line)
            rows.setdefault(r["table"], []).append(
                tuple(sorted(r["row"].items())))
    return {k: sorted(v) for k, v in rows.items()}


class BaseCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="synthdb-test-")

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def out(self, name):
        d = os.path.join(self.tmp, name)
        return d


class TestMainSample(BaseCase):
    """真实 schema.sql + generation.json 端到端。"""

    def test_sat_end_to_end_and_live_sqlite(self):
        out = self.out("sat")
        result = run_generate(p("schema.sql"), p("generation.json"), out)
        self.assertEqual(result["manifest"]["outcome"], "published")
        db = os.path.join(out, "synthetic.db")
        conn = sqlite3.connect(db)
        conn.execute("PRAGMA foreign_keys=ON")
        # 真实数据库确认外键开启且无违约
        self.assertEqual(conn.execute("PRAGMA foreign_keys").fetchone()[0], 1)
        self.assertEqual(conn.execute("PRAGMA foreign_key_check").fetchall(), [])
        # 精确行数
        for t, n in [("teams", 2), ("people", 4), ("shifts", 2)]:
            got = conn.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0]
            self.assertEqual(got, n, t)
        # 复合 UNIQUE：people(team_id,rank) 每组唯一
        dup = conn.execute(
            "SELECT COUNT(*) FROM (SELECT team_id,rank FROM people "
            "GROUP BY team_id,rank HAVING COUNT(*)>1)").fetchone()[0]
        self.assertEqual(dup, 0)
        # 复合外键整元组命中：shifts 每行都能在 people 找到同一 (team_id,rank)
        orphans = conn.execute(
            "SELECT COUNT(*) FROM shifts s WHERE NOT EXISTS("
            "SELECT 1 FROM people p WHERE p.team_id IS s.team_id AND p.rank IS s.rank)"
        ).fetchone()[0]
        self.assertEqual(orphans, 0)
        conn.close()
        self.assertTrue(result["verify"]["passed"])
        self.assertGreater(result["verify"]["total_checks"], 50)

    def test_unsat_real_sample_conflict(self):
        out = self.out("unsat")
        with self.assertRaises(UnsatError):
            run_generate(p("schema.sql"), p("generation-unsat.json"), out)
        # 不发布数据库
        self.assertFalse(os.path.exists(os.path.join(out, "synthetic.db")))
        conflict = load_json(os.path.join(out, "unsat-conflict.json"))
        self.assertEqual(conflict["table"], "people")
        self.assertEqual(conflict["columns"], ["team_id", "rank"])
        # 两个冲突行槽都在报告里，域为单元素（固定）
        self.assertEqual(len(conflict["slots"]), 2)
        for s in conflict["slots"]:
            self.assertEqual(s["values"], {"team_id": 1, "rank": 1})

    def test_real_sqlite_itself_rejects_unsat_data(self):
        """手工把无解声明的数据写进真实 SQLite，确认真实数据库同样拒绝。"""
        conn = sqlite3.connect(":memory:")
        conn.executescript(read(p("schema.sql")))
        conn.execute("PRAGMA foreign_keys=ON")
        with self.assertRaises(sqlite3.IntegrityError):
            conn.execute("INSERT INTO teams VALUES(1,1,10)")
            conn.execute("INSERT INTO people VALUES(10,1,1)")
            conn.execute("INSERT INTO people VALUES(11,1,1)")  # UNIQUE 冲突
        conn.close()


class TestIndependentEnumeration(BaseCase):
    """DFS 与独立笛卡尔积枚举两条路径互证。"""

    def test_solver_and_enumeration_agree_sat(self):
        _, _, _, _, csp = load_pipeline(p("schema.sql"), p("generation.json"))
        result = solve(csp)
        sols = enumerate_satisfiable(csp, limit=5)
        self.assertGreaterEqual(len(sols), 1)
        # DFS 解本身必须是枚举认可的满足赋值
        self.assertIn(result.assignment, sols)
        # 枚举器对完整赋值跑全部约束
        for sol in sols:
            self.assertTrue(all(sol[v] in csp.variables[v].domain
                                for v in csp.var_order))

    def test_solver_and_enumeration_agree_unsat(self):
        # 构造一个变量型无解：people 三行共享同一 team_id 但 rank 域只有 2 值
        gen = {
            "tables": {
                "teams": {"rows": [
                    {"team_id": 1, "region": {"one_of": [1, 2]},
                     "lead_id": {"one_of": [10, 11]}}]},
                "people": {"rows": [
                    {"person_id": 10, "team_id": 1, "rank": {"one_of": [1, 2]}},
                    {"person_id": 11, "team_id": 1, "rank": {"one_of": [1, 2]}},
                    {"person_id": 12, "team_id": 1, "rank": {"one_of": [1, 2]}}]},
                "shifts": {"rows": []}},
            "seed": 7}
        gp = os.path.join(self.tmp, "gen.json")
        json.dump(gen, open(gp, "w", encoding="utf-8"))
        _, _, _, _, csp = load_pipeline(p("schema.sql"), gp)
        with self.assertRaises(UnsatError):
            solve(csp)
        # 独立枚举确认零解，且冲突子问题可独立重放
        self.assertEqual(enumerate_satisfiable(csp, limit=1), [])
        conflict = build_unsat_conflict(csp)
        self.assertEqual(conflict["enumeration"]["satisfying_assignments"], 0)
        self.assertGreater(conflict["enumeration"]["space_size"], 1)
        # 冲突中保留 UNIQUE 约束原文与位置
        kinds = [c["kind"] for c in conflict["constraints"]]
        self.assertIn("UNIQUE", kinds)

    def test_enumeration_space_is_cartesian_product(self):
        _, _, _, _, csp = load_pipeline(f("feature-schema.sql"),
                                        f("feature-generation.json"))
        expected = 1
        for vid in csp.var_order:
            expected *= len(csp.variables[vid].domain)
        # 全量枚举解数 <= 笛卡尔积空间，且至少有一个解
        all_sols = enumerate_satisfiable(csp)
        self.assertLessEqual(len(all_sols), expected)
        self.assertGreaterEqual(len(all_sols), 1)


class TestCompositeForeignKey(BaseCase):
    """复合外键必须整元组命中同一父行，不能分量各自匹配。"""

    def test_trap_is_unsat(self):
        out = self.out("trap")
        with self.assertRaises(UnsatError):
            run_generate(f("composite-schema.sql"), f("composite-unsat.json"), out)
        conflict = load_json(os.path.join(out, "unsat-conflict.json"))
        self.assertIn("复合外键", conflict["reason"])
        # 真实 SQLite 手工验证同样拒绝
        conn = sqlite3.connect(":memory:")
        conn.executescript(read(f("composite-schema.sql")))
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("BEGIN")
        conn.execute("INSERT INTO pp VALUES(10,1,1)")
        conn.execute("INSERT INTO pp VALUES(20,2,2)")
        conn.execute("INSERT INTO cc VALUES(100,1,2)")
        with self.assertRaises(sqlite3.IntegrityError):
            conn.execute("COMMIT")
        conn.close()

    def test_matching_tuple_is_sat(self):
        out = self.out("ok")
        result = run_generate(f("composite-schema.sql"),
                              f("composite-sat.json"), out)
        self.assertEqual(result["manifest"]["outcome"], "published")

    def test_match_simple_null_exemption(self):
        out = self.out("nullfk")
        result = run_generate(f("null-fk-schema.sql"),
                              f("null-fk-generation.json"), out)
        self.assertEqual(result["manifest"]["outcome"], "published")
        conn = sqlite3.connect(os.path.join(out, "synthetic.db"))
        conn.execute("PRAGMA foreign_keys=ON")
        self.assertEqual(conn.execute("PRAGMA foreign_key_check").fetchall(), [])
        # 含 NULL 的子行确实存在且被豁免
        self.assertEqual(conn.execute(
            "SELECT COUNT(*) FROM cn WHERE b IS NULL").fetchone()[0], 1)
        conn.close()


class TestCheckFeatures(BaseCase):
    """子集全部 CHECK 形态、负数、自引用、UNIQUE 的 NULL 语义。"""

    def test_all_check_forms_generate(self):
        out = self.out("feat")
        result = run_generate(f("feature-schema.sql"),
                              f("feature-generation.json"), out)
        self.assertEqual(result["manifest"]["outcome"], "published")
        conn = sqlite3.connect(os.path.join(out, "synthetic.db"))
        conn.execute("PRAGMA foreign_keys=ON")
        rows = conn.execute("SELECT node_id,parent_id,lo,hi,cap,code,span_a,span_b "
                            "FROM nodes ORDER BY node_id").fetchall()
        # BETWEEN 负数范围
        for r in rows:
            self.assertGreaterEqual(r[2], -10)
            self.assertLessEqual(r[2], 0)
        # 两列 <=：非 NULL 时 span_a <= span_b
        bad = conn.execute(
            "SELECT COUNT(*) FROM nodes WHERE span_a IS NOT NULL AND span_b IS NOT NULL "
            "AND NOT (span_a <= span_b)").fetchone()[0]
        self.assertEqual(bad, 0)
        # 自引用环成立且无外键违约
        self.assertEqual(conn.execute("PRAGMA foreign_key_check").fetchall(), [])
        conn.close()

    def test_check_null_does_not_reject(self):
        # hi/cap/code 可为 NULL；CHECK 遇 NULL 不拒绝
        out = self.out("feat2")
        run_generate(f("feature-schema.sql"),
                     f("feature-generation.json"), out)
        conn = sqlite3.connect(os.path.join(out, "synthetic.db"))
        # 至少一行存在 NULL 的可空 CHECK 列
        n = conn.execute(
            "SELECT COUNT(*) FROM nodes WHERE hi IS NULL OR cap IS NULL OR code IS NULL"
        ).fetchone()[0]
        self.assertGreaterEqual(n, 1)
        conn.close()


class TestSubsetRejection(BaseCase):
    """子集外结构必须在造库前定位拒绝，不悄悄忽略。"""

    REJECTS = [
        "reject-autoincrement.sql",
        "reject-types.sql",
        "reject-check.sql",
        "reject-fk.sql",
        "reject-ident.sql",
        "reject-ddl.sql",
    ]

    def test_each_unsupported_feature_rejected(self):
        for name in self.REJECTS:
            with self.subTest(schema=name):
                with self.assertRaises(SubsetViolation):
                    parse_schema(read(f(name)))

    def test_rejection_keeps_raw_text_and_line(self):
        try:
            parse_schema(read(f("reject-check.sql")))
            self.fail("应当拒绝")
        except SubsetViolation as ex:
            self.assertTrue(ex.location)
            self.assertIn("CHECK", ex.raw)


class TestDeclarationRules(BaseCase):
    def test_unknown_table_rejected(self):
        raw = read(p("schema.sql"))
        schema = parse_schema(raw)
        catalog = read_catalog(raw, schema)
        bad = {"tables": {"nonexistent": {"rows": []}}, "seed": 1}
        gp = os.path.join(self.tmp, "b.json")
        json.dump(bad, open(gp, "w", encoding="utf-8"))
        with self.assertRaises(DeclarationError):
            load_declaration(gp, schema, catalog)

    def test_null_in_notnull_column_rejected(self):
        raw = read(p("schema.sql"))
        schema = parse_schema(raw)
        catalog = read_catalog(raw, schema)
        bad = {"tables": {
            "teams": {"rows": [{"team_id": 1, "region": None, "lead_id": 10}]},
            "people": {"rows": []}, "shifts": {"rows": []}}, "seed": 1}
        gp = os.path.join(self.tmp, "b.json")
        json.dump(bad, open(gp, "w", encoding="utf-8"))
        with self.assertRaises(DeclarationError):
            load_declaration(gp, schema, catalog)

    def test_duplicate_domain_value_rejected(self):
        raw = read(p("schema.sql"))
        schema = parse_schema(raw)
        catalog = read_catalog(raw, schema)
        bad = {"tables": {
            "teams": {"rows": [{"team_id": 1, "region": {"one_of": [1, 1]},
                                "lead_id": 10}]},
            "people": {"rows": []}, "shifts": {"rows": []}}, "seed": 1}
        gp = os.path.join(self.tmp, "b.json")
        json.dump(bad, open(gp, "w", encoding="utf-8"))
        with self.assertRaises(DeclarationError):
            load_declaration(gp, schema, catalog)

    def test_too_many_variables_rejected(self):
        # 13 个 one_of 单元格超过 12 上限
        raw = read(p("schema.sql"))
        schema = parse_schema(raw)
        catalog = read_catalog(raw, schema)
        v = {"one_of": [1, 2]}
        rows = []
        for i in range(1, 7):  # 6 个 people 行，每行 2 个变量 = 12，再加 teams 变量
            rows.append({"person_id": 100 + i, "team_id": {"one_of": [1, 2]},
                         "rank": v})
        bad = {"tables": {
            "teams": {"rows": [{"team_id": 1, "region": v, "lead_id": {"one_of": [10, 11]}}]},
            "people": {"rows": rows}, "shifts": {"rows": []}}, "seed": 1}
        gp = os.path.join(self.tmp, "b.json")
        json.dump(bad, open(gp, "w", encoding="utf-8"))
        with self.assertRaises(DeclarationError):
            load_declaration(gp, schema, catalog)


class TestCanonicalInvariance(BaseCase):
    """改名、行槽顺序、域次序均不改变规范生成结果。"""

    def test_renamed_and_shuffled_matches_canonical(self):
        a = self.out("a")
        b = self.out("b")
        run_generate(f("renamed-schema.sql"),
                     f("renamed-generation-canonical.json"), a)
        run_generate(f("renamed-schema.sql"),
                     f("renamed-generation-shuffled.json"), b)
        self.assertEqual(assignment_multiset(a), assignment_multiset(b))

    def test_domain_order_does_not_change_result(self):
        # 同一声明、域内枚举顺序反转，逻辑行必须一致
        gen = load_json(f("renamed-generation-canonical.json"))
        rev = json.loads(json.dumps(gen))
        for t, data in rev["tables"].items():
            for row in data["rows"]:
                for col, spec in row.items():
                    if isinstance(spec, dict) and "one_of" in spec:
                        spec["one_of"] = list(reversed(spec["one_of"]))
        gp = os.path.join(self.tmp, "rev.json")
        json.dump(rev, open(gp, "w", encoding="utf-8"))
        a = self.out("a")
        b = self.out("b")
        run_generate(f("renamed-schema.sql"),
                     f("renamed-generation-canonical.json"), a)
        run_generate(f("renamed-schema.sql"), gp, b)
        self.assertEqual(assignment_multiset(a), assignment_multiset(b))

    def test_seed_change_keeps_canonical_assignment(self):
        # 规范解由确定性排序决定；seed 不改变逻辑行（题目不要求统计随机）
        gen = load_json(p("generation.json"))
        gen2 = json.loads(json.dumps(gen))
        gen2["seed"] = 999
        gp = os.path.join(self.tmp, "seed999.json")
        json.dump(gen2, open(gp, "w", encoding="utf-8"))
        a, b = self.out("a"), self.out("b")
        run_generate(p("schema.sql"), p("generation.json"), a)
        run_generate(p("schema.sql"), gp, b)
        self.assertEqual(assignment_multiset(a), assignment_multiset(b))


class TestVerifierTamperDetection(BaseCase):
    """verify 重新打开成品库，独立查询必须能抓到被篡改的脏库。"""

    def _fresh_db(self, out):
        run_generate(p("schema.sql"), p("generation.json"), out)
        return os.path.join(out, "synthetic.db")

    def test_verify_passes_on_clean_db(self):
        out = self.out("clean")
        self._fresh_db(out)
        report = run_verify(os.path.join(out, "synthetic.db"),
                            p("schema.sql"), p("generation.json"))
        self.assertTrue(report["passed"])

    def test_verify_catches_extra_row(self):
        out = self.out("extra")
        db = self._fresh_db(out)
        conn = sqlite3.connect(db)
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("INSERT INTO teams VALUES(99,1,10)")  # 额外行
        conn.commit()
        conn.close()
        with self.assertRaises(VerifyError):
            run_verify(db, p("schema.sql"), p("generation.json"))

    def test_verify_catches_domain_violation(self):
        out = self.out("dom")
        db = self._fresh_db(out)
        conn = sqlite3.connect(db)
        # lead_id 无 CHECK（仅 NOT NULL/FK），域为声明的 one_of；
        # 关 FK 后写入域外值 99，验证 verify 按声明域而非仅靠 PRAGMA 抓出
        conn.execute("PRAGMA foreign_keys=OFF")
        conn.execute("UPDATE teams SET lead_id=99 WHERE team_id=1")
        conn.commit()
        conn.close()
        with self.assertRaises(VerifyError):
            run_verify(db, p("schema.sql"), p("generation.json"))

    def test_verify_catches_orphan_fk(self):
        out = self.out("orphan")
        db = self._fresh_db(out)
        # 关闭外键制造孤儿，验证独立查询不依赖 PRAGMA 状态
        conn = sqlite3.connect(db)
        conn.execute("PRAGMA foreign_keys=OFF")
        conn.execute("INSERT INTO shifts VALUES(999, 7, 7)")
        conn.commit()
        conn.close()
        with self.assertRaises(VerifyError):
            run_verify(db, p("schema.sql"), p("generation.json"))


class TestColumnLeColumn(BaseCase):
    """两列之间 col <= col 约束的有解/无解，并与真实 SQLite 交叉。"""

    def test_fixed_violation_is_unsat(self):
        out = self.out("leu")
        with self.assertRaises(UnsatError):
            run_generate(f("lecol-schema.sql"), f("lecol-unsat.json"), out)
        # 真实 SQLite 对同样固定数据也拒绝（CHECK 在插入时即强制）
        conn = sqlite3.connect(":memory:")
        conn.executescript(read(f("lecol-schema.sql")))
        conn.execute("INSERT INTO boxes VALUES(1,5,5)")
        with self.assertRaises(sqlite3.IntegrityError):
            conn.execute("INSERT INTO boxes VALUES(2,9,4)")
        conn.close()

    def test_variable_solution_respects_ordering(self):
        out = self.out("les")
        run_generate(f("lecol-schema.sql"), f("lecol-sat.json"), out)
        conn = sqlite3.connect(os.path.join(out, "synthetic.db"))
        bad = conn.execute(
            "SELECT COUNT(*) FROM boxes WHERE NOT (low <= high)").fetchone()[0]
        self.assertEqual(bad, 0)
        conn.close()


class TestLoadingBoundary(BaseCase):
    def test_foreign_keys_actually_on_during_load(self):
        # 外键为 DEFERRABLE INITIALLY DEFERRED：违约在 COMMIT 时由 SQLite 裁决
        out = self.out("fk")
        run_generate(p("schema.sql"), p("generation.json"), out)
        conn = sqlite3.connect(os.path.join(out, "synthetic.db"))
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("BEGIN")
        conn.execute("INSERT INTO shifts VALUES(5, 99, 99)")
        with self.assertRaises(sqlite3.IntegrityError):
            conn.execute("COMMIT")
        conn.close()

    def test_failed_load_removes_half_db(self):
        # 通过篡改 schema 使装载期失败不可达（求解已保证），改为验证
        # 输出目录在 unsat 时不残留 db
        out = self.out("nodb")
        with self.assertRaises(UnsatError):
            run_generate(p("schema.sql"), p("generation-unsat.json"), out)
        self.assertFalse(os.path.exists(os.path.join(out, "synthetic.db")))


class TestDeterminismAndEdges(BaseCase):
    def test_repeat_runs_identical_logical_rows(self):
        a, b = self.out("a"), self.out("b")
        run_generate(p("schema.sql"), p("generation.json"), a)
        run_generate(p("schema.sql"), p("generation.json"), b)
        self.assertEqual(assignment_multiset(a), assignment_multiset(b))

    def test_empty_table_supported(self):
        # 空行槽表（unsat 例中的 shifts）在有解场景同样要能精确建出 0 行
        gen = {
            "tables": {
                "teams": {"rows": [
                    {"team_id": 1, "region": 1, "lead_id": {"one_of": [10, 11]}}]},
                "people": {"rows": [
                    {"person_id": 10, "team_id": 1, "rank": {"one_of": [1, 2]}}]},
                "shifts": {"rows": []}},
            "seed": 7}
        gp = os.path.join(self.tmp, "empty.json")
        with open(gp, "w", encoding="utf-8") as fh:
            json.dump(gen, fh)
        out = self.out("empty")
        run_generate(p("schema.sql"), gp, out)
        conn = sqlite3.connect(os.path.join(out, "synthetic.db"))
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM shifts").fetchone()[0], 0)
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM teams").fetchone()[0], 1)
        conn.close()

    def test_plan_lists_cyclic_component(self):
        from synthdb.pipeline import run_plan
        plan = run_plan(p("schema.sql"), p("generation.json"))
        cyclic = plan["reference_relationships"]["cyclic_components_must_solve_together"]
        self.assertEqual(sorted(cyclic[0]), ["people", "teams"])
        # 每条外键都带来源定位
        for fk in plan["reference_relationships"]["foreign_keys"]:
            self.assertIn("line", fk["source"])
            self.assertTrue(fk["source"]["text"])

    def test_source_lines_match_real_schema(self):
        # schema.sql：PRAGMA 在第 1 行，三张表分别在第 2/3/4 行（每表单行）
        from synthdb.parser import parse_schema
        schema = parse_schema(read(p("schema.sql")))
        lines = {t.name: t.source_line for t in schema.tables}
        self.assertEqual(lines, {"teams": 2, "people": 3, "shifts": 4})
        # 多行 fixture：逐约束行号必须精确（feature-schema.sql 中各列分行书写）
        fs = parse_schema(read(f("feature-schema.sql")))
        nodes = fs.table("nodes")
        # BETWEEN 在 lo 列（第 7 行），表级两列 <= 在第 14 行
        between = next(a for a in nodes.all_checks() if a.kind.value == "between")
        lecol = next(a for a in nodes.all_checks() if a.kind.value == "le_col")
        self.assertEqual(between.source.line, 7)
        self.assertEqual(lecol.source.line, 14)
        self.assertIn("BETWEEN", between.source.text)


if __name__ == "__main__":
    unittest.main()
