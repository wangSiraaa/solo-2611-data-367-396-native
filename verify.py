"""端到端验证：值边界、停词间隔、重复词、非 ASCII 字节证据、
put/delete/重新 put、compact 生命周期与固定读视图并发、随机 fuzz、
非法查询拒绝，并在每个场景用 oracle 逐条比较位置组合与字节证据。
"""

from __future__ import annotations

import json
import os
import random
import shutil
import subprocess
import sys
import threading

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from docidx import Analyzer, IndexEngine, QueryError  # noqa: E402
from docidx.oracle import (  # noqa: E402
    OracleScanner, oracle_phrase, oracle_set_query,
)

WORK = os.path.join(os.path.dirname(os.path.abspath(__file__)), "index_demo")
DATA = os.path.dirname(os.path.abspath(__file__))
PASS = 0


def check(cond: bool, label: str) -> None:
    global PASS
    if not cond:
        raise AssertionError(f"断言失败: {label}")
    PASS += 1
    print(f"  ✓ {label}")


def make_engine(root: str, stopwords=("the",)) -> IndexEngine:
    if os.path.exists(root):
        shutil.rmtree(root)
    return IndexEngine(root, analyzer=Analyzer(list(stopwords)))


def active_docs(snap) -> dict:
    return {d: snap.get_doc(d) for d in snap.active_doc_ids()}


def assert_oracle(snap, active, queries) -> None:
    scanner = OracleScanner(snap.analyzer, snap.fields)
    for q in queries:
        if "terms" in q:
            exp = oracle_phrase(scanner, active, q["field"],
                                q["terms"], q.get("max_gap", 0))
            got = snap.phrase(q["field"], q["terms"], q.get("max_gap", 0))
            if _sig(exp) != _sig(got["hits"]):
                raise AssertionError(
                    f"短语 oracle 不一致\nq={q}\noracle={exp}\nindex={got}")
        else:
            exp = oracle_set_query(scanner, active, q)
            got = snap.run(q)
            if exp != set(got["docs"]):
                raise AssertionError(
                    f"集合 oracle 不一致\nq={q}\noracle={sorted(exp)}\n"
                    f"index={got['docs']}")


def _sig(hits):
    return sorted(
        (h["doc_id"], h["value_no"], tuple(h["positions"]),
         tuple(tuple(s) for s in h["byte_spans"]), tuple(h["evidence"]))
        for h in hits
    )


# ------------------------------------------------------------ 场景数据

DOCS = [
    # 两个数组元素含相同词，且都能出现 position=0：禁止跨值拼短语
    {"doc_id": "a",
     "title": ["alpha beta", "beta alpha"],
     "body": ["alpha the beta alpha beta",
              "beta alpha"]},
    # 停词占位：position 不压缩，gap 计算必须跨过 the
    {"doc_id": "b",
     "title": ["alpha the beta"],
     "body": ["alpha the the beta"]},
    # 重复词：同一出现不能喂给查询里两个重复 term
    {"doc_id": "c",
     "title": ["alpha alpha beta"],
     "body": ["alpha alpha alpha"]},
    # 非 ASCII：café、数字、组合附标无关，验证 UTF-8 字节证据
    {"doc_id": "d",
     "title": ["café résumé"],
     "body": ["42 café beta naïve 7"]},
]

PHRASE_QS = [
    {"field": "title", "terms": ["alpha", "beta"], "max_gap": 0},
    {"field": "title", "terms": ["alpha", "beta"], "max_gap": 1},
    {"field": "body", "terms": ["alpha", "beta"], "max_gap": 0},
    {"field": "body", "terms": ["alpha", "beta"], "max_gap": 1},
    {"field": "body", "terms": ["alpha", "alpha"], "max_gap": 0},
    {"field": "body", "terms": ["alpha", "alpha"], "max_gap": 1},
    {"field": "body", "terms": ["café", "beta"], "max_gap": 1},
    {"field": "title", "terms": ["ALPHA", "BETA"], "max_gap": 1},  # 大小写折叠
]

SET_QS = [
    {"field": "body", "word": "beta"},
    {"field": "body", "and": [{"word": "alpha"}, {"word": "beta"}]},
    {"field": "body", "or": [{"word": "café"}, {"word": "résumé"}]},
    {"field": "body", "not": {"word": "alpha"}},
    {"field": "title",
     "and": [{"word": "alpha"}, {"not": {"word": "café"}}]},
    {"field": "body",
     "or": [{"word": "naïve"}, {"and": [{"word": "alpha"},
                                        {"not": {"word": "beta"}}]}]},
]


def test_lifecycle() -> None:
    print("[1] 构建与基础短语/集合查询（值边界、停词、重复词、非 ASCII）")
    eng = make_engine(os.path.join(WORK, "idx"))
    for d in DOCS:
        eng.put(d)

    with eng.snapshot() as snap:
        active = active_docs(snap)
        assert_oracle(snap, active, PHRASE_QS + SET_QS)

        # 值边界：a.title 的两个元素都有 position=0 的词，跨值不得命中
        r = snap.phrase("title", ["alpha", "beta"], 0)
        a_hits = [h for h in r["hits"] if h["doc_id"] == "a"]
        check(len(a_hits) == 1 and a_hits[0]["value_no"] == 0
              and a_hits[0]["positions"] == [0, 1],
              "相邻短语只在同一数组元素内命中，跨值 position=0 不拼接")

        r2 = snap.phrase("title", ["beta", "alpha"], 0)
        check(all(h["value_no"] == 1 for h in r2["hits"])
              and all(h["doc_id"] == "a" for h in r2["hits"]),
              "反向短语只来自第二个值，不借用第一个值的尾词")

        # 停词占位：b.title = alpha(0) the(1) beta(2)
        check(not any(h["doc_id"] == "b" for h in
                      snap.phrase("title", ["alpha", "beta"], 0)["hits"]),
              "停词占据位置：max_gap=0 时停词间隔不命中")
        r3 = snap.phrase("title", ["alpha", "beta"], 1)
        hit_b = [h for h in r3["hits"] if h["doc_id"] == "b"]
        check(len(hit_b) == 1 and hit_b[0]["positions"] == [0, 2]
              and hit_b[0]["evidence"] == ["alpha", "beta"],
              "max_gap=1 跨过一个停词命中，位置差为 2 且证据回到原词")
        # 两个 the 间隔：body 里位置差 3，gap=1 不够，gap=2 才行
        check(not any(h["doc_id"] == "b" for h in
                      snap.phrase("body", ["alpha", "beta"], 1)["hits"]),
              "两个停词间隔在 max_gap=1 下仍不命中")
        check(any(h["doc_id"] == "b" and h["positions"] == [0, 3] for h in
                  snap.phrase("body", ["alpha", "beta"], 2)["hits"]),
              "max_gap=2 可跨两个停词")

        # 重复词：alpha alpha，gap=0；c.body 三个 alpha 产生两个相邻组合
        r4 = snap.phrase("body", ["alpha", "alpha"], 0)
        c_hits = [h for h in r4["hits"] if h["doc_id"] == "c"]
        check([h["positions"] for h in c_hits] == [[0, 1], [1, 2]],
              "重复词枚举全部位置组合，且同一出现不会被复用两次")
        check(snap.phrase("body", ["alpha", "alpha"], 0)["hit_count"]
              == len(oracle_phrase(OracleScanner(snap.analyzer, snap.fields),
                                   active, "body", ["alpha", "alpha"], 0)),
              "重复词命中数与 oracle 完全一致")

        # 非 ASCII 字节证据：café = 5 个 UTF-8 字节，résumé 更长
        r5 = snap.word("body", "café")
        occ = r5["occurrences"][0]
        check(occ["evidence"] == "café" and occ["byte_span"][1] - occ["byte_span"][0] == 5,
              "非 ASCII 词按 UTF-8 字节区间切回原文（café 占 5 字节）")
        r6 = snap.phrase("body", ["café", "beta"], 1)["hits"]
        check(all(h["evidence"] == ["café", "beta"] for h in r6),
              "短语中每个位置组合都带各自字节证据")
        # 数字词
        check(any(o["evidence"] == "42" for o in snap.word("body", "42")["occurrences"]),
              "连续数字码点构成词，字节证据正确")

        # NOT 全集是活跃文档（4 个），不是历史出现集合
        not_alpha = snap.set_query("body", {"not": {"word": "alpha"}})["docs"]
        check(not_alpha == ["d"], "NOT 的全集为当前活跃文档，未出现词的文档也在全集内")

    print("[2] put 完整替换：旧词消失，旧 revision 不复活")
    eng.put({"doc_id": "b", "title": ["green blue"], "body": ["green only"]})
    with eng.snapshot() as snap:
        check(snap.word("title", "alpha")["docs"] == ["a", "c"],
              "put 后文档 b 的旧词 alpha 从命中集合消失")
        check(snap.get_doc("b")["body"] == ["green only"],
              "get_doc 返回的是当前 revision 的完整原文")
        check(snap.set_query("body", {"not": {"word": "green"}})["docs"]
              == ["a", "c", "d"], "替换后 NOT 全集仍按活跃文档计算")
        assert_oracle(snap, active_docs(snap), PHRASE_QS + SET_QS)

    print("[3] delete：无任何命中，且 NOT 全集同步收缩")
    eng.delete("c")
    with eng.snapshot() as snap:
        check(snap.word("body", "alpha")["docs"] == ["a"],
              "delete 后文档 c 无任何单词命中")
        check(snap.phrase("body", ["alpha", "alpha"], 0)["hits"] == [],
              "delete 后旧短语命中全部消失")
        check(snap.get_doc("c") is None and "c" not in snap.active_doc_ids(),
              "delete 后文档不在活跃集合")
        check(snap.set_query("body", {"not": {"word": "alpha"}})["docs"]
              == ["b", "d"], "delete 后 NOT 全集不含墓碑文档")
        assert_oracle(snap, active_docs(snap), PHRASE_QS + SET_QS)

    print("[4] 重新 put 已删除文档：新 revision 正常可查，旧 posting 不复活")
    eng.put({"doc_id": "c", "title": ["cafe"], "body": ["cafe green green"]})
    with eng.snapshot() as snap:
        check(snap.word("title", "alpha")["docs"] == ["a"],
              "重新 put 后旧词 alpha 不会随旧 posting 复活")
        check([h["positions"] for h in
               snap.phrase("body", ["green", "green"], 0)["hits"]] == [[1, 2]],
              "新文档的重复词短语正常命中")
        assert_oracle(snap, active_docs(snap), PHRASE_QS + SET_QS)


def test_compact_and_reader() -> None:
    print("[5] compact：多段合并、旧 posting 清除、固定读视图并发")
    eng = make_engine(os.path.join(WORK, "compact"))
    for d in DOCS:
        eng.put(d)
    eng.put({"doc_id": "b", "title": ["green blue"], "body": ["green only"]})
    eng.delete("c")
    eng.put({"doc_id": "c", "title": ["cafe"], "body": ["cafe green green"]})
    # 7 次写 -> 7 个段
    before = eng._manifest.segments
    check(len(before) == 7, f"合并前段数为 7（实际 {len(before)}）")

    old_snap = eng.snapshot()  # 读者固定旧清单
    old_active = active_docs(old_snap)
    old_answers = []
    for q in PHRASE_QS + SET_QS:
        old_answers.append((q, json.dumps(old_snap.run(q),
                                          ensure_ascii=False, sort_keys=True)))

    info = eng.compact()
    print(f"    compact 报告: {info}")
    check(info["new_segment"] is not None and info["merged"] == before,
          "compact 合并全部旧段并生成一个新段")

    # 旧读视图在合并后仍可完整查询，答案与合并前逐字节一致
    for q, sig in old_answers:
        now = json.dumps(old_snap.run(q), ensure_ascii=False, sort_keys=True)
        check(now == sig, f"固定旧清单的读者在 compact 后查询不变: {q.get('terms') or q}")
    old_snap.close()

    with eng.snapshot() as snap:
        check(snap.manifest.segments == [info["new_segment"]],
              "新查询只看到合并后的单一段清单")
        check(snap.phrase("body", ["alpha", "alpha"], 0)["hits"] == [],
              "合并后被删/被覆盖文档的旧短语不复活")
        check([h["positions"] for h in
               snap.phrase("body", ["green", "green"], 0)["hits"]] == [[1, 2]],
              "合并后新版本命中完整保留")
        assert_oracle(snap, active_docs(snap), PHRASE_QS + SET_QS)

    # 旧段回收：旧视图关闭前应保留，关闭后 reap 清除
    check(len(info["retained_for_readers"]) == len(before),
          "读者固定旧清单期间旧段文件全部保留（租约保护）")
    reap = eng.reap()
    check(len(reap["removed_files"]) == len(before)
          and reap["retained_for_readers"] == [],
          "读者关闭后孤儿段被回收，清单内新段不受影响")
    seg_files = sorted(os.listdir(os.path.join(eng.root, "segments")))
    check(seg_files == [f"seg_{info['new_segment']}.json"],
          "磁盘上只剩清单指向的段，无半合并残留")

    # 再 compact 一次：单段时为 no-op
    again = eng.compact()
    check(again["new_segment"] is None, "只有一个段时 compact 为 no-op")

    print("[6] 并发：读者持有旧视图，写者 compact，双方都得到一致结果")
    eng2 = make_engine(os.path.join(WORK, "concurrent"))
    for d in DOCS:
        eng2.put(d)
    errors: list[BaseException] = []
    barrier = threading.Event()

    def reader() -> None:
        try:
            snap = eng2.snapshot()
            barrier.set()  # 通知写者：读视图已固定
            # 合并期间持续查询，答案必须始终自洽且等于 oracle
            scanner = OracleScanner(snap.analyzer, snap.fields)
            for _ in range(50):
                for q in PHRASE_QS:
                    got = snap.phrase(q["field"], q["terms"], q["max_gap"])
                    exp = oracle_phrase(scanner, active_docs(snap),
                                        q["field"], q["terms"], q["max_gap"])
                    assert _sig(exp) == _sig(got["hits"])
            snap.close()
        except BaseException as e:  # noqa: BLE001
            errors.append(e)

    t = threading.Thread(target=reader)
    t.start()
    barrier.wait()
    eng2.compact()  # 写者在读者固定旧清单后合并
    t.join()
    check(not errors, f"并发读视图在 compact 期间全程一致（{errors}）")
    with eng2.snapshot() as snap:
        check(len(snap.manifest.segments) == 1, "并发后新查询看到合并清单")


def test_rejections() -> None:
    global PASS
    print("[7] 非法查询明确拒绝")
    eng = make_engine(os.path.join(WORK, "reject"))
    for d in DOCS:
        eng.put(d)
    with eng.snapshot() as snap:
        bad = [
            {"field": "nope", "terms": ["alpha", "beta"], "max_gap": 0},
            {"field": "title", "terms": ["the", "beta"], "max_gap": 0},
            {"field": "title", "terms": ["the", "a"], "max_gap": 0},
            {"field": "title", "terms": ["alpha"], "max_gap": 0},
            {"field": "title", "terms": ["alpha", "beta"], "max_gap": -1},
            {"field": "body", "word": "the"},
        ]
        for q in bad:
            try:
                snap.run(q)
                raise AssertionError(f"应拒绝但未拒绝: {q}")
            except QueryError:
                PASS += 1
        print(f"  ✓ {len(bad)} 类非法查询（未知 field/停词/单词短语/负 gap）全部拒绝")


def test_fuzz() -> None:
    print("[8] 随机 fuzz：随机文档、停词集合、更新顺序与查询，全程 oracle 对账")
    rng = random.Random(20261115)
    vocab = ["alpha", "beta", "gamma", "delta", "42", "café", "naïve",
             "the", "of", "AND"]  # AND 作为普通词，检验大小写折叠
    for round_no in range(40):
        stops = rng.sample(["the", "of"], k=rng.randint(0, 2))
        eng = make_engine(os.path.join(WORK, f"fuzz_{round_no}"),
                          stopwords=tuple(stops))
        n_docs = rng.randint(1, 12)
        ids = [f"d{i}" for i in range(n_docs)]
        truth: dict[str, dict | None] = {}

        def rand_doc(did: str) -> dict:
            doc = {"doc_id": did}
            for fld in ("title", "body"):
                doc[fld] = [
                    " ".join(rng.choice(vocab)
                             for _ in range(rng.randint(0, 12)))
                    for _ in range(rng.randint(1, 4))
                ]
            return doc

        for did in ids:
            d = rand_doc(did)
            eng.put(d)
            truth[did] = d

        # 随机 put/delete 序列，维护一份独立真值
        for _ in range(rng.randint(1, 15)):
            did = rng.choice(ids)
            if rng.random() < 0.25:
                eng.delete(did)
                truth[did] = None
            else:
                d = rand_doc(did)
                eng.put(d)
                truth[did] = d

        # 穿插 compact
        if rng.random() < 0.5:
            eng.compact()

        with eng.snapshot() as snap:
            live = {k: v for k, v in truth.items() if v is not None}
            if live != active_docs(snap):
                raise AssertionError("fuzz 活跃文档真值与清单不一致")
            scanner = OracleScanner(snap.analyzer, snap.fields)
            terms_pool = [w for w in set(vocab)
                          if w.casefold() not in snap.analyzer.stopwords]
            qs = []
            for _ in range(12):
                t1, t2 = rng.sample(terms_pool, 2)
                qs.append({"field": rng.choice(["title", "body"]),
                           "terms": [t1, t2],
                           "max_gap": rng.randint(0, 4)})
            for t1 in rng.sample(terms_pool, min(4, len(terms_pool))):
                qs.append({"field": rng.choice(["title", "body"]),
                           "word": t1})
            qs.append({"field": "body",
                       "and": [{"word": terms_pool[0]},
                               {"not": {"word": terms_pool[1]}}]})
            for q in qs:
                if "terms" in q:
                    exp = oracle_phrase(scanner, live, q["field"],
                                        q["terms"], q["max_gap"])
                    got = snap.phrase(q["field"], q["terms"], q["max_gap"])
                    assert _sig(exp) == _sig(got["hits"]), (round_no, q)
                else:
                    exp = oracle_set_query(scanner, live, q)
                    assert exp == set(snap.run(q)["docs"]), (round_no, q)
    check(True, "40 轮随机场景（含 compact、delete、重新 put、随机停词）全部对账通过")


def test_cli_and_given_materials() -> None:
    print("[9] CLI 全流程 + 题面给定材料端到端")
    root = os.path.join(WORK, "cli")
    if os.path.exists(root):
        shutil.rmtree(root)
    py = sys.executable
    repo = os.path.dirname(os.path.abspath(__file__))
    cli = [py, "-m", "docidx.cli"]
    env = {**os.environ, "PYTHONPATH": repo}

    def run(*args):
        return subprocess.run(cli + list(args), check=True,
                              capture_output=True, text=True, env=env)

    run("build", "--root", root, "--docs", os.path.join(DATA, "docs.jsonl"),
        "--analyzer", os.path.join(DATA, "analyzer.json"))
    out = run("query", "--root", root,
              "--queries", os.path.join(DATA, "queries.json")).stdout
    res = json.loads(out)
    # 题面材料的手工预期（d1.title 是两个独立单元素值，Q1 只在 d2 命中）：
    #  Q1 title red blue gap0 -> 仅 d2
    #  Q2 body  red blue gap0 -> d1/d2/d3 共 3 命中
    #  Q3 body  red blue gap1 -> d1(2)/d2/d3(2) 共 5 命中
    #  Q4 body  red red  gap0 -> 仅 d3
    check([r["hit_count"] for r in res] == [1, 3, 5, 1],
          "给定材料初始查询命中数 [1,3,5,1]")
    check(res[0]["hits"][0]["positions"] == [0, 1]
          and res[0]["hits"][0]["evidence"] == ["red", "blue"],
          "CLI 输出带位置组合与原文字节证据")

    run("apply", "--root", root,
        "--updates", os.path.join(DATA, "updates.jsonl"))
    res2 = json.loads(run("query", "--root", root,
                          "--queries", os.path.join(DATA, "queries.json")).stdout)
    # d2 整体替换后旧 red 全部消失；d3 删除：只剩 d1.body 的 1/2 个短语
    check([r["hit_count"] for r in res2] == [0, 1, 2, 0],
          "apply（d2 替换、d3 删除）后命中数 [0,1,2,0]，旧文档不复活")
    run("compact", "--root", root)
    res3 = json.loads(run("query", "--root", root,
                          "--queries", os.path.join(DATA, "queries.json")).stdout)
    check(res3 == res2, "compact 后查询结果逐字节不变，无重复/半合并命中")
    audit = subprocess.run(cli + ["audit", "--root", root,
                                  "--queries", os.path.join(DATA, "queries.json")],
                           capture_output=True, text=True, env=env)
    check(audit.returncode == 0 and '"failures": 0' in audit.stdout,
          "audit 子命令以 oracle 对账，零失败")

    # 非法查询 CLI 退出码 2
    bad_q = os.path.join(WORK, "bad_query.json")
    with open(bad_q, "w", encoding="utf-8") as f:
        json.dump([{"field": "title", "terms": ["the", "blue"], "max_gap": 0}], f)
    bad = subprocess.run(cli + ["query", "--root", root, "--queries", bad_q],
                         capture_output=True, text=True, env=env)
    check(bad.returncode == 2 and "停词" in bad.stderr,
          "CLI 对停词查询明确拒绝并退出 2")


def main() -> int:
    if os.path.exists(WORK):
        shutil.rmtree(WORK)
    os.makedirs(WORK, exist_ok=True)
    test_lifecycle()
    test_compact_and_reader()
    test_rejections()
    test_fuzz()
    test_cli_and_given_materials()
    print(f"\n全部验证通过：{PASS} 项断言。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
