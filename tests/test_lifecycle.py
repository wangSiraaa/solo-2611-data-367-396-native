"""Focused lifecycle tests using unittest (no third-party deps).

Run with:  python3 -m unittest discover -s tests -v

Every test drives the *real* compiled source.c producer.  Because the producer's
counters are process-global, tests compare deltas and assert the live count
returns to its starting value.
"""

from __future__ import annotations

import ctypes as C
import sys
import threading
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from abr.abi import ArrowArray, ArrayReleaseFn  # noqa: E402
from abr.audit import AuditLog  # noqa: E402
from abr.batch import BatchReleasedError, ImportedBatch  # noqa: E402
from abr.driver import Driver  # noqa: E402
from abr.loader import load_library  # noqa: E402
from abr.stream import ProducerStream  # noqa: E402
from abr.view import CANCELLED, RELEASED, ViewStateError  # noqa: E402

LIB = load_library()
VALID = 0x1B


def value(batch_index: int, pos: int) -> int:
    return 100 * batch_index + pos


def valid(pos: int) -> bool:
    return bool((VALID >> pos) & 1)


class LifecycleCase(unittest.TestCase):
    def setUp(self) -> None:
        self.live0 = int(LIB.live_batches())
        self.rel0 = int(LIB.released_batches())
        self.str0 = int(LIB.released_streams())
        self.audit = AuditLog()
        self.driver = Driver(LIB, self.audit, max_live_batches=2)

    def tearDown(self) -> None:
        self.driver.drain_all(reason="test teardown")
        self.assertEqual(int(LIB.live_batches()), self.live0, "producer live count not zero after test")
        self.assertGreaterEqual(int(LIB.released_batches()), self.rel0)
        self.assertGreaterEqual(int(LIB.released_streams()), self.str0)

    def assert_read(self, view_id: str, batch_index: int, offset: int, length: int) -> None:
        res = self.driver.read(view_id)
        for i in range(length):
            pos = offset + i
            if valid(pos):
                self.assertEqual(res["values"][i], value(batch_index, pos))
                self.assertTrue(res["valid"][i])
            else:
                self.assertIsNone(res["values"][i])
                self.assertFalse(res["valid"][i])
        self.assertEqual(len(res["values"]), length)

    # 1 -------------------------------------------------------------------
    def test_move_clears_exporter_release_and_reuses_real_callback(self) -> None:
        stream = ProducerStream(LIB, self.audit)
        exported = stream.next_batch()
        assert exported is not None
        original_release = int(exported.release)
        self.assertNotEqual(original_release, 0)
        batch = ImportedBatch.take("x", exported, self.audit)
        # Arrow C Data move: exporter structure no longer owns release.
        self.assertFalse(bool(exported.release))
        self.assertFalse(bool(exported.private_data))
        self.assertEqual(batch.release_fn, original_release)
        self.assertEqual(int(LIB.live_batches()), self.live0 + 1)
        batch.drop_root()
        self.assertEqual(int(LIB.live_batches()), self.live0)
        self.assertEqual(int(LIB.released_batches()), self.rel0 + 1)
        stream.close()

    # 2 -------------------------------------------------------------------
    def test_sibling_release_keeps_other_view_valid(self) -> None:
        self.driver.next("a")
        self.driver.view("left", "a", 0, 3)
        self.driver.view("right", "a", 2, 3)
        self.driver.drop_import("a")
        self.driver.release_view("left")
        # right is still live and reads the original buffers.
        self.assert_read("right", 0, 2, 3)  # [None, 3, 4]
        self.driver.release_view("right")
        self.assertEqual(int(LIB.live_batches()), self.live0)

    # 3 -------------------------------------------------------------------
    def test_root_import_dropped_first(self) -> None:
        self.driver.next("a")
        self.driver.view("v", "a", 1, 4)
        self.driver.drop_import("a")
        self.assert_read("v", 0, 1, 4)
        # Batch cannot be freed while v lives.
        self.assertEqual(int(LIB.live_batches()), self.live0 + 1)
        self.driver.release_view("v")
        self.assertEqual(int(LIB.live_batches()), self.live0)

    # 4 -------------------------------------------------------------------
    def test_stream_close_does_not_invalidate_moved_batch(self) -> None:
        self.driver.next("a")
        self.driver.view("v", "a", 0, 5)
        self.driver.close_stream()
        self.driver.drop_import("a")
        self.assert_read("v", 0, 0, 5)
        self.driver.release_view("v")
        self.assertEqual(int(LIB.live_batches()), self.live0)

    # 5 -------------------------------------------------------------------
    def test_nested_slices_share_addresses_and_accumulate_offsets(self) -> None:
        self.driver.next("a")
        a = self.driver.views
        self.driver.view("l0", "a", 0, 5)
        self.driver.view("l1", "l0", 1, 4, parent="l0")
        self.driver.view("l2", "l1", 1, 2, parent="l1")
        for vid in ("l0", "l1", "l2"):
            v = self.driver.views[vid]
            self.assertEqual(v.values_addr, self.driver.roots["a"].values_addr)
            self.assertEqual(v.validity_addr, self.driver.roots["a"].validity_addr)
        self.assertEqual(self.driver.views["l1"].offset, 1)
        self.assertEqual(self.driver.views["l2"].offset, 2)
        self.assertEqual(self.driver.views["l2"].depth, 2)
        self.assert_read("l2", 0, 2, 2)  # [None, 3]
        self.driver.release_view("l0")
        self.assert_read("l2", 0, 2, 2)  # children live after parent release
        self.driver.release_view("l1")
        self.assert_read("l2", 0, 2, 2)
        self.driver.release_view("l2")
        self.driver.drop_import("a")

    # 6 -------------------------------------------------------------------
    def test_null_bit_at_shifted_offset(self) -> None:
        self.driver.next("a")
        self.driver.view("v", "a", 2, 1)       # exactly the NULL slot
        res = self.driver.read("v")
        self.assertEqual(res["values"], [None])
        self.assertEqual(res["null_count"], 1)
        self.driver.view("v2", "a", 1, 3)      # [1, None, 3]
        res2 = self.driver.read("v2")
        self.assertEqual(res2["values"], [1, None, 3])
        self.assertEqual(res2["valid"], [True, False, True])
        self.driver.release_view("v")
        self.driver.release_view("v2")
        self.driver.drop_import("a")

    # 7 -------------------------------------------------------------------
    def test_cancel_is_isolated_and_terminal(self) -> None:
        self.driver.next("a")
        self.driver.view("c", "a", 0, 5)
        self.driver.view("s", "a", 0, 5)
        self.driver.cancel_view("c")
        with self.assertRaises(ViewStateError):
            self.driver.read("c")
        with self.assertRaises(ViewStateError):
            self.driver.release_view("c")
        with self.assertRaises(ViewStateError):
            self.driver.cancel_view("c")
        self.assert_read("s", 0, 0, 5)
        rel_before = int(LIB.released_batches())
        self.driver.release_view("s")
        self.driver.drop_import("a")
        self.assertEqual(int(LIB.released_batches()), rel_before + 1)

    # 8 -------------------------------------------------------------------
    def test_released_handle_errors_without_extra_callback(self) -> None:
        self.driver.next("a")
        self.driver.view("v", "a", 0, 5)
        self.driver.release_view("v")
        self.driver.drop_import("a")
        self.assertEqual(
            sum(1 for e in self.audit.events if e.get("event") == "batch_release" and e["batch"] == "a"),
            1,
        )
        before = sum(1 for e in self.audit.events if e.get("event") == "batch_release")
        for fn in (lambda: self.driver.read("v"), lambda: self.driver.release_view("v"),
                   lambda: self.driver.cancel_view("v")):
            with self.assertRaises(ViewStateError):
                fn()
        after = sum(1 for e in self.audit.events if e.get("event") == "batch_release")
        self.assertEqual(before, after, "dead view triggered a producer callback")

    # 9 -------------------------------------------------------------------
    def test_backpressure_does_not_call_get_next(self) -> None:
        self.driver.next("a")
        self.driver.view("va", "a", 0, 5)
        self.driver.next("b")
        self.driver.view("vb", "b", 0, 5)
        calls_before = sum(1 for e in self.audit.events if e.get("event") == "get_next")
        before_live = int(LIB.live_batches())
        res = self.driver.next("c")
        self.assertTrue(res["backpressure"])
        calls_after = sum(1 for e in self.audit.events if e.get("event") == "get_next")
        self.assertEqual(calls_before, calls_after, "backpressure must not call get_next")
        self.assertEqual(int(LIB.live_batches()), before_live)
        # Free a slot by ending a's dependents.
        self.driver.release_view("va")
        self.driver.drop_import("a")
        ok = self.driver.next("c")
        self.assertFalse(ok.get("backpressure", False))
        self.assertTrue(ok.get("imported"))
        self.driver.release_view("vb")
        self.driver.drop_import("b")
        self.driver.drop_import("c")

    # 10 ------------------------------------------------------------------
    def test_empty_view_reads_nothing_but_holds_dependency(self) -> None:
        self.driver.next("a")
        self.driver.view("e", "a", 5, 0)
        self.driver.drop_import("a")
        self.assertEqual(int(LIB.live_batches()), self.live0 + 1)
        res = self.driver.read("e")
        self.assertEqual(res["values"], [])
        self.assertEqual(res["null_count"], 0)
        self.driver.release_view("e")
        self.assertEqual(int(LIB.live_batches()), self.live0)

    # 11 ------------------------------------------------------------------
    def test_exception_cleanup_releases_everything(self) -> None:
        self.driver.next("a")
        self.driver.view("v", "a", 0, 5)
        self.driver.next("b")
        with self.assertRaises(RuntimeError):
            self.driver.execute(
                [{"op": "read", "id": "v"}], fail_at=0
            )
        self.driver.drain_all(reason="after injected failure")
        self.assertEqual(int(LIB.live_batches()), self.live0)
        self.assertEqual(int(LIB.released_batches()), self.rel0 + 2)

    # 12 ------------------------------------------------------------------
    def test_concurrent_reads_on_independent_views(self) -> None:
        self.driver.next("a")
        self.driver.view("x", "a", 0, 5)
        self.driver.view("y", "a", 0, 5)
        errors: list[BaseException] = []

        def worker(vid: str) -> None:
            try:
                for _ in range(50):
                    self.driver.read(vid)
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)

        t1 = threading.Thread(target=worker, args=("x",))
        t2 = threading.Thread(target=worker, args=("y",))
        t1.start()
        t2.start()
        t1.join()
        t2.join()
        self.assertEqual(errors, [])
        self.driver.release_view("x")
        self.driver.release_view("y")
        self.driver.drop_import("a")


class BoundsCase(unittest.TestCase):
    def setUp(self) -> None:
        self.live0 = int(LIB.live_batches())
        self.rel0 = int(LIB.released_batches())
        self.audit = AuditLog()
        self.driver = Driver(LIB, self.audit, max_live_batches=2)

    def tearDown(self) -> None:
        self.driver.drain_all(reason="bounds teardown")
        self.assertEqual(int(LIB.live_batches()), self.live0)

    def assert_read(self, view_id: str, batch_index: int, offset: int, length: int) -> None:
        res = self.driver.read(view_id)
        for i in range(length):
            pos = offset + i
            if valid(pos):
                self.assertEqual(res["values"][i], value(batch_index, pos))
            else:
                self.assertIsNone(res["values"][i])

    def test_no_new_views_after_root_drop(self) -> None:
        self.driver.next("a")
        self.driver.view("keep", "a", 0, 5)
        self.driver.drop_import("a")
        with self.assertRaises(BatchReleasedError):
            self.driver.view("late", "a", 0, 1)
        self.assert_read("keep", 0, 0, 5)
        self.driver.release_view("keep")

    def test_max_batches_enforced(self) -> None:
        limited = Driver(LIB, self.audit, max_live_batches=2, max_batches=1)
        try:
            limited.next("a")
            with self.assertRaises(Exception):
                limited.next("b")
            limited.drop_import("a")
        finally:
            limited.drain_all(reason="batch limit test")

    def test_batch_released_promptly_when_last_dep_ends(self) -> None:
        self.driver.next("a")
        self.driver.view("v", "a", 0, 5)
        self.driver.drop_import("a")
        self.assertEqual(int(LIB.live_batches()), self.live0 + 1)  # still held by v
        self.driver.release_view("v")
        # Immediately released, not deferred to end of run.
        self.assertEqual(int(LIB.live_batches()), self.live0)
        self.assertEqual(int(LIB.released_batches()), self.rel0 + 1)

    def test_illegal_slices_rejected(self) -> None:
        self.driver.next("a")
        with self.assertRaises(ValueError):
            self.driver.view("neg", "a", -1, 2)
        with self.assertRaises(ValueError):
            self.driver.view("over", "a", 2, 4)
        with self.assertRaises(ValueError):
            self.driver.view("over2", "a", 6, 0)
        self.driver.view("ok", "a", 5, 0)
        with self.assertRaises(ValueError):
            self.driver.view("pover", "ok", 0, 1, parent="ok")
        self.driver.release_view("ok")
        self.driver.drop_import("a")

    def test_view_limit(self) -> None:
        self.driver.next("a")
        for i in range(8):
            self.driver.view(f"v{i}", "a", 0, 5)
        with self.assertRaises(Exception):
            self.driver.view("v8", "a", 0, 5)
        self.driver.release_view("v0")
        self.driver.view("v8", "a", 0, 5)  # slot freed
        for i in range(1, 8):
            self.driver.release_view(f"v{i}")
        self.driver.release_view("v8")
        self.driver.drop_import("a")

    def test_batch_name_reuse_rejected(self) -> None:
        self.driver.next("a")
        with self.assertRaises(Exception):
            self.driver.next("a")
        self.driver.drop_import("a")


if __name__ == "__main__":
    unittest.main(verbosity=2)
