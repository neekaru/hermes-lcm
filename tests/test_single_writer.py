"""Single-writer serialization tests for the LCM SQLite redesign.

Verifies that every store bound to the SAME database file shares ONE
process-wide write lock (``write_lock_for``), so two stores can never enter a
write transaction on the same file concurrently in-process. This is the
invariant the historical on-disk corruption violated: per-store RLocks let
MessageStore and SummaryDAG write the same lcm.db at the same time.
"""

from __future__ import annotations

import sqlite3
import threading
from pathlib import Path

from hermes_lcm.sqlite_util import (
    write_lock_for,
    reset_write_locks,
    write_transaction,
)
from hermes_lcm.store import MessageStore
from hermes_lcm.dag import SummaryDAG
from hermes_lcm.lifecycle_state import LifecycleStateStore
from hermes_lcm.rollup_store import RollupStore


def _reset():
    reset_write_locks()


def test_write_lock_is_shared_per_path(tmp_path: Path):
    _reset()
    db = tmp_path / "shared.db"
    a = write_lock_for(db)
    b = write_lock_for(db)
    assert a is b, "same path must return the identical lock object"


def test_write_lock_is_shared_across_stores(tmp_path: Path):
    _reset()
    db = tmp_path / "shared.db"
    store = MessageStore(db)
    dag = SummaryDAG(db)
    try:
        assert store._write_lock is dag._db_lock, (
            "MessageStore and SummaryDAG on the same file must share one lock"
        )
    finally:
        store.close()
        dag.close()


def test_write_lock_is_shared_across_all_stores(tmp_path: Path):
    _reset()
    db = tmp_path / "shared.db"
    store = MessageStore(db)
    dag = SummaryDAG(db)
    lc = LifecycleStateStore(db)
    roll = RollupStore(db)
    try:
        locks = {id(store._write_lock), id(dag._db_lock), id(lc._lock), id(roll._write_lock)}
        assert len(locks) == 1, "all stores on one file must share the same lock"
    finally:
        store.close()
        dag.close()
        lc.close()
        roll.close()


def test_different_paths_get_different_locks(tmp_path: Path):
    _reset()
    a = write_lock_for(tmp_path / "a.db")
    b = write_lock_for(tmp_path / "b.db")
    assert a is not b, "distinct files must not share a lock"


def test_memory_dbs_get_unique_locks():
    _reset()
    a = write_lock_for(":memory:")
    b = write_lock_for(":memory:")
    assert a is not b, ":memory: databases are private and must not share a lock"


def test_concurrent_writes_are_serialized_and_db_stays_intact(tmp_path: Path):
    """Hammer one file from many threads through two different stores.

    Without the shared lock, two stores on the same file could interleave write
    transactions. With it, all writes serialize and the database passes
    integrity_check afterwards.
    """
    _reset()
    db = tmp_path / "hammer.db"
    store = MessageStore(db)
    dag = SummaryDAG(db)

    errors: list[BaseException] = []
    barrier = threading.Barrier(8)
    lock = threading.Lock()

    def write_messages(n: int):
        try:
            barrier.wait(timeout=30.0)
            for i in range(20):
                store.append(f"sess-{n}", {"role": "user", "content": f"m-{n}-{i}"})
        except BaseException as exc:  # noqa: BLE001
            barrier.abort()
            with lock:
                errors.append(exc)

    def write_nodes(n: int):
        from hermes_lcm.dag import SummaryNode

        try:
            barrier.wait(timeout=30.0)
            for i in range(20):
                dag.add_node(
                    SummaryNode(
                        session_id=f"sess-{n}",
                        depth=0,
                        summary=f"summary-{n}-{i}",
                        source_ids=[],
                        source_type="messages",
                        created_at=0.0,
                    )
                )
        except BaseException as exc:  # noqa: BLE001
            barrier.abort()
            with lock:
                errors.append(exc)

    threads = []
    for n in range(4):
        threads.append(threading.Thread(target=write_messages, args=(n,)))
        threads.append(threading.Thread(target=write_nodes, args=(n,)))
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=120.0)

    stuck = [t for t in threads if t.is_alive()]
    assert not stuck, f"{len(stuck)} writer threads still running"
    assert not errors, f"writers raised: {errors!r}"

    store.close()
    dag.close()

    conn = sqlite3.connect(str(db))
    try:
        assert conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert conn.execute("SELECT count(*) FROM messages").fetchone()[0] == 80
        assert conn.execute("SELECT count(*) FROM summary_nodes").fetchone()[0] == 80
    finally:
        conn.close()


def test_write_transaction_rolls_back_on_error(tmp_path: Path):
    _reset()
    db = tmp_path / "txn.db"
    conn = sqlite3.connect(str(db))
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
    conn.commit()
    try:
        with write_transaction(conn, db):
            conn.execute("INSERT INTO t (v) VALUES ('a')")
            raise RuntimeError("boom")
    except RuntimeError:
        pass
    assert conn.execute("SELECT count(*) FROM t").fetchone()[0] == 0
    conn.close()
