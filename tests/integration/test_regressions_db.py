"""
Regression tests for the 2026-07 production-readiness audit (live database).

Each test names the audit finding it guards: C=critical, H=high, M=medium.
Unit-level regressions live in tests/test_regressions.py.
"""

import multiprocessing
import os
import shutil
import tempfile
import threading
import time

import psycopg
import pytest

from migretti import __main__ as main_mod
from migretti.core import (
    apply_migrations,
    fix_migration,
    get_head,
    get_migration_status,
    rollback_migrations,
    verify_checksums,
)
from migretti.logging_setup import setup_logging

TEST_DB_NAME = "migretti_test"
TEST_DB_URL = os.environ.get(
    "MIGRETTI_TEST_DB_URL",
    f"postgresql://postgres:postgres@localhost:5432/{TEST_DB_NAME}",
)

ID_A = "01AAAAAAAAAAAAAAAAAAAAAAAA"
ID_B = "01BBBBBBBBBBBBBBBBBBBBBBBB"


@pytest.fixture(scope="function")
def test_db():
    try:
        conn = psycopg.connect(TEST_DB_URL, autocommit=True)
    except psycopg.OperationalError:
        pytest.fail(f"Could not connect to test database at {TEST_DB_URL}")

    with conn.cursor() as cur:
        cur.execute("DROP SCHEMA public CASCADE;")
        cur.execute("CREATE SCHEMA public;")
    conn.close()

    os.environ["MG_DATABASE_URL"] = TEST_DB_URL
    yield TEST_DB_URL
    del os.environ["MG_DATABASE_URL"]


@pytest.fixture(scope="function")
def temp_project():
    setup_logging(verbose=True)
    old_cwd = os.getcwd()
    tmp_dir = tempfile.mkdtemp()
    os.chdir(tmp_dir)

    class Args:
        pass

    main_mod.cmd_init(Args())
    yield tmp_dir
    os.chdir(old_cwd)
    shutil.rmtree(tmp_dir)


def write_migration(fname, up, down="SELECT 1;", no_txn=False):
    lines = []
    if no_txn:
        lines.append("-- migrate: no-transaction")
    lines += ["-- migrate: up", up, "", "-- migrate: down", down, ""]
    with open(os.path.join("migrations", fname), "w") as f:
        f.write("\n".join(lines))


def q(sql, params=None):
    with psycopg.connect(TEST_DB_URL, autocommit=True) as c:
        cur = c.execute(sql, params or ())
        return cur.fetchall()


def table_exists(name):
    return q(
        "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
        "WHERE table_schema='public' AND table_name=%s)",
        (name,),
    )[0][0]


# --- C1: mixed transactional / no-transaction batches ------------------------


def test_c1_mixed_batch_applies_everything(test_db, temp_project):
    """A no-transaction migration after a transactional one must not discard
    the earlier migration's work (audit C1: it silently did)."""
    write_migration(f"{ID_A}_t1.sql", "CREATE TABLE t1 (id int);", "DROP TABLE t1;")
    write_migration(
        f"{ID_B}_t2.sql", "CREATE TABLE t2 (id int);", "DROP TABLE t2;", no_txn=True
    )

    apply_migrations()

    assert table_exists("t1")
    assert table_exists("t2")
    rows = dict(q("SELECT id, status FROM _migrations"))
    assert rows == {ID_A: "applied", ID_B: "applied"}
    assert q("SELECT count(*) FROM _migrations_log WHERE action='UP'")[0][0] == 2


def test_c1_mixed_batch_rollback(test_db, temp_project):
    write_migration(f"{ID_A}_t1.sql", "CREATE TABLE t1 (id int);", "DROP TABLE t1;")
    write_migration(
        f"{ID_B}_t2.sql", "CREATE TABLE t2 (id int);", "DROP TABLE t2;", no_txn=True
    )
    apply_migrations()

    rollback_migrations(steps=2)

    assert not table_exists("t1")
    assert not table_exists("t2")
    assert q("SELECT count(*) FROM _migrations")[0][0] == 0


def test_c1_migration_durable_before_next_starts(test_db, temp_project):
    """Each migration commits before the next starts (audit C1/C2: the whole
    batch used to ride one implicit transaction committed at lock release)."""
    write_migration(f"{ID_A}_vis.sql", "CREATE TABLE vis_t (id int);", "DROP TABLE vis_t;")
    write_migration(f"{ID_B}_slow.sql", "SELECT pg_sleep(2);")

    errors = []

    def run():
        try:
            apply_migrations()
        except Exception as e:  # pragma: no cover - failure reported via assert
            errors.append(e)

    t = threading.Thread(target=run)
    t.start()
    seen_mid_run = False
    deadline = time.time() + 1.8
    while time.time() < deadline:
        if table_exists("vis_t"):
            seen_mid_run = True
            break
        time.sleep(0.05)
    t.join()

    assert not errors
    assert seen_mid_run, "first migration not committed while the batch was still running"
    assert q("SELECT count(*) FROM _migrations WHERE status='applied'")[0][0] == 2
    # Per-migration transactions mean per-migration timestamps.
    assert q("SELECT count(DISTINCT applied_at) FROM _migrations")[0][0] == 2


# --- C2: concurrency ----------------------------------------------------------


def _worker_apply(url):
    os.environ["MG_DATABASE_URL"] = url
    setup_logging(verbose=True)
    try:
        apply_migrations()
    except Exception as e:  # pragma: no cover - loser may legitimately see no work
        print(f"Worker failed: {e}")


def test_c2_concurrent_runners_apply_each_migration_once(test_db, temp_project):
    """Work is committed before the advisory lock is released, so a concurrent
    runner acquiring the lock never sees stale state and re-applies (audit C2)."""
    write_migration(f"{ID_A}_conc.sql", "CREATE TABLE conc_t (id int);", "DROP TABLE conc_t;")
    write_migration(f"{ID_B}_slow.sql", "SELECT pg_sleep(1);")

    p1 = multiprocessing.Process(target=_worker_apply, args=(TEST_DB_URL,))
    p2 = multiprocessing.Process(target=_worker_apply, args=(TEST_DB_URL,))
    p1.start()
    p2.start()
    p1.join()
    p2.join()

    counts = dict(
        q(
            "SELECT migration_id, count(*) FROM _migrations_log "
            "WHERE action='UP' GROUP BY migration_id"
        )
    )
    assert counts == {ID_A: 1, ID_B: 1}


# --- H4: empty-down rollbacks -------------------------------------------------


def _write_no_down_migration():
    with open(os.path.join("migrations", f"{ID_A}_nodown.sql"), "w") as f:
        f.write("-- migrate: up\nCREATE TABLE keepme (id int);\n-- migrate: down\n")


def test_h4_rollback_refuses_empty_down(test_db, temp_project):
    _write_no_down_migration()
    apply_migrations()

    with pytest.raises(RuntimeError, match="allow-missing-down"):
        rollback_migrations(steps=1)

    # Nothing changed: schema and history still agree.
    assert table_exists("keepme")
    assert q("SELECT count(*) FROM _migrations")[0][0] == 1


def test_h4_rollback_empty_down_with_flag(test_db, temp_project):
    _write_no_down_migration()
    apply_migrations()

    rollback_migrations(steps=1, allow_missing_down=True)

    assert table_exists("keepme")  # intentionally untouched
    assert q("SELECT count(*) FROM _migrations")[0][0] == 0


# --- H2: mg fix ---------------------------------------------------------------


def _write_failing_no_txn():
    with open(os.path.join("migrations", f"{ID_A}_fail.sql"), "w") as f:
        f.write(
            "-- migrate: no-transaction\n-- migrate: up\n"
            "CREATE TABLE IF NOT EXISTS partial (id int);\nSELECT 1/0;\n"
            "-- migrate: down\nDROP TABLE partial;\n"
        )


def test_h2_fix_applied_clears_dirty_state(test_db, temp_project):
    _write_failing_no_txn()
    with pytest.raises(psycopg.DataError):
        apply_migrations()
    with pytest.raises(RuntimeError, match="Dirty database state"):
        apply_migrations()

    fix_migration(ID_A, mark_applied=True)

    apply_migrations()  # no longer blocked
    assert q("SELECT status FROM _migrations WHERE id=%s", (ID_A,))[0][0] == "applied"
    assert q(
        "SELECT count(*) FROM _migrations_log WHERE action='FIX' AND migration_id=%s",
        (ID_A,),
    )[0][0] == 1


def test_h2_fix_remove_makes_pending_again(test_db, temp_project):
    _write_failing_no_txn()
    with pytest.raises(psycopg.DataError):
        apply_migrations()

    fix_migration(ID_A, mark_applied=False)

    assert q("SELECT count(*) FROM _migrations")[0][0] == 0
    assert get_migration_status()[0]["status"] == "pending"


def test_h2_fix_unknown_id_errors(test_db, temp_project):
    with pytest.raises(RuntimeError, match="No record"):
        fix_migration("01DOESNOTEXIST", mark_applied=True)


# --- M3: ordering and verification blind spots --------------------------------


def test_m3_out_of_order_blocked_then_allowed(test_db, temp_project):
    write_migration(f"{ID_B}_newer.sql", "CREATE TABLE n1 (id int);", "DROP TABLE n1;")
    apply_migrations()

    # A migration that sorts before the applied head arrives (branch merge).
    write_migration(f"{ID_A}_older.sql", "CREATE TABLE n0 (id int);", "DROP TABLE n0;")

    with pytest.raises(RuntimeError, match="Out-of-order"):
        apply_migrations()

    apply_migrations(allow_out_of_order=True)
    rows = dict(q("SELECT id, status FROM _migrations"))
    assert rows == {ID_A: "applied", ID_B: "applied"}


def test_m3_verify_detects_missing_file(test_db, temp_project):
    write_migration(f"{ID_A}_gone.sql", "CREATE TABLE gone_t (id int);", "DROP TABLE gone_t;")
    apply_migrations()

    os.remove(os.path.join("migrations", f"{ID_A}_gone.sql"))

    assert verify_checksums() is False


# --- M4: head ignores failed rows ---------------------------------------------


def test_m4_head_ignores_failed(test_db, temp_project):
    write_migration(f"{ID_A}_ok.sql", "CREATE TABLE h1 (id int);", "DROP TABLE h1;")
    apply_migrations()
    with psycopg.connect(TEST_DB_URL, autocommit=True) as c:
        c.execute(
            "INSERT INTO _migrations (id, name, checksum, status) "
            "VALUES (%s, 'broken', 'x', 'failed')",
            (ID_B,),
        )

    head = get_head()
    assert head is not None
    assert head["id"] == ID_A


# --- M5: read commands perform no DDL -----------------------------------------


def test_m5_read_commands_are_read_only(test_db, temp_project):
    write_migration(f"{ID_A}_x.sql", "SELECT 1;")

    status = get_migration_status()
    assert status == [{"id": ID_A, "name": "x", "status": "pending"}]
    assert get_head() is None
    assert verify_checksums() is True

    # None of the above may have created the tracking tables.
    assert not table_exists("_migrations")
    assert not table_exists("_migrations_log")


# --- M1: cleanup never masks the real error -----------------------------------


def test_m1_underlying_error_not_masked(test_db, temp_project):
    """A pre-existing foreign _migrations table used to surface as
    'current transaction is aborted' from the unlock; the real error must
    propagate instead."""
    with psycopg.connect(TEST_DB_URL, autocommit=True) as c:
        c.execute("CREATE TABLE _migrations (id varchar(26) PRIMARY KEY)")
    write_migration(f"{ID_A}_x.sql", "SELECT 1;")

    with pytest.raises(psycopg.errors.UndefinedColumn):
        apply_migrations()
