import pytest
import psycopg
import os
import shutil
import tempfile
import multiprocessing
import time
from migretti import __main__ as main_mod
from migretti.core import apply_migrations, get_migration_status
from migretti.db import get_connection
from migretti.logging_setup import setup_logging

TEST_DB_NAME = "migretti_test"
TEST_DB_URL = f"postgresql://postgres:postgres@localhost:5432/{TEST_DB_NAME}"
ASSETS_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "test_assets", "migrations")
)


@pytest.fixture(scope="function")
def test_db_complex():
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
def temp_project_complex():
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


def copy_asset(filename, dest_name=None):
    src = os.path.join(ASSETS_DIR, filename)
    if dest_name is None:
        dest_name = filename
    dst = os.path.join("migrations", dest_name)
    shutil.copy(src, dst)
    return dst


def worker_apply():
    os.environ["MG_DATABASE_URL"] = TEST_DB_URL
    setup_logging(verbose=True)
    try:
        apply_migrations()
        return True
    except Exception as e:
        print(f"Worker failed: {e}")
        return False


def test_concurrent_migrations(test_db_complex, temp_project_complex):
    """
    Test: Concurrent Migrations (Advisory Locks)
    Uses: 03_slow.sql
    """
    copy_asset("03_slow.sql")

    p1 = multiprocessing.Process(target=worker_apply)
    p2 = multiprocessing.Process(target=worker_apply)

    p1.start()
    p2.start()

    p1.join()
    p2.join()

    status = get_migration_status()
    assert len(status) == 1
    assert status[0]["status"] == "applied"

    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM _migrations_log WHERE action='UP'")
        count = cur.fetchone()[0]
        assert count == 1
    conn.close()


def test_rollback_atomicity(test_db_complex, temp_project_complex):
    """
    Test: Rollback Atomicity on Failure
    Uses: 04_fail.sql
    """
    copy_asset("04_fail.sql")

    with pytest.raises(psycopg.DataError):  # Division by zero
        apply_migrations()

    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(
            "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'should_not_exist')"
        )
        assert cur.fetchone()[0] is False
    conn.close()

    status = get_migration_status()
    assert status[0]["status"] == "pending"


def test_large_dataset_performance(test_db_complex, temp_project_complex):
    """
    Test: Large Dataset Performance
    Uses: 05_add_index.sql
    """
    # 1. Setup Data
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute("CREATE TABLE big_data (id SERIAL PRIMARY KEY, val TEXT);")
        cur.execute(
            "INSERT INTO big_data (val) SELECT 'value-' || generate_series(1, 100000);"
        )
    conn.commit()
    conn.close()

    # 2. Add Index Migration
    copy_asset("05_add_index.sql")

    # 3. Apply
    start = time.time()
    apply_migrations()
    duration = time.time() - start

    print(f"Applied index on 100k rows in {duration:.2f}s")

    # Verify index
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(
            "SELECT EXISTS (SELECT FROM pg_class WHERE relname = 'idx_big_data_val')"
        )
        assert cur.fetchone()[0] is True
    conn.close()
