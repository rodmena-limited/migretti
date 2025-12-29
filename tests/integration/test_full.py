import pytest
import psycopg
import sys
import os
import shutil
import tempfile
from migretti import __main__ as main_mod
from migretti.core import (
    apply_migrations,
    rollback_migrations,
    get_migration_status,
    verify_checksums,
)
from migretti.db import get_connection
from migretti.logging_setup import setup_logging

# Test DB Config
TEST_DB_NAME = "migretti_test"
TEST_DB_URL = f"postgresql://postgres:postgres@localhost:5432/{TEST_DB_NAME}"
ASSETS_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "test_assets", "migrations")
)


@pytest.fixture(scope="function")
def test_db():
    """
    Sets up a clean test database environment.
    """
    try:
        conn = psycopg.connect(TEST_DB_URL, autocommit=True)
    except psycopg.OperationalError:
        pytest.fail(
            f"Could not connect to test database at {TEST_DB_URL}. Is it running?"
        )

    with conn.cursor() as cur:
        cur.execute("DROP SCHEMA public CASCADE;")
        cur.execute("CREATE SCHEMA public;")

    conn.close()

    os.environ["MG_DATABASE_URL"] = TEST_DB_URL
    yield TEST_DB_URL
    del os.environ["MG_DATABASE_URL"]


@pytest.fixture(scope="function")
def temp_project():
    """
    Creates a temporary directory, sets it as CWD, and initializes migretti.
    """
    setup_logging(verbose=True)
    old_cwd = os.getcwd()
    tmp_dir = tempfile.mkdtemp()
    os.chdir(tmp_dir)

    # Init project
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


def test_full_lifecycle(test_db, temp_project):
    """
    Test: Init -> Copy Static Migration -> Apply -> Verify -> Rollback
    Uses: 01_create_users.sql
    """
    copy_asset("01_create_users.sql")

    # 2. Apply
    apply_migrations()

    # Check DB
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(
            "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'users')"
        )
        assert cur.fetchone()[0] is True
    conn.close()

    # 3. Status
    status = get_migration_status()
    assert len(status) == 1
    assert status[0]["status"] == "applied"

    # 4. Verify Checksums
    assert verify_checksums() is True

    # 5. Rollback
    rollback_migrations()

    # Check DB
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(
            "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'users')"
        )
        assert cur.fetchone()[0] is False
    conn.close()

    # 6. Status
    status = get_migration_status()
    assert len(status) == 1
    assert status[0]["status"] == "pending"


def test_non_transactional_migration(test_db, temp_project):
    """
    Test: -- migrate: no-transaction
    Uses: 02_concurrent_index.sql
    """
    copy_asset("02_concurrent_index.sql")

    # Apply
    apply_migrations()

    # Verify index exists
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(
            "SELECT EXISTS (SELECT FROM pg_class WHERE relname = 'idx_test_conc')"
        )
        assert cur.fetchone()[0] is True
    conn.close()

    # Rollback
    rollback_migrations()

    # Verify gone
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(
            "SELECT EXISTS (SELECT FROM pg_class WHERE relname = 'idx_test_conc')"
        )
        assert cur.fetchone()[0] is False
    conn.close()


def test_dry_run(test_db, temp_project, capsys):
    """
    Test: --dry-run does not apply changes
    Uses: 07_dry_run.sql
    """
    copy_asset("07_dry_run.sql")

    # Apply with dry_run
    apply_migrations(dry_run=True)

    # Check status - should be pending
    status = get_migration_status()
    assert status[0]["status"] == "pending"

    # Apply real
    apply_migrations()
    status = get_migration_status()
    assert status[0]["status"] == "applied"


def test_verify_checksum_failure(test_db, temp_project):
    """
    Test: Checksum mismatch detection
    Uses: 06_tamper.sql
    """
    dst = copy_asset("06_tamper.sql")

    apply_migrations()

    assert verify_checksums() is True

    # Tamper with file
    with open(dst, "a") as f:
        f.write("\n-- modified")

    assert verify_checksums() is False


def test_prod_protection(test_db, temp_project, monkeypatch):
    """
    Test: Production environment requires confirmation
    """
    os.environ["MG_ENV"] = "prod"

    def mock_exit(code):
        if code != 0:
            raise SystemExit(code)

    monkeypatch.setattr(sys, "exit", mock_exit)
    monkeypatch.setattr("builtins.input", lambda _: "no")

    class Args:
        env = "prod"
        dry_run = False
        yes = False
        steps = 1

    try:
        main_mod.cmd_apply(Args())
    except SystemExit as e:
        assert e.code == 0

    monkeypatch.setattr("builtins.input", lambda _: "yes")
    main_mod.cmd_apply(Args())
