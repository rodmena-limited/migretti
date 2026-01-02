import pytest
import psycopg
import os
import shutil
import tempfile
from migretti import __main__ as main_mod
from migretti.logging_setup import setup_logging

TEST_DB_NAME = "migretti_test"
TEST_DB_URL = f"postgresql://postgres:postgres@localhost:5432/{TEST_DB_NAME}"


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


def test_smart_dry_run_failure(test_db, temp_project):
    """
    Test: Smart Dry Run catches SQL errors without applying changes.
    """
    # Create invalid migration
    import pytest
    from migretti.core import apply_migrations, get_migration_status

    mig_id = "01KCCFAILDRY"
    with open(f"migrations/{mig_id}_invalid.sql", "w") as f:
        f.write("-- migrate: up\n")
        f.write("SELECT * FROM non_existent_table;\n")
        f.write("-- migrate: down\n")
        f.write("SELECT 1;\n")

    # Apply dry-run - should fail
    import psycopg

    with pytest.raises(psycopg.errors.UndefinedTable):
        apply_migrations(dry_run=True)

    # Verify status is still pending
    status = get_migration_status()
    assert status[0]["status"] == "pending"
