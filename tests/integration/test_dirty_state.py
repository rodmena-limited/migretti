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


def test_dirty_state_recovery(test_db, temp_project):
    """
    Test: Non-transactional migration fails -> Status is 'failed' -> Apply blocks -> Manual fix.
    """
    # 1. Create a partial failure migration (non-transactional)
    # 02_concurrent_index.sql is good, but we need it to FAIL halfway.
    # We can fake this by injecting a failure after the first statement.

    # Manually create file

    mig_id = "01KCC999FAIL"
    filename = f"{mig_id}_fail_conc.sql"
    with open(f"migrations/{filename}", "w") as f:
        f.write("-- migrate: no-transaction\n")
        f.write("-- migrate: up\n")
        f.write("CREATE TABLE IF NOT EXISTS partial (id INT);\n")
        f.write("SELECT 1/0;\n")  # Fail here
        f.write("-- migrate: down\n")
        f.write("DROP TABLE partial;\n")

    # 2. Apply - should raise error
    import pytest
    import psycopg
    from migretti.core import apply_migrations, get_migration_status, get_connection

    with pytest.raises(psycopg.DataError):  # Div by zero
        apply_migrations()

    # 3. Check Status
    status = get_migration_status()
    assert len(status) == 1
    assert status[0]["status"] == "failed"

    # 4. Check DB state - table should exist (partial apply)
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(
            "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'partial')"
        )
        assert cur.fetchone()[0] is True

        # Check _migrations table
        cur.execute("SELECT status FROM _migrations WHERE id = %s", (mig_id,))
        assert cur.fetchone()[0] == "failed"
    conn.close()

    # 5. Try Apply again - should be blocked
    with pytest.raises(RuntimeError, match="Dirty database state"):
        apply_migrations()
