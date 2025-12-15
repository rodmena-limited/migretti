import pytest
import os
import psycopg
from migretti.__main__ import cmd_seed
from migretti.logging_setup import setup_logging
import tempfile
from migretti import __main__ as main_mod

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
    import shutil

    shutil.rmtree(tmp_dir)


def test_seed_create(capsys, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    class Args:
        seed_command = "create"
        name = "init_data"

    cmd_seed(Args())

    assert (tmp_path / "seeds/init_data.sql").exists()


def test_seed_run(capsys, test_db, temp_project):
    # Create a seed file
    os.makedirs("seeds", exist_ok=True)
    with open("seeds/01_data.sql", "w") as f:
        f.write(
            "CREATE TABLE IF NOT EXISTS seed_test (id INT); INSERT INTO seed_test VALUES (1);"
        )

    class Args:
        seed_command = None  # run
        env = None

    cmd_seed(Args())

    # Verify execution
    import psycopg

    conn = psycopg.connect(
        os.environ["MG_DATABASE_URL"]
    )  # test_db fixture sets this? No, I need to check how test_db works in this file.
    # Ah, test_db fixture in test_full.py sets env var but tears it down.
    # I need to use the connection to verify.

    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM seed_test")
        assert cur.fetchone()[0] == 1
    conn.close()
