import pytest
import os
import glob
from migretti.__main__ import cmd_squash, cmd_create
from migretti.logging_setup import setup_logging
from migretti import __main__ as main_mod
import psycopg
import tempfile

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


def test_squash(capsys, test_db, temp_project):
    # 1. Create 2 migrations
    class CreateArgs:
        name = "Mig1"

    cmd_create(CreateArgs())

    # Needs delay to ensure order? ULID uses time but millisecond precision.
    import time

    time.sleep(0.01)

    class CreateArgs2:
        name = "Mig2"

    cmd_create(CreateArgs2())

    files = glob.glob("migrations/*.sql")
    assert len(files) == 2

    # 2. Squash them
    class SquashArgs:
        name = "Squashed"
        env = None

    cmd_squash(SquashArgs())

    # 3. Verify
    files = glob.glob("migrations/*.sql")
    assert len(files) == 1
    assert "squashed" in files[0]

    with open(files[0], "r") as f:
        content = f.read()
        assert "-- Source:" in content
        assert "mig1" in content.lower()
        assert "mig2" in content.lower()
