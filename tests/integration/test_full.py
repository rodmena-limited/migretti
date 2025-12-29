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
TEST_DB_NAME = "migretti_test"
TEST_DB_URL = f"postgresql://postgres:postgres@localhost:5432/{TEST_DB_NAME}"
ASSETS_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "test_assets", "migrations")
)

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
