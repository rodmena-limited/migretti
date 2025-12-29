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
