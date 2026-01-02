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
