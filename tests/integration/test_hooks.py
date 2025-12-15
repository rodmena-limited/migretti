import pytest
import os
import shutil
import tempfile
import psycopg
from migretti.core import apply_migrations
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

def test_hooks_execution(monkeypatch, caplog, tmp_path, test_db, temp_project):
    # Mock config to return hooks
    # We can write to mg.yaml in temp_project
    
    with open("mg.yaml", "a") as f:
        f.write("""
hooks:
  pre_apply: echo "Running pre_apply hook"
  post_apply: echo "Running post_apply hook"
""")
    
    # Create migration
    from migretti.__main__ import cmd_create
    class Args:
        name = "Hook Test"
    cmd_create(Args())
    
    # Apply
    import logging
    with caplog.at_level(logging.INFO):
        apply_migrations()
    
    # Check logs
    assert "Running pre_apply hook" in caplog.text
    assert "Hook output: Running pre_apply hook" in caplog.text
    assert "Running post_apply hook" in caplog.text
    assert "Hook output: Running post_apply hook" in caplog.text