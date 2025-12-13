import pytest
import psycopg
import sys
import os
import shutil
import tempfile
from contextlib import contextmanager
from migretti import __main__ as main_mod
from migretti.core import apply_migrations, rollback_migrations, get_migration_status, verify_checksums
from migretti.db import get_connection

# Test DB Config
TEST_DB_NAME = "migretti_test"
TEST_DB_URL = f"postgresql://postgres:postgres@localhost:5432/{TEST_DB_NAME}"

@pytest.fixture(scope="function")
def test_db():
    """
    Sets up a clean test database environment.
    Connects to the test database, cleans all tables, and yields the connection string.
    """
    # Connect to the DB to clean it up
    try:
        conn = psycopg.connect(TEST_DB_URL, autocommit=True)
    except psycopg.OperationalError:
        pytest.fail(f"Could not connect to test database at {TEST_DB_URL}. Is it running?")

    # Drop all tables in public schema
    with conn.cursor() as cur:
        cur.execute("DROP SCHEMA public CASCADE;")
        cur.execute("CREATE SCHEMA public;")
    
    conn.close()
    
    # Set env var for the test
    os.environ["MG_DATABASE_URL"] = TEST_DB_URL
    yield TEST_DB_URL
    # Cleanup env var
    del os.environ["MG_DATABASE_URL"]

from migretti.logging_setup import setup_logging

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
    # We mock args
    class Args:
        pass
    args = Args()
    main_mod.cmd_init(args)
    
    yield tmp_dir
    
    os.chdir(old_cwd)
    shutil.rmtree(tmp_dir)

def test_full_lifecycle(test_db, temp_project):
    """
    Test: Init -> Create -> Apply -> Verify -> Rollback
    """
    # 1. Create Migration
    class Args:
        name = "Create Users"
    main_mod.cmd_create(Args())
    
    # Modify the created migration file
    migrations = os.listdir("migrations")
    assert len(migrations) == 1
    mig_file = os.path.join("migrations", migrations[0])
    
    with open(mig_file, "w") as f:
        f.write("-- migration: Create Users\n")
        f.write("-- migrate: up\n")
        f.write("CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT);\n")
        f.write("-- migrate: down\n")
        f.write("DROP TABLE users;\n")
    
    # 2. Apply
    apply_migrations()
    
    # Check DB
    conn = get_connection()
    with conn.cursor() as cur:
        # Check table existence using information_schema or casting
        cur.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'users')")
        assert cur.fetchone()[0] is True
    conn.close()
    
    # 3. Status
    status = get_migration_status()
    assert len(status) == 1
    assert status[0]['status'] == 'applied'
    
    # 4. Verify Checksums
    assert verify_checksums() is True
    
    # 5. Rollback
    rollback_migrations()
    
    # Check DB
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'users')")
        assert cur.fetchone()[0] is False
    conn.close()
    
    # 6. Status
    status = get_migration_status()
    assert len(status) == 1
    assert status[0]['status'] == 'pending'

def test_non_transactional_migration(test_db, temp_project):
    """
    Test: -- migrate: no-transaction
    """
    class Args:
        name = "Concurrent Index"
    main_mod.cmd_create(Args())
    
    migrations = os.listdir("migrations")
    mig_file = os.path.join("migrations", migrations[0])
    
    # Write a non-transactional migration
    with open(mig_file, "w") as f:
        f.write("-- migrate: no-transaction\n")
        f.write("-- migrate: up\n")
        f.write("CREATE TABLE IF NOT EXISTS test_conc (id INT);\n")
        # CREATE INDEX CONCURRENTLY cannot run in a transaction block
        f.write("CREATE INDEX CONCURRENTLY idx_test_conc ON test_conc(id);\n")
        f.write("-- migrate: down\n")
        f.write("DROP INDEX CONCURRENTLY idx_test_conc;\n")
        f.write("DROP TABLE test_conc;\n")
        
    # Apply
    apply_migrations()
    
    # Verify index exists
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute("SELECT EXISTS (SELECT FROM pg_class WHERE relname = 'idx_test_conc')")
        assert cur.fetchone()[0] is True
    conn.close()
    
    # Rollback
    rollback_migrations()
    
    # Verify gone
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute("SELECT EXISTS (SELECT FROM pg_class WHERE relname = 'idx_test_conc')")
        assert cur.fetchone()[0] is False
    conn.close()

def test_dry_run(test_db, temp_project, capsys):
    """
    Test: --dry-run does not apply changes
    """
    class Args:
        name = "Dry Run Test"
    main_mod.cmd_create(Args())
    
    # Apply with dry_run
    apply_migrations(dry_run=True)
    
    # Check DB - should be empty (no _migrations table even)
    # Actually ensure_schema might run inside apply_migrations before check? 
    # Let's check status - should be pending
    status = get_migration_status() # this will create schema if not exists
    assert status[0]['status'] == 'pending'
    
    # Apply real
    apply_migrations()
    status = get_migration_status()
    assert status[0]['status'] == 'applied'

def test_verify_checksum_failure(test_db, temp_project):
    """
    Test: Checksum mismatch detection
    """
    class Args:
        name = "Tamper Test"
    main_mod.cmd_create(Args())
    
    apply_migrations()
    
    assert verify_checksums() is True
    
    # Tamper with file
    migrations = os.listdir("migrations")
    mig_file = os.path.join("migrations", migrations[0])
    with open(mig_file, "a") as f:
        f.write("\n-- modified")
        
    assert verify_checksums() is False

def test_prod_protection(test_db, temp_project, monkeypatch):
    """
    Test: Production environment requires confirmation
    """
    os.environ["MG_ENV"] = "prod"
    
    # Mock sys.exit to catch it
    def mock_exit(code):
        if code != 0:
            raise SystemExit(code)
    monkeypatch.setattr(sys, "exit", mock_exit)
    
    # Mock input to say 'no'
    monkeypatch.setattr("builtins.input", lambda _: "no")
    
    class Args:
        env = "prod"
        dry_run = False
        yes = False
        steps = 1
        
    # Should exit
    # We call cmd_apply directly which calls check_prod_protection
    # check_prod_protection calls sys.exit(0) if no
    try:
        main_mod.cmd_apply(Args())
    except SystemExit as e:
        assert e.code == 0
    
    # Mock input to say 'yes'
    monkeypatch.setattr("builtins.input", lambda _: "yes")
    
    # Should proceed (will do nothing as no migrations, but won't exit)
    main_mod.cmd_apply(Args())