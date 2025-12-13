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

# Re-use fixtures from test_full via imports or copy-paste (prefer copy for isolation)
TEST_DB_NAME = "migretti_test"
TEST_DB_URL = f"postgresql://postgres:postgres@localhost:5432/{TEST_DB_NAME}"

@pytest.fixture(scope="function")
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

@pytest.fixture(scope="function")
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

def worker_apply():
    """Worker function to apply migrations in a separate process."""
    # Re-setup env in worker
    os.environ["MG_DATABASE_URL"] = TEST_DB_URL 
    setup_logging(verbose=True)
    try:
        apply_migrations()
        return True
    except Exception as e:
        print(f"Worker failed: {e}")
        return False

def test_concurrent_migrations(test_db_complex, temp_project_complex):
    """
    Test: Two processes try to apply migrations at the same time.
    One should succeed, the other might wait or just do nothing (idempotent).
    """
    # Create a migration that takes some time (e.g., pg_sleep)
    # to ensure overlap window is wide enough.
    class Args:
        name = "Slow Migration"
    main_mod.cmd_create(Args())
    
    migrations = os.listdir("migrations")
    mig_file = os.path.join("migrations", migrations[0])
    
    with open(mig_file, "w") as f:
        f.write("-- migration: Slow\n")
        f.write("-- migrate: up\n")
        f.write("CREATE TABLE slow_test (id INT);\n")
        f.write("SELECT pg_sleep(2);\n") # Sleep 2 seconds inside transaction
        f.write("-- migrate: down\n")
        f.write("DROP TABLE slow_test;\n")
        
    # Start two processes
    p1 = multiprocessing.Process(target=worker_apply)
    p2 = multiprocessing.Process(target=worker_apply)
    
    start_time = time.time()
    
    p1.start()
    p2.start()
    
    p1.join()
    p2.join()
    
    end_time = time.time()
    
    # Verify execution
    status = get_migration_status()
    assert len(status) == 1
    assert status[0]['status'] == 'applied'
    
    # Verify log contains exactly one "UP"
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM _migrations_log WHERE action='UP'")
        count = cur.fetchone()[0]
        # Should be 1 because the second process sees it as applied (or waits and sees it applied)
        assert count == 1
    conn.close()

def test_rollback_atomicity(test_db_complex, temp_project_complex):
    """
    Test: Migration fails halfway. Verify NO changes persisted.
    """
    class Args:
        name = "Fail Migration"
    main_mod.cmd_create(Args())
    
    migrations = os.listdir("migrations")
    mig_file = os.path.join("migrations", migrations[0])
    
    with open(mig_file, "w") as f:
        f.write("-- migration: Fail\n")
        f.write("-- migrate: up\n")
        f.write("CREATE TABLE should_not_exist (id INT);\n")
        f.write("INSERT INTO should_not_exist VALUES (1);\n")
        f.write("SELECT 1/0;\n") # Division by zero error
        f.write("-- migrate: down\n")
        f.write("DROP TABLE should_not_exist;\n")
        
    # Apply should raise error
    with pytest.raises(psycopg.DataError): # Division by zero
        apply_migrations()
        
    # Verify DB state
    conn = get_connection()
    with conn.cursor() as cur:
        # Table should NOT exist
        cur.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'should_not_exist')")
        assert cur.fetchone()[0] is False
    conn.close()
    
    # Status should be pending
    status = get_migration_status()
    assert status[0]['status'] == 'pending'

def test_large_dataset_performance(test_db_complex, temp_project_complex):
    """
    Test: Migration on 100k rows.
    """
    # 1. Setup Data
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute("CREATE TABLE big_data (id SERIAL PRIMARY KEY, val TEXT);")
        # Generate 100k rows
        cur.execute("INSERT INTO big_data (val) SELECT 'value-' || generate_series(1, 100000);")
    conn.commit()
    conn.close()
    
    # 2. Create Migration to Add Index
    class Args:
        name = "Add Index"
    main_mod.cmd_create(Args())
    
    migrations = os.listdir("migrations")
    mig_file = os.path.join("migrations", migrations[0])
    
    with open(mig_file, "w") as f:
        f.write("-- migration: Add Index\n")
        f.write("-- migrate: up\n")
        f.write("CREATE INDEX idx_big_data_val ON big_data(val);\n")
        f.write("-- migrate: down\n")
        f.write("DROP INDEX idx_big_data_val;\n")
        
    # 3. Apply
    start = time.time()
    apply_migrations()
    duration = time.time() - start
    
    print(f"Applied index on 100k rows in {duration:.2f}s")
    
    # Verify index
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute("SELECT EXISTS (SELECT FROM pg_class WHERE relname = 'idx_big_data_val')")
        assert cur.fetchone()[0] is True
    conn.close()
