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


def _add_sql_to_migration(filepath: str, up_sql: str, down_sql: str) -> None:
    """Helper to add SQL content to a migration file."""
    with open(filepath, "r") as f:
        content = f.read()
    content = content.replace(
        "-- migrate: up\n\n",
        f"-- migrate: up\n{up_sql}\n"
    )
    content = content.replace(
        "-- migrate: down\n\n",
        f"-- migrate: down\n{down_sql}\n"
    )
    with open(filepath, "w") as f:
        f.write(content)


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

    files = sorted(glob.glob("migrations/*.sql"))
    assert len(files) == 2

    # Add SQL content to migrations
    _add_sql_to_migration(files[0], "CREATE TABLE mig1 (id INT);", "DROP TABLE mig1;")
    _add_sql_to_migration(files[1], "CREATE TABLE mig2 (id INT);", "DROP TABLE mig2;")

    # 2. Squash them
    class SquashArgs:
        name = "Squashed"
        env = None
        dry_run = False

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

    # 4. Verify backups exist
    backup_files = glob.glob("migrations/.squash_backup/*.sql")
    assert len(backup_files) == 2


def test_squash_dry_run(capsys, test_db, temp_project):
    """Dry run should not modify any files."""
    import time

    # 1. Create 2 migrations
    class CreateArgs:
        name = "DryMig1"

    cmd_create(CreateArgs())
    time.sleep(0.01)

    class CreateArgs2:
        name = "DryMig2"

    cmd_create(CreateArgs2())

    files = sorted(glob.glob("migrations/*.sql"))
    assert len(files) == 2

    # Add SQL content to migrations
    _add_sql_to_migration(files[0], "CREATE TABLE dry1 (id INT);", "DROP TABLE dry1;")
    _add_sql_to_migration(files[1], "CREATE TABLE dry2 (id INT);", "DROP TABLE dry2;")

    files_before = set(glob.glob("migrations/*.sql"))

    # 2. Dry run squash
    class SquashArgs:
        name = "DrySquashed"
        env = None
        dry_run = True

    cmd_squash(SquashArgs())

    # 3. Verify no changes
    files_after = set(glob.glob("migrations/*.sql"))
    assert files_before == files_after
    assert not os.path.exists("migrations/.squash_backup")


def test_squash_creates_backups(capsys, test_db, temp_project):
    """Squash should create backups of original files."""
    import time

    # 1. Create 2 migrations with content
    class CreateArgs:
        name = "BackupMig1"

    cmd_create(CreateArgs())
    time.sleep(0.01)

    class CreateArgs2:
        name = "BackupMig2"

    cmd_create(CreateArgs2())

    original_files = sorted(glob.glob("migrations/*.sql"))
    assert len(original_files) == 2

    # Add SQL content to migrations
    _add_sql_to_migration(original_files[0], "CREATE TABLE bak1 (id INT);", "DROP TABLE bak1;")
    _add_sql_to_migration(original_files[1], "CREATE TABLE bak2 (id INT);", "DROP TABLE bak2;")

    # Read original content
    original_contents = {}
    for f in original_files:
        with open(f, "r") as fp:
            original_contents[os.path.basename(f)] = fp.read()

    # 2. Squash
    class SquashArgs:
        name = "BackupSquashed"
        env = None
        dry_run = False

    cmd_squash(SquashArgs())

    # 3. Verify backups match originals
    backup_files = glob.glob("migrations/.squash_backup/*.sql")
    assert len(backup_files) == 2

    for backup_path in backup_files:
        basename = os.path.basename(backup_path)
        with open(backup_path, "r") as fp:
            backup_content = fp.read()
        assert backup_content == original_contents[basename]
