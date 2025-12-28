import os
import glob
import hashlib
import psycopg
import sqlparse
from typing import Tuple, List, Set, Optional, Dict, Any
from migretti.db import get_connection, ensure_schema, advisory_lock, get_lock_id
from migretti.logging_setup import get_logger
from migretti.hooks import execute_hook
logger = get_logger()

def parse_migration_sql(
    content: str, filepath: str = "<unknown>"
) -> Tuple[str, str, bool]:
    lines = content.splitlines()
    up_sql = []
    down_sql = []
    current_section = None
    no_transaction = False
    found_up = False
    found_down = False

    for line in lines:
        stripped = line.strip()
        if stripped.startswith("-- migrate: up"):
            current_section = "up"
            found_up = True
            continue
        elif stripped.startswith("-- migrate: down"):
            current_section = "down"
            found_down = True
            continue
        elif stripped.startswith("-- migrate: no-transaction"):
            no_transaction = True
            continue

        if current_section == "up":
            up_sql.append(line)
        elif current_section == "down":
            down_sql.append(line)

    up_sql_str = "\n".join(up_sql).strip()
    down_sql_str = "\n".join(down_sql).strip()

    # Validate up section (required)
    if not found_up:
        raise ValueError(f"Migration {filepath} missing '-- migrate: up' marker")
    if not up_sql_str:
        raise ValueError(f"Migration {filepath} has empty '-- migrate: up' section")

    # Warn about missing down section
    if not found_down or not down_sql_str:
        logger.warning(f"Migration {filepath} has no '-- migrate: down' section")

    return up_sql_str, down_sql_str, no_transaction

def calculate_checksum(content: str) -> str:
    return hashlib.sha256(content.encode("utf-8")).hexdigest()

def get_migration_files() -> List[Tuple[str, str, str]]:
    """Returns list of (id, name, filepath) sorted by id."""
    if not os.path.exists("migrations"):
        return []

    files = glob.glob(os.path.join("migrations", "*.sql"))
    migrations = []
    for f in files:
        basename = os.path.basename(f)
        # format: <id>_<slug>.sql
        parts = basename.split("_", 1)
        if len(parts) < 2:
            continue  # Skip invalid files
        mig_id = parts[0]
        # remove .sql from name
        name = parts[1][:-4] if parts[1].endswith(".sql") else parts[1]
        migrations.append((mig_id, name, f))

    migrations.sort(key=lambda x: x[0])
    return migrations

def get_applied_migrations(conn: psycopg.Connection[Any]) -> Set[str]:
    with conn.cursor() as cur:
        # Only consider successfully applied migrations as "done"
        cur.execute("SELECT id FROM _migrations WHERE status = 'applied'")
        return {row[0] for row in cur.fetchall()}

def check_failed_migrations(conn: psycopg.Connection[Any]) -> List[Tuple[str, str]]:
    with conn.cursor() as cur:
        cur.execute("SELECT id, name FROM _migrations WHERE status = 'failed'")
        return cur.fetchall()

def get_applied_migrations_details(
    conn: psycopg.Connection[Any],
) -> List[Tuple[str, str, str]]:
    """Returns list of (id, name, checksum) sorted by applied_at DESC, id DESC."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id, name, checksum FROM _migrations WHERE status = 'applied' ORDER BY applied_at DESC, id DESC"
        )
        return cur.fetchall()

def verify_checksums(env: Optional[str] = None) -> bool:
    """Verifies that applied migrations match files on disk."""
    conn = get_connection(env=env)
    try:
        ensure_schema(conn)
        applied = get_applied_migrations_details(conn)
        # Map ID -> Checksum
        applied_map = {m[0]: m[2] for m in applied}

        all_migrations = get_migration_files()

        issues = []

        for mig_id, name, filepath in all_migrations:
            if mig_id in applied_map:
                try:
                    with open(filepath, "r", encoding="utf-8") as f:
                        content = f.read()
                except OSError as e:
                    issues.append(f"Error reading {filepath}: {e}")
                    continue

                current_checksum = calculate_checksum(content)
                stored_checksum = applied_map[mig_id]

                if current_checksum != stored_checksum:
                    issues.append(f"Checksum mismatch for {mig_id} ({name})")

        if issues:
            for issue in issues:
                logger.error(issue)
            return False

        logger.info("All applied migrations match files on disk.")
        return True
    finally:
        conn.close()
