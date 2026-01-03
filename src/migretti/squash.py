import os
import shutil
import sys
from migretti.ulid import ULID
import re
import argparse
from typing import List
from migretti.core import (
    get_migration_files,
    get_applied_migrations,
    parse_migration_sql,
)
from migretti.io_utils import atomic_write
from migretti.logging_setup import get_logger
from migretti.db import get_connection

logger = get_logger()

BACKUP_DIR = "migrations/.squash_backup"


def cmd_squash(args: argparse.Namespace) -> None:
    """
    Squash all pending migrations into a single new migration.
    """
    dry_run = getattr(args, "dry_run", False)

    conn = get_connection(env=args.env)
    try:
        from migretti.db import ensure_schema

        ensure_schema(conn)
        applied_ids = get_applied_migrations(conn)
    finally:
        conn.close()

    all_migrations = get_migration_files()
    pending = [m for m in all_migrations if m[0] not in applied_ids]

    if not pending:
        logger.info("No pending migrations to squash.")
        return

    if len(pending) < 2:
        logger.info("Only 1 pending migration. Nothing to squash.")
        return

    prefix = "[DRY RUN] " if dry_run else ""
    logger.info(f"{prefix}Squashing {len(pending)} migrations:")
    for _, name, _ in pending:
        logger.info(f"  - {name}")

    # Concatenate SQL
    final_up: List[str] = []
    final_down: List[str] = []
    has_no_trans = False

    for _, _, filepath in pending:
        with open(filepath, "r", encoding="utf-8") as f:
            content = f.read()

        up, down, no_trans = parse_migration_sql(content, filepath)
        if no_trans:
            has_no_trans = True

        final_up.append(f"-- Source: {os.path.basename(filepath)}\n{up.strip()}")
        # Prepend to down to maintain reverse order
        final_down.insert(0, f"-- Source: {os.path.basename(filepath)}\n{down.strip()}")

    if has_no_trans:
        logger.warning(
            "Warning: Squashing contains non-transactional migrations. "
            "Result will be marked transactional unless manually edited."
        )

    # Build new migration content
    name = args.name
    slug = re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")
    migration_id = str(ULID())
    filename = f"{migration_id}_{slug}.sql"
    new_filepath = os.path.join("migrations", filename)

    template = """-- migration: {name} (Squashed)
-- id: {id}

-- migrate: up
{up_sql}

-- migrate: down
{down_sql}
"""
    new_content = template.format(
        name=name,
        id=migration_id,
        up_sql="\n\n".join(final_up),
        down_sql="\n\n".join(final_down),
    )

    if dry_run:
        logger.info(f"{prefix}Would create: {new_filepath}")
        logger.info(f"{prefix}Content preview:\n{new_content[:500]}...")
        logger.info(f"{prefix}Would backup and delete {len(pending)} original files")
        for _, _, old_path in pending:
            logger.info(f"{prefix}  - {old_path}")
        return

    # Actual squash operation
    try:
        # Step 1: Create backup directory
        os.makedirs(BACKUP_DIR, exist_ok=True)

        # Step 2: Backup original files
        backed_up: List[str] = []
        for _, _, old_path in pending:
            backup_path = os.path.join(BACKUP_DIR, os.path.basename(old_path))
            shutil.copy2(old_path, backup_path)
            backed_up.append(backup_path)
            logger.info(f"Backed up {old_path} -> {backup_path}")

        # Step 3: Create new squashed migration
        with atomic_write(new_filepath, exclusive=True) as f:
            f.write(new_content)

        # Step 4: Verify new file exists and has content
        if not os.path.exists(new_filepath):
            raise RuntimeError(f"Failed to create {new_filepath}")
        if os.path.getsize(new_filepath) == 0:
            raise RuntimeError(f"Created file {new_filepath} is empty")

        print(f"Created squashed migration: {new_filepath}")

        # Step 5: Delete original files (only after confirming new file exists)
        for _, _, old_path in pending:
            os.remove(old_path)
            logger.info(f"Deleted {old_path}")

        print(f"Backups saved to: {BACKUP_DIR}/")
        print("You can delete backups after verifying the squashed migration works.")

    except Exception as e:
        logger.error(f"Failed to squash migrations: {e}")
        logger.error("Original files may be restored from backups in: " + BACKUP_DIR)
        sys.exit(1)
