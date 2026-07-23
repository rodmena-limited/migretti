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
    check_failed_migrations,
    parse_migration_sql,
)
from migretti.io_utils import atomic_write
from migretti.logging_setup import get_logger
from migretti.db import get_connection, advisory_lock, ensure_schema, get_lock_id

logger = get_logger()

BACKUP_DIR = "migrations/.squash_backup"


def cmd_squash(args: argparse.Namespace) -> None:
    """
    Squash all pending migrations into a single new migration.

    Runs under the advisory lock so a concurrent `mg apply` cannot consume one
    of the pending files while it is being squashed away.
    """
    dry_run = getattr(args, "dry_run", False)
    env = getattr(args, "env", None)

    conn = get_connection(env=env)
    lock_id = get_lock_id(env=env)
    try:
        with advisory_lock(conn, lock_id=lock_id):
            ensure_schema(conn)

            failed = check_failed_migrations(conn)
            if failed:
                logger.error(
                    "Database is in a dirty state; resolve failed migrations "
                    "with 'mg fix' before squashing:"
                )
                for fid, fname in failed:
                    logger.error(f"  - {fid} ({fname})")
                sys.exit(1)

            applied_ids = get_applied_migrations(conn)

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
                final_down.insert(
                    0, f"-- Source: {os.path.basename(filepath)}\n{down.strip()}"
                )

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
                logger.info(
                    f"{prefix}Would backup and delete {len(pending)} original files"
                )
                for _, _, old_path in pending:
                    logger.info(f"{prefix}  - {old_path}")
                return

            # Actual squash operation. Ordering is chosen so that no crash
            # point can leave both the originals and the squashed file active
            # at once (which would double-apply the same SQL): the squashed
            # file only becomes visible (*.sql) after the originals are gone.
            tmp_filepath = new_filepath + ".tmp"
            try:
                # Step 1: Create backup directory
                os.makedirs(BACKUP_DIR, exist_ok=True)

                # Step 2: Backup original files
                for _, _, old_path in pending:
                    backup_path = os.path.join(BACKUP_DIR, os.path.basename(old_path))
                    shutil.copy2(old_path, backup_path)
                    logger.info(f"Backed up {old_path} -> {backup_path}")

                # Step 3: Write the squashed migration to a temp name that the
                # migration scanner does not pick up.
                with atomic_write(tmp_filepath, exclusive=True) as f:
                    f.write(new_content)

                if not os.path.exists(tmp_filepath):
                    raise RuntimeError(f"Failed to create {tmp_filepath}")
                if os.path.getsize(tmp_filepath) == 0:
                    raise RuntimeError(f"Created file {tmp_filepath} is empty")

                # Step 4: Delete original files
                for _, _, old_path in pending:
                    os.remove(old_path)
                    logger.info(f"Deleted {old_path}")

                # Step 5: Activate the squashed migration
                os.replace(tmp_filepath, new_filepath)

                print(f"Created squashed migration: {new_filepath}")
                print(f"Backups saved to: {BACKUP_DIR}/")
                print("You can delete backups after verifying the squashed migration works.")

            except Exception as e:
                logger.error(f"Failed to squash migrations: {e}")
                logger.error(
                    "Original files may be restored from backups in: " + BACKUP_DIR
                )
                if os.path.exists(tmp_filepath):
                    try:
                        os.remove(tmp_filepath)
                    except OSError:
                        pass
                sys.exit(1)
    finally:
        conn.close()
