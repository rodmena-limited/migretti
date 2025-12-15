import os
import sys
import ulid
import re
import argparse
from typing import List
from migretti.core import get_migration_files, get_applied_migrations, parse_migration_sql
from migretti.io_utils import atomic_write
from migretti.logging_setup import get_logger
from migretti.db import get_connection

logger = get_logger()

def cmd_squash(args: argparse.Namespace) -> None:
    """
    Squash all pending migrations into a single new migration.
    """
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
        
    logger.info(f"Squashing {len(pending)} migrations:")
    for _, name, _ in pending:
        logger.info(f"  - {name}")
        
    # Concatenate SQL
    final_up: List[str] = []
    final_down: List[str] = []
    
    for _, _, filepath in pending:
        with open(filepath, "r", encoding="utf-8") as f:
            content = f.read()
        
        up, down, no_trans = parse_migration_sql(content)
        if no_trans:
            logger.warning("Warning: Squashing contains non-transactional migrations. Result will be marked transactional unless manually edited.")
            
        final_up.append(f"-- Source: {os.path.basename(filepath)}\n{up.strip()}")
        # Prepend to down to maintain reverse order
        final_down.insert(0, f"-- Source: {os.path.basename(filepath)}\n{down.strip()}")
        
    # Create new file
    name = args.name
    slug = re.sub(r'[^a-z0-9]+', '_', name.lower()).strip('_')
    migration_id = str(ulid.ULID())
    filename = f"{migration_id}_{slug}.sql"
    filepath = os.path.join("migrations", filename)
    
    template = """-- migration: {name} (Squashed)
-- id: {id}

-- migrate: up
{up_sql}

-- migrate: down
{down_sql}
"""
    try:
        with atomic_write(filepath, exclusive=True) as f:
            f.write(template.format(
                name=name, 
                id=migration_id,
                up_sql="\n\n".join(final_up),
                down_sql="\n\n".join(final_down)
            ))
        print(f"Created squashed migration: {filepath}")
        
        # Delete old files
        for _, _, old_path in pending:
            os.remove(old_path)
            logger.info(f"Deleted {old_path}")
            
    except Exception as e:
        logger.error(f"Failed to squash migrations: {e}")
        sys.exit(1)