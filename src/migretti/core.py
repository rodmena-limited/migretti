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
