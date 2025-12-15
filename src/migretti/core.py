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


def parse_migration_sql(content: str) -> Tuple[str, str, bool]:
    lines = content.splitlines()
    up_sql = []
    down_sql = []
    current_section = None
    no_transaction = False

    for line in lines:
        stripped = line.strip()
        if stripped.startswith("-- migrate: up"):
            current_section = "up"
            continue
        elif stripped.startswith("-- migrate: down"):
            current_section = "down"
            continue
        elif stripped.startswith("-- migrate: no-transaction"):
            no_transaction = True
            continue

        if current_section == "up":
            up_sql.append(line)
        elif current_section == "down":
            down_sql.append(line)

    return "\n".join(up_sql), "\n".join(down_sql), no_transaction


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


def get_applied_migrations_details(conn: psycopg.Connection[Any]) -> List[Tuple[str, str, str]]:
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


def rollback_migrations(steps: int = 1, env: Optional[str] = None, dry_run: bool = False) -> None:
    execute_hook("pre_rollback", env=env)
    conn = get_connection(env=env)
    lock_id = get_lock_id(env=env)
    try:
        with advisory_lock(conn, lock_id=lock_id):
            ensure_schema(conn)

            applied = get_applied_migrations_details(conn)
            if not applied:
                logger.info("No migrations to rollback.")
                return

            to_rollback = applied[:steps]
            logger.info(f"Rolling back {len(to_rollback)} migrations.")

            # Need to map ID to filepath
            all_migrations_files = get_migration_files()  # [(id, name, path)]
            id_to_path = {m[0]: m[2] for m in all_migrations_files}

            for mig_id, name, _ in to_rollback:
                filepath = id_to_path.get(mig_id)
                if not filepath:
                    logger.warning(
                        f"Migration file for {mig_id} ({name}) not found. Cannot rollback SQL."
                    )
                    raise RuntimeError(f"File for migration {mig_id} not found.")

                try:
                    with open(filepath, "r", encoding="utf-8") as f:
                        content = f.read()
                except OSError as e:
                    logger.error(f"Error reading {filepath}: {e}")
                    raise

                _, down_sql, no_transaction = parse_migration_sql(content)
                checksum = calculate_checksum(content)

                if dry_run:
                    logger.info(f"[DRY RUN] Rolling back {mig_id} - {name}")
                    logger.info(f"[DRY RUN] SQL:\n{down_sql}")
                    
                    if not no_transaction:
                        try:
                            with conn.transaction():
                                with conn.cursor() as cur:
                                    logger.info(f"[DRY RUN] Verifying Rollback SQL for {mig_id}...")
                                    if down_sql.strip():
                                        cur.execute(down_sql)
                                    logger.info("[DRY RUN] Verification successful. Rolling back changes.")
                                    raise psycopg.Rollback()
                        except psycopg.Rollback:
                            pass
                        except Exception as e:
                            logger.error(f"[DRY RUN] Rollback SQL Verification FAILED: {e}")
                            raise e
                    else:
                        logger.info("[DRY RUN] Skipping verification for non-transactional migration.")
                        
                    continue

                logger.info(f"Rolling back {mig_id} - {name}...")

                try:
                    # If no_transaction is set, we must execute outside of a transaction block
                    if no_transaction:
                        if (
                            conn.info.transaction_status
                            != psycopg.pq.TransactionStatus.IDLE
                        ):
                            logger.warning(
                                f"Connection in state {conn.info.transaction_status} before switching to autocommit. Rolling back."
                            )
                            conn.rollback()

                        conn.autocommit = True
                        with conn.cursor() as cur:
                            if down_sql.strip():
                                statements = sqlparse.split(down_sql)
                                for stmt in statements:
                                    if stmt.strip():
                                        cur.execute(stmt)

                            cur.execute(
                                "DELETE FROM _migrations WHERE id = %s", (mig_id,)
                            )

                            try:
                                user = os.getlogin()
                            except OSError:
                                user = "system"

                            cur.execute(
                                """
                                INSERT INTO _migrations_log (migration_id, name, action, performed_by, checksum)
                                VALUES (%s, %s, 'DOWN', %s, %s)
                            """,
                                (mig_id, name, user, checksum),
                            )
                        conn.autocommit = False  # Reset
                    else:
                        with conn.transaction():
                            with conn.cursor() as cur:
                                if down_sql.strip():
                                    cur.execute(down_sql)

                                cur.execute(
                                    "DELETE FROM _migrations WHERE id = %s", (mig_id,)
                                )

                                try:
                                    user = os.getlogin()
                                except OSError:
                                    user = "system"

                                cur.execute(
                                    """
                                    INSERT INTO _migrations_log (migration_id, name, action, performed_by, checksum)
                                    VALUES (%s, %s, 'DOWN', %s, %s)
                                """,
                                    (mig_id, name, user, checksum),
                                )
                    logger.info(f"Rolled back {mig_id}.")
                except Exception as e:
                    logger.error(f"Failed to rollback {mig_id}: {e}")
                    raise e
        execute_hook("post_rollback", env=env)
    finally:
        conn.close()


def apply_migrations(limit: Optional[int] = None, env: Optional[str] = None, dry_run: bool = False) -> None:
    execute_hook("pre_apply", env=env)
    conn = get_connection(env=env)
    lock_id = get_lock_id(env=env)
    try:
        with advisory_lock(conn, lock_id=lock_id):
            ensure_schema(conn)

            # Check for dirty state
            failed = check_failed_migrations(conn)
            if failed:
                logger.error("❌ Database is in a DIRTY STATE.")
                logger.error("The following migrations failed partially:")
                for fid, fname in failed:
                    logger.error(f"  - {fid} ({fname})")
                logger.error(
                    "Please manually fix the database state and then run 'mg fix <id>' (to mark as applied) or remove the entry from _migrations if you rolled back manually."
                )
                raise RuntimeError("Dirty database state.")

            applied_ids = get_applied_migrations(conn)
            all_migrations = get_migration_files()

            pending = [m for m in all_migrations if m[0] not in applied_ids]

            if not pending:
                logger.info("No pending migrations.")
                return

            if limit:
                pending = pending[:limit]

            logger.info(
                f"Found {len(pending)} pending migrations (applying {len(pending)})."
            )

            for mig_id, name, filepath in pending:
                try:
                    with open(filepath, "r", encoding="utf-8") as f:
                        content = f.read()
                except OSError as e:
                    logger.error(f"Error reading {filepath}: {e}")
                    raise

                up_sql, _, no_transaction = parse_migration_sql(content)
                checksum = calculate_checksum(content)

                if dry_run:
                    logger.info(f"[DRY RUN] Applying {mig_id} - {name}")
                    logger.info(f"[DRY RUN] SQL:\n{up_sql}")
                    
                    if not no_transaction:
                        try:
                            # Smart Dry Run: Execute inside a transaction that we explicitly rollback
                            with conn.transaction():
                                with conn.cursor() as cur:
                                    logger.info(f"[DRY RUN] Verifying SQL execution for {mig_id}...")
                                    if up_sql.strip():
                                        cur.execute(up_sql)
                                    logger.info("[DRY RUN] Verification successful. Rolling back changes.")
                                    raise psycopg.Rollback() # Force rollback
                        except psycopg.Rollback:
                            pass # Expected
                        except Exception as e:
                            logger.error(f"[DRY RUN] SQL Verification FAILED: {e}")
                            raise e
                    else:
                        logger.info("[DRY RUN] Skipping verification for non-transactional migration.")
                        
                    continue

                logger.info(f"Applying {mig_id} - {name}...")

                try:
                    if no_transaction:
                        # Ensure we are not in a transaction before switching autocommit
                        if (
                            conn.info.transaction_status
                            != psycopg.pq.TransactionStatus.IDLE
                        ):
                            logger.warning(
                                f"Connection in state {conn.info.transaction_status} before switching to autocommit. Rolling back."
                            )
                            conn.rollback()

                        conn.autocommit = True
                        with conn.cursor() as cur:
                            if up_sql.strip():
                                # Split statements for non-transactional execution
                                statements = sqlparse.split(up_sql)
                                for stmt in statements:
                                    if stmt.strip():
                                        try:
                                            cur.execute(stmt)
                                        except Exception as stmt_err:
                                            # Mark as failed in DB
                                            # We need to reconnect or use a separate connection/transaction if the error invalidated the state
                                            # But for non-transactional, the connection might be okay?
                                            # Actually, creating a new cursor for metadata update is safer.
                                            logger.error(
                                                f"Statement failed: {stmt_err}"
                                            )

                                            cur.execute(
                                                """
                                                INSERT INTO _migrations (id, name, checksum, status)
                                                VALUES (%s, %s, %s, 'failed')
                                                ON CONFLICT (id) DO UPDATE SET status = 'failed', applied_at = NOW()
                                            """,
                                                (mig_id, name, checksum),
                                            )

                                            raise stmt_err

                            cur.execute(
                                """
                                INSERT INTO _migrations (id, name, checksum, status)
                                VALUES (%s, %s, %s, 'applied')
                                ON CONFLICT (id) DO UPDATE SET status = 'applied', checksum = EXCLUDED.checksum, applied_at = NOW()
                            """,
                                (mig_id, name, checksum),
                            )

                            try:
                                user = os.getlogin()
                            except OSError:
                                user = "system"

                            cur.execute(
                                """
                                INSERT INTO _migrations_log (migration_id, name, action, performed_by, checksum)
                                VALUES (%s, %s, 'UP', %s, %s)
                            """,
                                (mig_id, name, user, checksum),
                            )
                        conn.autocommit = False  # Reset
                    else:
                        with conn.transaction():
                            with conn.cursor() as cur:
                                if up_sql.strip():
                                    cur.execute(up_sql)

                                cur.execute(
                                    """
                                    INSERT INTO _migrations (id, name, checksum)
                                    VALUES (%s, %s, %s)
                                """,
                                    (mig_id, name, checksum),
                                )

                                try:
                                    user = os.getlogin()
                                except OSError:
                                    user = "system"

                                cur.execute(
                                    """
                                    INSERT INTO _migrations_log (migration_id, name, action, performed_by, checksum)
                                    VALUES (%s, %s, 'UP', %s, %s)
                                """,
                                    (mig_id, name, user, checksum),
                                )
                    logger.info(f"Applied {mig_id}.")
                except Exception as e:
                    logger.error(f"Failed to apply {mig_id}: {e}")
                    raise e
        execute_hook("post_apply", env=env)
    finally:
        conn.close()


def get_migration_status(env: Optional[str] = None) -> List[Dict[str, str]]:
    conn = get_connection(env=env)
    try:
        ensure_schema(conn)

        # Get all entries from DB including failed
        with conn.cursor() as cur:
            cur.execute("SELECT id, status FROM _migrations")
            db_status = {row[0]: row[1] for row in cur.fetchall()}

        all_migrations = get_migration_files()  # [(id, name, path)]

        status_list = []
        for mig_id, name, _ in all_migrations:
            if mig_id in db_status:
                status = db_status[mig_id]
            else:
                status = "pending"

            status_list.append({"id": mig_id, "name": name, "status": status})
        return status_list
    finally:
        conn.close()


def get_head(env: Optional[str] = None) -> Optional[Dict[str, Any]]:
    conn = get_connection(env=env)
    try:
        ensure_schema(conn)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, name, applied_at FROM _migrations ORDER BY applied_at DESC, id DESC LIMIT 1"
            )
            row = cur.fetchone()
            if row:
                return {"id": row[0], "name": row[1], "applied_at": row[2]}
            return None
    finally:
        conn.close()