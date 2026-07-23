import os
import glob
import hashlib
import re
import psycopg
import sqlparse
from typing import Tuple, List, Set, Optional, Dict, Any
from migretti.db import (
    get_connection,
    ensure_schema,
    advisory_lock,
    get_lock_id,
    tracking_tables_exist,
)
from migretti.logging_setup import get_logger
from migretti.hooks import execute_hook

logger = get_logger()

# _migrations.id is VARCHAR(26); validating here means a bad id fails before
# any SQL runs instead of after it.
MIGRATION_ID_RE = re.compile(r"^[A-Za-z0-9]{1,26}$")


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
    """
    Returns list of (id, name, filepath) sorted by id.

    Ids sort as strings; `mg create` produces fixed-width ULIDs which sort
    chronologically. Malformed filenames are skipped with a warning; invalid,
    duplicate or oversized ids are rejected outright rather than silently
    misordered or failed mid-apply.
    """
    if not os.path.exists("migrations"):
        return []

    files = glob.glob(os.path.join("migrations", "*.sql"))
    migrations = []
    seen_ids: Dict[str, str] = {}
    for f in sorted(files):
        basename = os.path.basename(f)
        # format: <id>_<slug>.sql
        parts = basename.split("_", 1)
        if len(parts) < 2 or not parts[0]:
            logger.warning(
                f"Skipping {f}: migration filenames must look like '<id>_<name>.sql'."
            )
            continue
        mig_id = parts[0]
        name = parts[1][:-4] if parts[1].endswith(".sql") else parts[1]
        if not name:
            logger.warning(
                f"Skipping {f}: migration filenames must look like '<id>_<name>.sql'."
            )
            continue
        if not MIGRATION_ID_RE.match(mig_id):
            raise ValueError(
                f"Invalid migration id '{mig_id}' in {f}: ids must be 1-26 "
                "alphanumeric characters."
            )
        if mig_id in seen_ids:
            raise ValueError(
                f"Duplicate migration id '{mig_id}': {seen_ids[mig_id]} and {f}."
            )
        seen_ids[mig_id] = f
        migrations.append((mig_id, name, f))

    id_lengths = {len(mig_id) for mig_id in seen_ids}
    if len(id_lengths) > 1:
        logger.warning(
            "Migration ids have mixed lengths; ids sort as strings, so e.g. '10' "
            "applies before '2'. Prefer fixed-width ids (`mg create` ULIDs)."
        )

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


def _current_user() -> str:
    try:
        return os.getlogin()
    except OSError:
        return "system"


def _read_migration(filepath: str) -> str:
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            return f.read()
    except OSError as e:
        logger.error(f"Error reading {filepath}: {e}")
        raise


def verify_checksums(env: Optional[str] = None) -> bool:
    """
    Verifies that applied migrations match files on disk — in both directions:
    a changed file and a missing file are both failures.
    """
    conn = get_connection(env=env)
    try:
        if not tracking_tables_exist(conn):
            logger.info("No migration tracking tables found; nothing applied yet.")
            return True

        applied = get_applied_migrations_details(conn)
        files_by_id = {m[0]: m[2] for m in get_migration_files()}

        issues = []

        for mig_id, name, stored_checksum in applied:
            filepath = files_by_id.get(mig_id)
            if filepath is None:
                issues.append(
                    f"Applied migration {mig_id} ({name}) has no file on disk."
                )
                continue
            try:
                with open(filepath, "r", encoding="utf-8") as f:
                    content = f.read()
            except OSError as e:
                issues.append(f"Error reading {filepath}: {e}")
                continue

            if calculate_checksum(content) != stored_checksum:
                issues.append(f"Checksum mismatch for {mig_id} ({name})")

        if issues:
            for issue in issues:
                logger.error(issue)
            return False

        logger.info("All applied migrations match files on disk.")
        return True
    finally:
        conn.close()


def _dry_run_verify(
    conn: psycopg.Connection[Any],
    mig_id: str,
    sql: str,
    no_transaction: bool,
    action: str,
) -> None:
    """
    Execute SQL in its own transaction and roll it back.

    This catches invalid SQL early, but it is not side-effect free: sequences
    advance, and DDL takes its usual locks while each statement runs.
    """
    if no_transaction:
        logger.info("[DRY RUN] Skipping verification for non-transactional migration.")
        return
    if not sql.strip():
        return
    try:
        with conn.transaction():
            with conn.cursor() as cur:
                logger.info(
                    f"[DRY RUN] Verifying {action} SQL for {mig_id} "
                    "(executed, then rolled back)..."
                )
                cur.execute(sql)
                raise psycopg.Rollback()  # Force rollback (swallowed by transaction())
    except Exception as e:
        logger.error(f"[DRY RUN] {action} SQL verification FAILED: {e}")
        raise
    logger.info("[DRY RUN] Verification successful. Changes rolled back.")


def rollback_migrations(
    steps: int = 1,
    env: Optional[str] = None,
    dry_run: bool = False,
    allow_missing_down: bool = False,
) -> None:
    if steps < 1:
        raise ValueError(f"steps must be a positive integer, got {steps}.")
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
            logger.info(f"Rolling back {len(to_rollback)} migration(s):")
            for mig_id, name, _ in to_rollback:
                logger.info(f"  - {mig_id} ({name})")

            id_to_path = {m[0]: m[2] for m in get_migration_files()}

            # Validate the whole set up front so a run never stops halfway
            # through with a surprise.
            plans = []
            for mig_id, name, _ in to_rollback:
                filepath = id_to_path.get(mig_id)
                if not filepath:
                    raise RuntimeError(
                        f"File for migration {mig_id} ({name}) not found; cannot roll back."
                    )
                content = _read_migration(filepath)
                _, down_sql, no_transaction = parse_migration_sql(content, filepath)
                if not down_sql.strip() and not allow_missing_down:
                    raise RuntimeError(
                        f"Migration {mig_id} ({name}) has no '-- migrate: down' SQL. "
                        "Rolling it back would delete its history while leaving its "
                        "schema changes in place. Pass --allow-missing-down to do "
                        "this anyway."
                    )
                plans.append(
                    (mig_id, name, down_sql, no_transaction, calculate_checksum(content))
                )

            for mig_id, name, down_sql, no_transaction, checksum in plans:
                if dry_run:
                    logger.info(f"[DRY RUN] Rolling back {mig_id} - {name}")
                    logger.info(f"[DRY RUN] SQL:\n{down_sql}")
                    _dry_run_verify(conn, mig_id, down_sql, no_transaction, "rollback")
                    continue

                logger.info(f"Rolling back {mig_id} - {name}...")

                try:
                    if no_transaction:
                        # Connection is autocommit: statements run (and commit)
                        # one at a time, as required by e.g. DROP INDEX CONCURRENTLY.
                        with conn.cursor() as cur:
                            if down_sql.strip():
                                for stmt in sqlparse.split(down_sql):
                                    if stmt.strip():
                                        cur.execute(stmt)
                        with conn.transaction():
                            with conn.cursor() as cur:
                                cur.execute(
                                    "DELETE FROM _migrations WHERE id = %s", (mig_id,)
                                )
                                cur.execute(
                                    """
                                    INSERT INTO _migrations_log (migration_id, name, action, performed_by, checksum)
                                    VALUES (%s, %s, 'DOWN', %s, %s)
                                """,
                                    (mig_id, name, _current_user(), checksum),
                                )
                    else:
                        # One explicit transaction per migration: the down SQL
                        # and its bookkeeping commit (or fail) atomically,
                        # before the next migration starts.
                        with conn.transaction():
                            with conn.cursor() as cur:
                                if down_sql.strip():
                                    cur.execute(down_sql)
                                cur.execute(
                                    "DELETE FROM _migrations WHERE id = %s", (mig_id,)
                                )
                                cur.execute(
                                    """
                                    INSERT INTO _migrations_log (migration_id, name, action, performed_by, checksum)
                                    VALUES (%s, %s, 'DOWN', %s, %s)
                                """,
                                    (mig_id, name, _current_user(), checksum),
                                )
                    logger.info(f"Rolled back {mig_id}.")
                except Exception as e:
                    logger.error(f"Failed to rollback {mig_id}: {e}")
                    raise
        execute_hook("post_rollback", env=env)
    finally:
        conn.close()


def apply_migrations(
    limit: Optional[int] = None,
    env: Optional[str] = None,
    dry_run: bool = False,
    allow_out_of_order: bool = False,
) -> None:
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
                    "Fix the database state manually, then run "
                    "'mg fix <id> --applied' (you completed the change by hand) or "
                    "'mg fix <id> --remove' (you undid the partial change by hand)."
                )
                raise RuntimeError("Dirty database state.")

            applied_ids = get_applied_migrations(conn)
            all_migrations = get_migration_files()

            pending = [m for m in all_migrations if m[0] not in applied_ids]

            if not pending:
                logger.info("No pending migrations.")
                return

            if applied_ids:
                newest_applied = max(applied_ids)
                out_of_order = [m for m in pending if m[0] < newest_applied]
                if out_of_order and not allow_out_of_order:
                    for mig_id, name, _ in out_of_order:
                        logger.error(
                            f"Out-of-order migration: {mig_id} ({name}) sorts before "
                            f"already-applied {newest_applied}."
                        )
                    raise RuntimeError(
                        "Out-of-order migrations detected (this usually happens after "
                        "merging a branch). Review them, then re-run with "
                        "--allow-out-of-order to apply anyway."
                    )

            total_pending = len(pending)
            if limit is not None:
                pending = pending[:limit]

            logger.info(
                f"Found {total_pending} pending migration(s); applying {len(pending)}."
            )
            if dry_run:
                logger.info(
                    "[DRY RUN] Note: SQL is executed against the database and rolled "
                    "back; side effects such as sequence advancement persist, and DDL "
                    "holds its usual locks while each statement runs."
                )

            for mig_id, name, filepath in pending:
                content = _read_migration(filepath)
                up_sql, _, no_transaction = parse_migration_sql(content, filepath)
                checksum = calculate_checksum(content)

                if dry_run:
                    logger.info(f"[DRY RUN] Applying {mig_id} - {name}")
                    logger.info(f"[DRY RUN] SQL:\n{up_sql}")
                    _dry_run_verify(conn, mig_id, up_sql, no_transaction, "apply")
                    continue

                logger.info(f"Applying {mig_id} - {name}...")

                try:
                    if no_transaction:
                        # Connection is autocommit: each statement commits on
                        # its own, as required by e.g. CREATE INDEX CONCURRENTLY.
                        with conn.cursor() as cur:
                            if up_sql.strip():
                                for stmt in sqlparse.split(up_sql):
                                    if not stmt.strip():
                                        continue
                                    try:
                                        cur.execute(stmt)
                                    except Exception as stmt_err:
                                        # Earlier statements already committed:
                                        # record the partial application as a
                                        # dirty state before propagating.
                                        logger.error(f"Statement failed: {stmt_err}")
                                        cur.execute(
                                            """
                                            INSERT INTO _migrations (id, name, checksum, status)
                                            VALUES (%s, %s, %s, 'failed')
                                            ON CONFLICT (id) DO UPDATE SET status = 'failed', applied_at = NOW()
                                        """,
                                            (mig_id, name, checksum),
                                        )
                                        raise
                        with conn.transaction():
                            with conn.cursor() as cur:
                                cur.execute(
                                    """
                                    INSERT INTO _migrations (id, name, checksum, status)
                                    VALUES (%s, %s, %s, 'applied')
                                    ON CONFLICT (id) DO UPDATE SET status = 'applied', checksum = EXCLUDED.checksum, applied_at = NOW()
                                """,
                                    (mig_id, name, checksum),
                                )
                                cur.execute(
                                    """
                                    INSERT INTO _migrations_log (migration_id, name, action, performed_by, checksum)
                                    VALUES (%s, %s, 'UP', %s, %s)
                                """,
                                    (mig_id, name, _current_user(), checksum),
                                )
                    else:
                        # One explicit transaction per migration: the SQL and
                        # its bookkeeping are durable before "Applied" is
                        # logged and before the next migration starts.
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
                                cur.execute(
                                    """
                                    INSERT INTO _migrations_log (migration_id, name, action, performed_by, checksum)
                                    VALUES (%s, %s, 'UP', %s, %s)
                                """,
                                    (mig_id, name, _current_user(), checksum),
                                )
                    logger.info(f"Applied {mig_id}.")
                except Exception as e:
                    logger.error(f"Failed to apply {mig_id}: {e}")
                    raise
        execute_hook("post_apply", env=env)
    finally:
        conn.close()


def fix_migration(
    mig_id: str, mark_applied: bool, env: Optional[str] = None
) -> None:
    """
    Repair the recorded state of a migration after a partial (dirty) failure.

    mark_applied=True: the operator completed the change by hand — mark the
    row as applied. mark_applied=False: the operator undid the partial change
    by hand — remove the row so the migration is pending again.
    """
    conn = get_connection(env=env)
    lock_id = get_lock_id(env=env)
    try:
        with advisory_lock(conn, lock_id=lock_id):
            ensure_schema(conn)

            checksum: Optional[str] = None
            files_by_id = {m[0]: m[2] for m in get_migration_files()}
            filepath = files_by_id.get(mig_id)
            if filepath:
                try:
                    with open(filepath, "r", encoding="utf-8") as f:
                        checksum = calculate_checksum(f.read())
                except OSError:
                    pass

            with conn.transaction():
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT name, status FROM _migrations WHERE id = %s", (mig_id,)
                    )
                    row = cur.fetchone()
                    if row is None:
                        raise RuntimeError(
                            f"No record of migration '{mig_id}' in _migrations; nothing to fix."
                        )
                    name, status = row

                    if mark_applied:
                        if checksum is not None:
                            cur.execute(
                                "UPDATE _migrations SET status = 'applied', checksum = %s, applied_at = NOW() WHERE id = %s",
                                (checksum, mig_id),
                            )
                        else:
                            cur.execute(
                                "UPDATE _migrations SET status = 'applied', applied_at = NOW() WHERE id = %s",
                                (mig_id,),
                            )
                        action_desc = "marked as applied"
                    else:
                        cur.execute(
                            "DELETE FROM _migrations WHERE id = %s", (mig_id,)
                        )
                        action_desc = "removed from history (pending again)"

                    cur.execute(
                        """
                        INSERT INTO _migrations_log (migration_id, name, action, performed_by, checksum)
                        VALUES (%s, %s, 'FIX', %s, %s)
                    """,
                        (mig_id, name, _current_user(), checksum),
                    )
            logger.info(
                f"Migration {mig_id} ({name}) {action_desc}. Previous status: {status}."
            )
    finally:
        conn.close()


def get_migration_status(env: Optional[str] = None) -> List[Dict[str, str]]:
    """Read-only: never creates or alters tracking tables."""
    conn = get_connection(env=env)
    try:
        db_status: Dict[str, str] = {}
        if tracking_tables_exist(conn):
            with conn.cursor() as cur:
                cur.execute("SELECT id, status FROM _migrations")
                db_status = {row[0]: row[1] for row in cur.fetchall()}

        all_migrations = get_migration_files()  # [(id, name, path)]

        status_list = []
        for mig_id, name, _ in all_migrations:
            status = db_status.get(mig_id, "pending")
            status_list.append({"id": mig_id, "name": name, "status": status})
        return status_list
    finally:
        conn.close()


def get_head(env: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Read-only: reports the newest successfully applied migration."""
    conn = get_connection(env=env)
    try:
        if not tracking_tables_exist(conn):
            return None
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, name, applied_at FROM _migrations WHERE status = 'applied' "
                "ORDER BY applied_at DESC, id DESC LIMIT 1"
            )
            row = cur.fetchone()
            if row:
                return {"id": row[0], "name": row[1], "applied_at": row[2]}
            return None
    finally:
        conn.close()
