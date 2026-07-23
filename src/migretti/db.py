import psycopg
from psycopg.conninfo import conninfo_to_dict
from contextlib import contextmanager
from typing import Optional, Generator, Any, Dict
from migretti.config import load_config
from migretti.logging_setup import get_logger

logger = get_logger()

DEFAULT_LOCK_ID = 894321
CONNECT_TIMEOUT_SECONDS = 10


def get_connection(env: Optional[str] = None) -> psycopg.Connection[Any]:
    """
    Open a connection in autocommit mode.

    Transactionality is managed explicitly by the callers (one transaction per
    migration) so that every migration is durable the moment it is reported as
    applied. Autocommit also guarantees that advisory locks and bookkeeping
    reads never open implicit transactions that would silently defer commits.
    """
    config = load_config(env=env)
    db_config = config.get("database", {})

    if not db_config:
        raise RuntimeError("No database configuration found.")

    try:
        if "conninfo" in db_config:
            params: Dict[str, Any] = dict(conninfo_to_dict(db_config["conninfo"]))
        else:
            params = dict(db_config)
    except psycopg.Error as e:
        raise RuntimeError(f"Invalid database configuration: {e}")

    params.setdefault("connect_timeout", CONNECT_TIMEOUT_SECONDS)
    params.setdefault("application_name", "migretti")
    params.pop("autocommit", None)

    try:
        return psycopg.connect(**params, autocommit=True)
    except (psycopg.Error, TypeError) as e:
        raise RuntimeError(f"Database connection failed: {e}")


def get_lock_id(env: Optional[str] = None) -> int:
    config = load_config(env=env)
    val = config.get("lock_id", DEFAULT_LOCK_ID)
    # bool is an int subclass; `lock_id: true` must not become lock id 1.
    if isinstance(val, bool) or not isinstance(val, int):
        logger.warning(
            f"Invalid lock_id {val!r} in configuration; using default {DEFAULT_LOCK_ID}."
        )
        return DEFAULT_LOCK_ID
    return val


@contextmanager
def advisory_lock(
    conn: psycopg.Connection[Any], lock_id: int = DEFAULT_LOCK_ID
) -> Generator[None, None, None]:
    """
    Acquire a session-level advisory lock on an autocommit connection.

    The caller commits its own work (each migration runs in its own explicit
    transaction), so nothing is pending when the lock is released. The unlock
    is guarded: any leftover transaction state is rolled back first, and an
    unlock failure is logged instead of masking the original error — the lock
    dies with the connection anyway.
    """
    acquired = False
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_try_advisory_lock(%s)", (lock_id,))
            row = cur.fetchone()
            acquired = bool(row and row[0])
            if not acquired:
                logger.info(
                    "Another migretti process holds the migration lock; waiting..."
                )
                cur.execute("SELECT pg_advisory_lock(%s)", (lock_id,))
                acquired = True
        yield
    finally:
        if acquired:
            try:
                if conn.info.transaction_status != psycopg.pq.TransactionStatus.IDLE:
                    conn.rollback()
                with conn.cursor() as cur:
                    cur.execute("SELECT pg_advisory_unlock(%s)", (lock_id,))
            except Exception:
                logger.warning(
                    f"Could not release advisory lock {lock_id}; it will be released "
                    "when the connection closes.",
                    exc_info=True,
                )


def tracking_tables_exist(conn: psycopg.Connection[Any]) -> bool:
    """True if the _migrations tracking table exists (respects search_path)."""
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass('_migrations') IS NOT NULL")
        row = cur.fetchone()
        return bool(row and row[0])


def ensure_schema(conn: psycopg.Connection[Any]) -> None:
    """
    Ensure _migrations and _migrations_log tables exist.

    This performs DDL, so it is only called from write paths (apply, rollback,
    fix, squash) while the advisory lock is held; read-only commands use
    tracking_tables_exist() instead.
    """
    with conn.transaction():
        with conn.cursor() as cur:
            # _migrations table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS _migrations (
                    id VARCHAR(26) PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    checksum VARCHAR(64),
                    status VARCHAR(20) DEFAULT 'applied' -- 'applied', 'failed'
                );
            """)

            # Add status column if it doesn't exist (migration for migretti itself!)
            cur.execute("""
                DO $$
                BEGIN
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='_migrations' AND column_name='status') THEN
                        ALTER TABLE _migrations ADD COLUMN status VARCHAR(20) DEFAULT 'applied';
                    END IF;
                END
                $$;
            """)

            # _migrations_log table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS _migrations_log (
                    id BIGSERIAL PRIMARY KEY,
                    migration_id VARCHAR(26) NOT NULL,
                    name VARCHAR(255) NOT NULL,
                    action VARCHAR(10) NOT NULL, -- 'UP', 'DOWN', 'FIX'
                    performed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    performed_by VARCHAR(255),
                    checksum VARCHAR(64)
                );
            """)
