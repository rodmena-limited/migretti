import psycopg
from contextlib import contextmanager
from typing import Optional, Generator, Any
from migretti.config import load_config


def get_connection(env: Optional[str] = None) -> psycopg.Connection[Any]:
    config = load_config(env=env)
    db_config = config.get("database", {})

    if not db_config:
        raise RuntimeError("No database configuration found.")

    try:
        # If conninfo is present (from MG_DATABASE_URL), use it.
        if "conninfo" in db_config:
            conn = psycopg.connect(db_config["conninfo"], autocommit=False)
        else:
            conn = psycopg.connect(
                **db_config, autocommit=False
            )  # Default to transaction mode
        return conn
    except psycopg.Error as e:
        raise RuntimeError(f"Database connection failed: {e}")

def get_lock_id(env: Optional[str] = None) -> int:
    config = load_config(env=env)
    # Default: 894321
    val = config.get("lock_id", 894321)
    if isinstance(val, int):
        return val
    return 894321

@contextmanager
def advisory_lock(conn: psycopg.Connection[Any], lock_id: int = 894321) -> Generator[None, None, None]: 
    """
    Acquire a transaction-level advisory lock.
    """
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_lock(%s)", (lock_id,))
        conn.commit()  # Ensure we are not in a transaction
        yield
    finally:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_unlock(%s)", (lock_id,))
        conn.commit()  # Ensure unlock is committed (though unlock is immediate usually)


def ensure_schema(conn: psycopg.Connection[Any]) -> None:
    """Ensure _migrations and _migrations_log tables exist."""
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
                action VARCHAR(10) NOT NULL, -- 'UP', 'DOWN'
                performed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                performed_by VARCHAR(255),
                checksum VARCHAR(64)
            );
        """)
        conn.commit()