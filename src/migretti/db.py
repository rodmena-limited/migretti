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
