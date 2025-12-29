import pytest
import psycopg
import sys
import os
import shutil
import tempfile
from migretti import __main__ as main_mod
from migretti.core import (
    apply_migrations,
    rollback_migrations,
    get_migration_status,
    verify_checksums,
)
from migretti.db import get_connection
from migretti.logging_setup import setup_logging
TEST_DB_NAME = "migretti_test"
TEST_DB_URL = f"postgresql://postgres:postgres@localhost:5432/{TEST_DB_NAME}"
ASSETS_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "test_assets", "migrations")
)
