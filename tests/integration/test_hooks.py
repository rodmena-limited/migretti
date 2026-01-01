import pytest
import os
import shutil
import tempfile
import psycopg
from migretti.core import apply_migrations
from migretti import __main__ as main_mod
from migretti.logging_setup import setup_logging
TEST_DB_NAME = "migretti_test"
TEST_DB_URL = f"postgresql://postgres:postgres@localhost:5432/{TEST_DB_NAME}"
