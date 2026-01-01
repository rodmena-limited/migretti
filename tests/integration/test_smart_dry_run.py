import pytest
import psycopg
import os
import shutil
import tempfile
from migretti import __main__ as main_mod
from migretti.logging_setup import setup_logging
TEST_DB_NAME = "migretti_test"
TEST_DB_URL = f"postgresql://postgres:postgres@localhost:5432/{TEST_DB_NAME}"
