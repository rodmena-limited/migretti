import pytest
import os
import psycopg
from migretti.__main__ import cmd_seed
from migretti.logging_setup import setup_logging
import tempfile
from migretti import __main__ as main_mod
TEST_DB_NAME = "migretti_test"
TEST_DB_URL = f"postgresql://postgres:postgres@localhost:5432/{TEST_DB_NAME}"
