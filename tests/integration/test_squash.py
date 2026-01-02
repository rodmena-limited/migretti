import pytest
import os
import glob
from migretti.__main__ import cmd_squash, cmd_create
from migretti.logging_setup import setup_logging
from migretti import __main__ as main_mod
import psycopg
import tempfile
TEST_DB_NAME = "migretti_test"
TEST_DB_URL = f"postgresql://postgres:postgres@localhost:5432/{TEST_DB_NAME}"
