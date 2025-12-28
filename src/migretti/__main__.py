import argparse
import sys
import os
import re
from migretti.ulid import ULID
from typing import Optional
from migretti.config import CONFIG_FILENAME
from migretti.core import (
    apply_migrations,
    rollback_migrations,
    get_migration_status,
    get_head,
    verify_checksums,
)
from migretti.logging_setup import setup_logging, get_logger
from migretti.io_utils import atomic_write
from migretti.prompt_cmd import cmd_prompt
from migretti.seed import cmd_seed
from migretti.squash import cmd_squash
logger = get_logger()
