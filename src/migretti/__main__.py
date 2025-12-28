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

def check_prod_protection(args: argparse.Namespace) -> None:
    """
    Check if running against production and ask for confirmation.
    Simple detection: if MG_ENV or --env is 'prod', 'production', 'live'.
    """
    env: Optional[str] = getattr(args, "env", None) or os.getenv("MG_ENV", "default")
    if env and env.lower() in ["prod", "production", "live"]:
        if not getattr(args, "dry_run", False) and not getattr(args, "yes", False):
            print(
                f"⚠️  WARNING: You are about to run this operation against the '{env}' environment!"
            )
            response = input("Are you sure you want to continue? (yes/no): ")
            if response.lower() != "yes":
                print("Operation cancelled.")
                sys.exit(0)
