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

def cmd_init(args: argparse.Namespace) -> None:
    """Initialize a new migration project."""
    if os.path.exists(CONFIG_FILENAME):
        logger.error(f"{CONFIG_FILENAME} already exists.")
        return

    # Create mg.yaml
    default_config = """database:
  host: localhost
  port: 5432
  user: postgres
  password: password
  dbname: my_database

envs:
  dev:
    database:
      host: localhost
      port: 5432
      user: postgres
      password: password
      dbname: my_app_dev
  prod:
    database:
      host: db.prod.example.com
      port: 5432
      user: dbuser
      password: securepassword
      dbname: my_app_prod
"""
    try:
        with atomic_write(CONFIG_FILENAME, exclusive=True) as f:
            f.write(default_config)
        print(f"Created {CONFIG_FILENAME}")
    except FileExistsError:
        logger.error(f"{CONFIG_FILENAME} already exists.")
    except Exception as e:
        logger.error(f"Failed to create config: {e}")

    # Create migrations directory
    if not os.path.exists("migrations"):
        os.makedirs("migrations")
        print("Created migrations/ directory")
    else:
        print("migrations/ directory already exists")
