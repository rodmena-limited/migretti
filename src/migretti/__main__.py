import argparse
import sys
import os
import re
import ulid
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


def cmd_create(args: argparse.Namespace) -> None:
    """Create a new migration script."""
    name = args.name
    # Sanitize name
    slug = re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")

    migration_id = str(ulid.ULID())
    filename = f"{migration_id}_{slug}.sql"
    filepath = os.path.join("migrations", filename)

    if not os.path.exists("migrations"):
        logger.error("migrations directory not found. Run 'mg init' first.")
        sys.exit(1)

    template = """-- migration: {name}
-- id: {id}

-- migrate: up


-- migrate: down

"""
    try:
        with atomic_write(filepath, exclusive=True) as f:
            f.write(template.format(name=name, id=migration_id))
        print(f"Created {filepath}")
    except Exception as e:
        logger.error(f"Failed to create migration file: {e}")
        sys.exit(1)


def cmd_apply(args: argparse.Namespace) -> None:
    """Apply all pending migrations."""
    check_prod_protection(args)
    apply_migrations(env=args.env, dry_run=args.dry_run)


def cmd_rollback(args: argparse.Namespace) -> None:
    """Rollback migrations."""
    check_prod_protection(args)
    rollback_migrations(steps=args.steps, env=args.env, dry_run=args.dry_run)


def cmd_up(args: argparse.Namespace) -> None:
    """Apply the next pending migration."""
    check_prod_protection(args)
    apply_migrations(limit=1, env=args.env, dry_run=args.dry_run)


def cmd_down(args: argparse.Namespace) -> None:
    """Rollback the last applied migration."""
    check_prod_protection(args)
    rollback_migrations(steps=1, env=args.env, dry_run=args.dry_run)


def cmd_status(args: argparse.Namespace) -> None:
    """Show migration status."""
    try:
        status_list = get_migration_status(env=args.env)
        applied = sum(1 for s in status_list if s["status"] == "applied")
        pending = sum(1 for s in status_list if s["status"] == "pending")
        print(f"Total migrations: {len(status_list)}")
        print(f"Applied: {applied}")
        print(f"Pending: {pending}")
    except Exception as e:
        logger.error(f"Error getting status: {e}")
        sys.exit(1)


def cmd_list(args: argparse.Namespace) -> None:
    """List all migrations."""
    try:
        status_list = get_migration_status(env=args.env)
        if not status_list:
            print("No migrations found.")
            return

        print(f"{'ID':<26} | {'Status':<10} | {'Name'}")
        print("-" * 60)
        for item in status_list:
            print(f"{item['id']:<26} | {item['status']:<10} | {item['name']}")
    except Exception as e:
        logger.error(f"Error listing migrations: {e}")
        sys.exit(1)


def cmd_head(args: argparse.Namespace) -> None:
    """Show current schema version."""
    try:
        head = get_head(env=args.env)
        if head:
            print(f"Current Head: {head['id']}")
            print(f"Name: {head['name']}")
            print(f"Applied At: {head['applied_at']}")
        else:
            print("No migrations applied.")
    except Exception as e:
        logger.error(f"Error getting head: {e}")
        sys.exit(1)


def cmd_verify(args: argparse.Namespace) -> None:
    """Verify applied migrations checksums."""
    try:
        if verify_checksums(env=args.env):
            print("Verification Successful: All applied migrations match.")
        else:
            print("Verification Failed: Checksum mismatches found. Check logs.")
            sys.exit(1)
    except Exception as e:
        logger.error(f"Error verifying checksums: {e}")
        sys.exit(1)


def main() -> None:
    parser = argparse.ArgumentParser(description="migretti - Database Migration Tool")

    # Global arguments
    parser.add_argument("--env", help="Environment profile to use (e.g. dev, prod)")
    parser.add_argument(
        "--json-log", action="store_true", help="Output logs in JSON format"
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose logging")

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # init
    parser_init = subparsers.add_parser(
        "init", help="Initialize a new migration project"
    )
    parser_init.set_defaults(func=cmd_init)

    # create
    parser_create = subparsers.add_parser(
        "create", help="Create a new migration script"
    )
    parser_create.add_argument("name", help="Name of the migration")
    parser_create.set_defaults(func=cmd_create)

    # apply
    parser_apply = subparsers.add_parser("apply", help="Apply all pending migrations")
    parser_apply.add_argument(
        "--dry-run", action="store_true", help="Show SQL without executing"
    )
    parser_apply.add_argument(
        "--yes", "-y", action="store_true", help="Skip confirmation prompts"
    )
    parser_apply.set_defaults(func=cmd_apply)

    # rollback
    parser_rollback = subparsers.add_parser("rollback", help="Rollback migrations")
    parser_rollback.add_argument(
        "steps",
        type=int,
        nargs="?",
        default=1,
        help="Number of steps to rollback (default: 1)",
    )
    parser_rollback.add_argument(
        "--dry-run", action="store_true", help="Show SQL without executing"
    )
    parser_rollback.add_argument(
        "--yes", "-y", action="store_true", help="Skip confirmation prompts"
    )
    parser_rollback.set_defaults(func=cmd_rollback)

    # status
    parser_status = subparsers.add_parser("status", help="Show migration status")
    parser_status.set_defaults(func=cmd_status)

    # list
    parser_list = subparsers.add_parser("list", help="List all migrations")
    parser_list.set_defaults(func=cmd_list)

    # up
    parser_up = subparsers.add_parser("up", help="Apply the next pending migration")
    parser_up.add_argument(
        "--dry-run", action="store_true", help="Show SQL without executing"
    )
    parser_up.add_argument(
        "--yes", "-y", action="store_true", help="Skip confirmation prompts"
    )
    parser_up.set_defaults(func=cmd_up)

    # down
    parser_down = subparsers.add_parser(
        "down", help="Rollback the last applied migration"
    )
    parser_down.add_argument(
        "--dry-run", action="store_true", help="Show SQL without executing"
    )
    parser_down.add_argument(
        "--yes", "-y", action="store_true", help="Skip confirmation prompts"
    )
    parser_down.set_defaults(func=cmd_down)

    # head
    parser_head = subparsers.add_parser("head", help="Show current schema version")
    parser_head.set_defaults(func=cmd_head)

    # verify
    parser_verify = subparsers.add_parser(
        "verify", help="Verify applied migration checksums"
    )
    parser_verify.set_defaults(func=cmd_verify)

    # prompt
    parser_prompt = subparsers.add_parser("prompt", help="Show instructions for AI agents")
    parser_prompt.set_defaults(func=cmd_prompt)

    # seed
    parser_seed = subparsers.add_parser("seed", help="Manage data seeding")
    seed_subparsers = parser_seed.add_subparsers(dest="seed_command")
    
    # seed run (default)
    parser_seed.set_defaults(func=cmd_seed)
    
    # seed create
    seed_create = seed_subparsers.add_parser("create", help="Create a new seed file")
    seed_create.add_argument("name", help="Name of the seed script")
    seed_create.set_defaults(func=cmd_seed)

    # squash
    parser_squash = subparsers.add_parser("squash", help="Squash pending migrations")
    parser_squash.add_argument("name", help="Name of the new squashed migration")
    parser_squash.set_defaults(func=cmd_squash)

    args = parser.parse_args()

    # Setup logging globally
    setup_logging(json_format=args.json_log, verbose=args.verbose)

    if hasattr(args, "func"):
        try:
            args.func(args)
        except Exception as e:
            logger.critical(f"Unhandled error: {e}", exc_info=True)
            sys.exit(1)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()