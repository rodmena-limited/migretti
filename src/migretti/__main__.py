import argparse
import sys
import os
import re
from migretti.ulid import ULID
from typing import Any
from migretti.config import CONFIG_FILENAME
from migretti.core import (
    apply_migrations,
    rollback_migrations,
    fix_migration,
    get_migration_status,
    get_head,
    verify_checksums,
)
from migretti.logging_setup import setup_logging, get_logger
from migretti.io_utils import atomic_write
from migretti.prompt_cmd import cmd_prompt
from migretti.safety import check_prod_protection, confirm_or_abort
from migretti.seed import cmd_seed
from migretti.squash import cmd_squash

logger = get_logger()


def positive_int(value: str) -> int:
    try:
        ivalue = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError(f"'{value}' is not an integer")
    if ivalue < 1:
        raise argparse.ArgumentTypeError("steps must be a positive integer (>= 1)")
    return ivalue


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

    migration_id = str(ULID())
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
    apply_migrations(
        env=args.env,
        dry_run=args.dry_run,
        allow_out_of_order=getattr(args, "allow_out_of_order", False),
    )


def cmd_rollback(args: argparse.Namespace) -> None:
    """Rollback migrations."""
    check_prod_protection(args)
    if args.steps > 1 and not args.dry_run:
        confirm_or_abort(
            f"Roll back up to {args.steps} migrations?",
            assume_yes=getattr(args, "yes", False),
        )
    rollback_migrations(
        steps=args.steps,
        env=args.env,
        dry_run=args.dry_run,
        allow_missing_down=getattr(args, "allow_missing_down", False),
    )


def cmd_up(args: argparse.Namespace) -> None:
    """Apply the next pending migration."""
    check_prod_protection(args)
    apply_migrations(
        limit=1,
        env=args.env,
        dry_run=args.dry_run,
        allow_out_of_order=getattr(args, "allow_out_of_order", False),
    )


def cmd_down(args: argparse.Namespace) -> None:
    """Rollback the last applied migration."""
    check_prod_protection(args)
    rollback_migrations(
        steps=1,
        env=args.env,
        dry_run=args.dry_run,
        allow_missing_down=getattr(args, "allow_missing_down", False),
    )


def cmd_fix(args: argparse.Namespace) -> None:
    """Repair the recorded state of a migration after a partial failure."""
    check_prod_protection(args)
    fix_migration(args.id, mark_applied=args.applied, env=args.env)


def cmd_status(args: argparse.Namespace) -> None:
    """Show migration status."""
    try:
        status_list = get_migration_status(env=args.env)
        applied = sum(1 for s in status_list if s["status"] == "applied")
        pending = sum(1 for s in status_list if s["status"] == "pending")
        failed = sum(1 for s in status_list if s["status"] == "failed")
        print(f"Total migrations: {len(status_list)}")
        print(f"Applied: {applied}")
        print(f"Pending: {pending}")
        if failed:
            print(f"Failed: {failed}")
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


def _add_shared_options(parser: argparse.ArgumentParser, for_subcommand: bool) -> None:
    """
    Register --env/--json-log/--verbose.

    They live on the root parser (so `mg --env prod apply` works) and on every
    subparser (so the documented `mg apply --env prod` works too). Subparser
    copies default to SUPPRESS so an absent flag never overwrites a value
    parsed by the root parser.
    """
    default: Any = argparse.SUPPRESS if for_subcommand else None
    flag_default: Any = argparse.SUPPRESS if for_subcommand else False
    parser.add_argument(
        "--env", default=default, help="Environment profile to use (e.g. dev, prod)"
    )
    parser.add_argument(
        "--json-log",
        action="store_true",
        default=flag_default,
        help="Output logs in JSON format",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        default=flag_default,
        help="Verbose logging",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="migretti - Database Migration Tool")
    _add_shared_options(parser, for_subcommand=False)

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    def add_command(name: str, help_text: str) -> argparse.ArgumentParser:
        sub = subparsers.add_parser(name, help=help_text)
        _add_shared_options(sub, for_subcommand=True)
        return sub

    # init
    parser_init = add_command("init", "Initialize a new migration project")
    parser_init.set_defaults(func=cmd_init)

    # create
    parser_create = add_command("create", "Create a new migration script")
    parser_create.add_argument("name", help="Name of the migration")
    parser_create.set_defaults(func=cmd_create)

    # apply
    parser_apply = add_command("apply", "Apply all pending migrations")
    parser_apply.add_argument(
        "--dry-run",
        action="store_true",
        help="Execute SQL inside a rolled-back transaction instead of applying",
    )
    parser_apply.add_argument(
        "--yes", "-y", action="store_true", help="Skip confirmation prompts"
    )
    parser_apply.add_argument(
        "--allow-out-of-order",
        action="store_true",
        help="Apply pending migrations that sort before already-applied ones",
    )
    parser_apply.set_defaults(func=cmd_apply)

    # rollback
    parser_rollback = add_command("rollback", "Rollback migrations")
    parser_rollback.add_argument(
        "steps",
        type=positive_int,
        nargs="?",
        default=1,
        help="Number of steps to rollback (default: 1)",
    )
    parser_rollback.add_argument(
        "--dry-run",
        action="store_true",
        help="Execute rollback SQL inside a rolled-back transaction instead of applying",
    )
    parser_rollback.add_argument(
        "--yes", "-y", action="store_true", help="Skip confirmation prompts"
    )
    parser_rollback.add_argument(
        "--allow-missing-down",
        action="store_true",
        help="Allow rolling back migrations that have no down SQL (removes history only)",
    )
    parser_rollback.set_defaults(func=cmd_rollback)

    # status
    parser_status = add_command("status", "Show migration status")
    parser_status.set_defaults(func=cmd_status)

    # list
    parser_list = add_command("list", "List all migrations")
    parser_list.set_defaults(func=cmd_list)

    # up
    parser_up = add_command("up", "Apply the next pending migration")
    parser_up.add_argument(
        "--dry-run",
        action="store_true",
        help="Execute SQL inside a rolled-back transaction instead of applying",
    )
    parser_up.add_argument(
        "--yes", "-y", action="store_true", help="Skip confirmation prompts"
    )
    parser_up.add_argument(
        "--allow-out-of-order",
        action="store_true",
        help="Apply pending migrations that sort before already-applied ones",
    )
    parser_up.set_defaults(func=cmd_up)

    # down
    parser_down = add_command("down", "Rollback the last applied migration")
    parser_down.add_argument(
        "--dry-run",
        action="store_true",
        help="Execute rollback SQL inside a rolled-back transaction instead of applying",
    )
    parser_down.add_argument(
        "--yes", "-y", action="store_true", help="Skip confirmation prompts"
    )
    parser_down.add_argument(
        "--allow-missing-down",
        action="store_true",
        help="Allow rolling back migrations that have no down SQL (removes history only)",
    )
    parser_down.set_defaults(func=cmd_down)

    # fix
    parser_fix = add_command(
        "fix", "Repair the recorded state of a migration after a partial failure"
    )
    parser_fix.add_argument("id", help="Migration id to fix")
    fix_group = parser_fix.add_mutually_exclusive_group(required=True)
    fix_group.add_argument(
        "--applied",
        action="store_true",
        help="Mark as applied (you completed the change by hand)",
    )
    fix_group.add_argument(
        "--remove",
        action="store_true",
        help="Remove from history so it is pending again (you undid the change by hand)",
    )
    parser_fix.add_argument(
        "--yes", "-y", action="store_true", help="Skip confirmation prompts"
    )
    parser_fix.set_defaults(func=cmd_fix)

    # head
    parser_head = add_command("head", "Show current schema version")
    parser_head.set_defaults(func=cmd_head)

    # verify
    parser_verify = add_command("verify", "Verify applied migration checksums")
    parser_verify.set_defaults(func=cmd_verify)

    # prompt
    parser_prompt = add_command("prompt", "Show instructions for AI agents")
    parser_prompt.set_defaults(func=cmd_prompt)

    # seed
    parser_seed = add_command("seed", "Manage data seeding")
    parser_seed.add_argument(
        "--yes", "-y", action="store_true", help="Skip confirmation prompts"
    )
    seed_subparsers = parser_seed.add_subparsers(dest="seed_command")

    # seed run (default)
    parser_seed.set_defaults(func=cmd_seed)

    # seed create
    seed_create = seed_subparsers.add_parser("create", help="Create a new seed file")
    seed_create.add_argument("name", help="Name of the seed script")
    seed_create.set_defaults(func=cmd_seed)

    # squash
    parser_squash = add_command("squash", "Squash pending migrations")
    parser_squash.add_argument("name", help="Name of the new squashed migration")
    parser_squash.add_argument(
        "--dry-run", action="store_true", help="Preview squash without making changes"
    )
    parser_squash.set_defaults(func=cmd_squash)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    # Setup logging globally
    setup_logging(
        json_format=getattr(args, "json_log", False),
        verbose=getattr(args, "verbose", False),
    )

    if hasattr(args, "func"):
        try:
            args.func(args)
        except KeyboardInterrupt:
            print("Interrupted.")
            sys.exit(130)
        except (RuntimeError, ValueError) as e:
            # Expected operational failures: show the message, keep the
            # traceback for --verbose.
            logger.error(str(e))
            logger.debug("Details:", exc_info=True)
            sys.exit(1)
        except Exception as e:
            logger.critical(f"Unhandled error: {e}", exc_info=True)
            sys.exit(1)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
