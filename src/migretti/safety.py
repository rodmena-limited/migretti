import os
import sys
import argparse
from typing import Optional


def check_prod_protection(args: argparse.Namespace) -> None:
    """
    Ask for confirmation before running against a production-named environment.

    This applies to dry runs too: a "smart" dry run executes SQL against the
    target (inside a transaction that is rolled back), so it is not a
    read-only operation.
    """
    env: Optional[str] = getattr(args, "env", None) or os.getenv("MG_ENV", "default")
    if not env or env.lower() not in ("prod", "production", "live"):
        return
    if getattr(args, "yes", False):
        return

    note = (
        " (dry run: SQL is still executed and rolled back)"
        if getattr(args, "dry_run", False)
        else ""
    )
    print(
        f"⚠️  WARNING: You are about to run this operation against the '{env}' environment!{note}"
    )
    try:
        response = input("Are you sure you want to continue? (yes/no): ")
    except (EOFError, KeyboardInterrupt):
        print("\nNo interactive confirmation available; pass --yes to proceed without prompting.")
        sys.exit(1)
    if response.lower() != "yes":
        print("Operation cancelled.")
        sys.exit(0)


def confirm_or_abort(prompt: str, assume_yes: bool) -> None:
    """Yes/no gate for multi-step destructive operations."""
    if assume_yes:
        return
    try:
        response = input(prompt + " (yes/no): ")
    except (EOFError, KeyboardInterrupt):
        print("\nNo interactive confirmation available; pass --yes to proceed without prompting.")
        sys.exit(1)
    if response.lower() != "yes":
        print("Operation cancelled.")
        sys.exit(0)
