import argparse


def cmd_prompt(args: argparse.Namespace) -> None:
    """Print instructions for AI agents."""
    prompt = """# Migretti - Database Migration Tool Guide

You are an AI agent using `migretti` (mg) to manage database migrations for a Python/PostgreSQL project.

## Core Rules
1. **Migrations Directory**: All SQL files reside in `migrations/`. Do not create them manually; use `mg create`.
2. **File Format**: Each file has a ULID prefix. Content is split into `-- migrate: up` and `-- migrate: down`.
3. **Atomicity**: Transactional by default. Use `-- migrate: no-transaction` header for concurrent index creation.
4. **Configuration**: Managed via `mg.yaml` or `MG_DATABASE_URL`.

## Command Reference

### Setup
- `mg init`: Initialize a new project (creates `mg.yaml` and `migrations/`).

### Development
- `mg create <name>`: Generate a new migration file.
  - *Example*: `mg create add_users_table`
  - *Action*: After running, read the generated file, edit the SQL in `up` and `down` sections.
- `mg apply`: Apply all pending migrations.
- `mg status`: Check which migrations are applied/pending.
- `mg verify`: Verify checksums of applied migrations against disk.

### Rollback
- `mg down`: Rollback the last applied migration.
- `mg rollback <n>`: Rollback the last N migrations (N must be >= 1; prompts unless `--yes`).
- Rolling back a migration with no `down` SQL is refused unless `--allow-missing-down` is passed.

### Recovery
- `mg fix <id> --applied`: After a partial (dirty) failure you repaired by hand, mark the migration as applied.
- `mg fix <id> --remove`: After undoing a partial failure by hand, make the migration pending again.

### Advanced
- `mg apply --dry-run`: Validate SQL by executing it inside a transaction that is rolled back.
  Note: this is not read-only — sequences advance and DDL takes its usual locks while running.
- `mg apply --env prod` or `mg --env prod apply`: Target an environment profile from `mg.yaml` (both orders work).
  An `--env` name that is not defined in `mg.yaml` is an error.
- `mg apply --allow-out-of-order`: Apply migrations that sort before already-applied ones (after branch merges).

## Typical Workflow for Agent
1. Check current status: `mg status`
2. Create migration: `mg create <description>`
3. Read file: `cat migrations/<generated_file>.sql`
4. Write SQL: Edit the file with UP/DOWN logic.
5. Apply: `mg apply`
6. Verify: `mg status`
"""
    print(prompt)
