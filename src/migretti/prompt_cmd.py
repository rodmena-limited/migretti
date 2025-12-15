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
- `mg rollback <n>`: Rollback the last N migrations.

### Advanced
- `mg apply --dry-run`: Preview SQL without executing.
- `mg apply --env prod`: Target a specific environment profile from `mg.yaml`.

## Typical Workflow for Agent
1. Check current status: `mg status`
2. Create migration: `mg create <description>`
3. Read file: `cat migrations/<generated_file>.sql`
4. Write SQL: Edit the file with UP/DOWN logic.
5. Apply: `mg apply`
6. Verify: `mg status`
"""
    print(prompt)