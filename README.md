# Migretti

Migretti is a simple, efficient, and enterprise-ready database migration tool for Python applications using PostgreSQL (via psycopg3).

## Features

-   **SQL-based migrations**: Write plain SQL for UP and DOWN migrations.
-   **Enterprise Configuration**: Support for environment variables, `.env` files, and multiple environment profiles (dev, prod).
-   **Strict Concurrency Control**: Uses PostgreSQL advisory locks to ensure safe concurrent execution.
-   **Non-Transactional Migrations**: Support for `CREATE INDEX CONCURRENTLY` and other operations via `-- migrate: no-transaction`.
-   **Dry Run**: Inspect SQL before applying with `--dry-run`.
-   **Integrity Verification**: Verify that applied migrations match files on disk with `mg verify`.
-   **Production Protection**: Safety prompts when running against production environments.
-   **Structured Logging**: JSON-formatted logging support for observability.
-   **Atomic migrations**: Strict atomicity ensures data integrity.
-   **Audit logging**: Tracks who applied/rolled back migrations and when.

## Installation

```bash
pip install migretti
```

## Quick Start

### 1. Initialize a Project

Run `init` in your project root. This creates `mg.yaml` and a `migrations/` directory.

```bash
mg init
```

### 2. Configure Database

Migretti supports multiple ways to configure your database:

**Option A: Environment Variables (Recommended for Production)**
```bash
export MG_DATABASE_URL=postgresql://user:pass@host:5432/dbname
```

**Option B: `mg.yaml` Profiles**
```yaml
envs:
  dev:
    database:
      conninfo: postgresql://postgres:password@localhost/myapp_dev
  prod:
    database:
      host: db.prod.com
      user: dbuser
      password: securepassword
      dbname: myapp_prod
```

Use profiles with the `--env` flag:
```bash
mg apply --env prod
```

### 3. Create a Migration

```bash
mg create add_users_table
```

Edit the generated file `migrations/<ULID>_add_users_table.sql`:

```sql
-- migration: Add Users Table
-- id: 01KCC600...

-- migrate: up
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    username TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- migrate: down
DROP TABLE users;
```

**Non-Transactional Migrations (e.g., Concurrent Index)**
Add the `-- migrate: no-transaction` directive:

```sql
-- migrate: no-transaction
-- migrate: up
CREATE INDEX CONCURRENTLY idx_users_name ON users(username);

-- migrate: down
DROP INDEX CONCURRENTLY idx_users_name;
```

### 4. Apply Migrations

Apply all pending migrations:

```bash
mg apply
```

Preview changes without applying:

```bash
mg apply --dry-run
```

### 5. Check Status & Verify

See what has been applied:

```bash
mg status
mg list
```

Verify integrity (checksums):

```bash
mg verify
```

### 6. Rollback

Rollback the last migration:

```bash
mg down
```

## Logging

Enable verbose or JSON logging:

```bash
mg apply --verbose
mg apply --json-log
```

## Audit Log

Migretti maintains an `_migrations_log` table in your database tracking all operations for auditing purposes.
