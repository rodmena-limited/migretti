# MIGRETTI

Migretti is a database migration tool designed for Python applications utilizing PostgreSQL. It provides a strict, SQL-first approach to schema management, ensuring atomicity, consistency, and traceability of database changes.

## 1. INSTALLATION

To install Migretti, use pip:

```bash
pip install migretti
```

Dependencies include `psycopg[binary]`, `pyyaml`, `python-ulid`, `python-dotenv`, and `sqlparse`.

## 2. GETTING STARTED

Initialize a new migration project in your repository root:

```bash
mg init
```

This command creates a `migrations/` directory and a `mg.yaml` configuration file.

## 3. CONFIGURATION

Configuration is managed via the `mg.yaml` file. The tool also supports environment variable overrides and interpolation.

Example `mg.yaml`:

```yaml
database:
  host: localhost
  port: 5432
  user: postgres
  password: ${DB_PASSWORD}
  dbname: my_database

lock_id: 894321

envs:
  production:
    database:
      host: db.prod.example.com
      dbname: prod_db
    lock_id: 999999

hooks:
  pre_apply: echo "Backup starting..."
  post_apply: echo "Migration complete."
```

**Environment Variables:**
- `MG_DATABASE_URL`: Overrides connection settings (e.g., `postgresql://user:pass@host/db`).
- `MG_ENV`: Selects the active environment profile (default: `default`).
- `MG_LOCK_ID`: Overrides the advisory lock ID.

Environment variable interpolation (e.g., `${VAR}`) is supported within `mg.yaml`.

## 4. MIGRATION WORKFLOW

### 4.1. Creating Migrations

Generate a new migration script:

```bash
mg create add_users_table
```

This creates a file in `migrations/` with a unique ULID prefix. Edit the file to define the schema changes:

```sql
-- migration: Add Users Table
-- id: 01H...

-- migrate: up
CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT);

-- migrate: down
DROP TABLE users;
```

### 4.2. Applying Migrations

Apply all pending migrations:

```bash
mg apply
```

To apply only the next pending migration:

```bash
mg up
```

### 4.3. Rolling Back

Rollback the last applied migration:

```bash
mg down
```

Rollback multiple steps:

```bash
mg rollback 3
```

### 4.4. Status and Verification

View the status of all migrations:

```bash
mg status
mg list
```

Verify that applied migrations on disk match the database checksums:

```bash
mg verify
```

## 5. ADVANCED FEATURES

### 5.1. Non-Transactional Migrations

Certain operations, such as `CREATE INDEX CONCURRENTLY`, cannot run inside a transaction block. Use the `-- migrate: no-transaction` directive in your SQL file.

```sql
-- migrate: no-transaction
-- migrate: up
CREATE INDEX CONCURRENTLY idx_users ON users(name);
```

If a non-transactional migration fails, Migretti records a "failed" status in the database. You must manually resolve the issue and then fix the migration state.

### 5.2. Dry Run

Preview the SQL to be executed without modifying the database:

```bash
mg apply --dry-run
```

For transactional migrations, Migretti performs a "Smart Dry Run," executing the SQL inside a transaction that is immediately rolled back to ensure validity.

### 5.3. Data Seeding

Manage data seeding scripts in the `seeds/` directory.

Create a seed file:

```bash
mg seed create initial_data
```

Run all seeds:

```bash
mg seed
```

### 5.4. Hooks

Define shell commands to run before or after operations in `mg.yaml`:

```yaml
hooks:
  pre_apply: ./scripts/backup_db.sh
  post_rollback: ./scripts/notify_team.sh
```

Supported hooks: `pre_apply`, `post_apply`, `pre_rollback`, `post_rollback`.

### 5.5. Migration Squashing

Combine multiple pending migrations into a single file to maintain a clean history:

```bash
mg squash release_v1
```

### 5.6. Production Safety

When running against environments named `prod`, `production`, or `live`, Migretti requires interactive confirmation unless the `--yes` flag is provided.

### 5.7. Concurrency Control

Migretti uses PostgreSQL advisory locks to ensure that only one migration process runs simultaneously, preventing race conditions in distributed deployment environments.

### 5.8. Logging

For machine-readable output, use the JSON logging flag:

```bash
mg apply --json-log
```

## 6. LICENSE

This software is released under the **Apache License 2.0**.
