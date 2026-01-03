import pytest
from migretti.core import parse_migration_sql, calculate_checksum


def test_parse_migration_sql():
    content = """-- migration: Test
-- migrate: up
CREATE TABLE test (id INT);
-- migrate: down
DROP TABLE test;
"""
    up, down, no_trans = parse_migration_sql(content)
    assert "CREATE TABLE test (id INT);" in up
    assert "DROP TABLE test;" in down
    assert no_trans is False

    # Verify markers are not included
    assert "-- migrate: up" not in up
    assert "-- migrate: down" not in down


def test_parse_migration_sql_no_trans():
    content = """-- migrate: no-transaction
-- migrate: up
CREATE INDEX x;
-- migrate: down
DROP INDEX x;
"""
    up, down, no_trans = parse_migration_sql(content)
    assert no_trans is True
    assert "CREATE INDEX x;" in up


def test_calculate_checksum():
    c1 = calculate_checksum("abc")
    c2 = calculate_checksum("abc")
    assert c1 == c2
    assert calculate_checksum("def") != c1


def test_parse_migration_sql_missing_up_marker():
    """Migration without '-- migrate: up' marker should raise ValueError."""
    content = """-- migration: Test
CREATE TABLE test (id INT);
-- migrate: down
DROP TABLE test;
"""
    with pytest.raises(ValueError, match="missing '-- migrate: up' marker"):
        parse_migration_sql(content, "test_migration.sql")


def test_parse_migration_sql_empty_up_section():
    """Migration with empty up section should raise ValueError."""
    content = """-- migration: Test
-- migrate: up

-- migrate: down
DROP TABLE test;
"""
    with pytest.raises(ValueError, match="has empty '-- migrate: up' section"):
        parse_migration_sql(content, "test_migration.sql")


def test_parse_migration_sql_missing_down_section(caplog):
    """Migration without down section should warn but succeed."""
    content = """-- migration: Test
-- migrate: up
CREATE TABLE test (id INT);
"""
    up, down, no_trans = parse_migration_sql(content, "test_migration.sql")
    assert "CREATE TABLE test (id INT);" in up
    assert down == ""
    assert "has no '-- migrate: down' section" in caplog.text


def test_parse_migration_sql_whitespace_only_up_section():
    """Migration with only whitespace in up section should raise ValueError."""
    content = """-- migrate: up


-- migrate: down
DROP TABLE test;
"""
    with pytest.raises(ValueError, match="has empty '-- migrate: up' section"):
        parse_migration_sql(content, "test_migration.sql")
