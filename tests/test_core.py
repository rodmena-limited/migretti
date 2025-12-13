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
