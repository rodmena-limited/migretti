-- migration: Concurrent Index
-- id: 01KCC000000000000000000002

-- migrate: no-transaction
-- migrate: up
CREATE TABLE IF NOT EXISTS test_conc (id INT);
CREATE INDEX CONCURRENTLY idx_test_conc ON test_conc(id);

-- migrate: down
DROP INDEX CONCURRENTLY idx_test_conc;
DROP TABLE test_conc;
