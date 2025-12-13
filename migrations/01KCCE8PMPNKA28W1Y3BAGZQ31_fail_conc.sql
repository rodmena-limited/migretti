-- migration: Fail Conc
-- id: 01KCCE8PMPNKA28W1Y3BAGZQ31

-- migrate: no-transaction
-- migrate: up
CREATE TABLE IF NOT EXISTS partial_table (id INT);
SELECT 1/0; -- Fail here
CREATE INDEX CONCURRENTLY idx_partial ON partial_table(id);

-- migrate: down
DROP TABLE partial_table;