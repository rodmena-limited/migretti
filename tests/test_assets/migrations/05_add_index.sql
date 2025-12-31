-- migration: Add Index
-- id: 01KCC000000000000000000005

-- migrate: up
CREATE INDEX idx_big_data_val ON big_data(val);

-- migrate: down
DROP INDEX idx_big_data_val;
