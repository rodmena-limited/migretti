-- migration: Large Dataset
-- id: 01KCCE833259H1VWP7SRBRK50S

-- migrate: up
CREATE TABLE big_data (id SERIAL PRIMARY KEY, val TEXT);
-- Generate 100k rows
INSERT INTO big_data (val) SELECT 'value-' || generate_series(1, 100000);
-- Index it
CREATE INDEX idx_big_data_val ON big_data(val);

-- migrate: down
DROP TABLE big_data;