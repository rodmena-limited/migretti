-- migration: Slow Migration
-- id: 01KCC000000000000000000003

-- migrate: up
CREATE TABLE slow_test (id INT);
SELECT pg_sleep(2);

-- migrate: down
DROP TABLE slow_test;
