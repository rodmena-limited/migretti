-- migration: Dry Run Test
-- id: 01KCC000000000000000000007

-- migrate: up
CREATE TABLE dry_run_table (id INT);

-- migrate: down
DROP TABLE dry_run_table;
