-- migration: Tamper Test
-- id: 01KCC000000000000000000006

-- migrate: up
CREATE TABLE tamper_test (id INT);

-- migrate: down
DROP TABLE tamper_test;
