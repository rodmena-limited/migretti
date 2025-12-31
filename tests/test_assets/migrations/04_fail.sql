-- migration: Fail Migration
-- id: 01KCC000000000000000000004

-- migrate: up
CREATE TABLE should_not_exist (id INT);
INSERT INTO should_not_exist VALUES (1);
SELECT 1/0;

-- migrate: down
DROP TABLE should_not_exist;
