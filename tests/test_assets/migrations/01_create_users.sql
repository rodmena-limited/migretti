-- migration: Create Users
-- id: 01KCC000000000000000000001

-- migrate: up
CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT);

-- migrate: down
DROP TABLE users;
