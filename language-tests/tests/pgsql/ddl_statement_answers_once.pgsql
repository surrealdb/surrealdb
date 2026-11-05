/**
[env]
namespace = true
database = true

[test]
reason = "A PostgreSQL statement that defines several things (a table with its columns and its primary key, several columns added by one ALTER TABLE, several tables dropped by one DROP TABLE) is answered once, as PostgreSQL answers it with one command tag"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ age: 40i32, city: 'London', id: 1, name: 'Tobie', score: 7i32 }]"

[[test.results]]
error = "Database index `ddl_once_person_name_uniq` already contains 'Tobie', with record `ddl_once_person:1`"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The table 'ddl_once_a' does not exist"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: 1, name: 'Tobie' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: 1, name: 'Tobie' }]"
*/

CREATE TABLE ddl_once_person (id INT PRIMARY KEY, name TEXT UNIQUE, age INT);

CREATE TABLE IF NOT EXISTS ddl_once_person (id INT PRIMARY KEY, name TEXT UNIQUE, age INT);

CREATE TABLE ddl_once_a (x INT);

CREATE TABLE ddl_once_b (y INT);

ALTER TABLE ddl_once_person ADD COLUMN city TEXT, ADD COLUMN score INT;

INSERT INTO ddl_once_person (id, name, age, city, score) VALUES (1, 'Tobie', 40, 'London', 7) RETURNING *;

INSERT INTO ddl_once_person (id, name, age) VALUES (2, 'Tobie', 41);

DROP TABLE ddl_once_a, ddl_once_b;

DROP TABLE IF EXISTS ddl_once_a, ddl_once_b;

SELECT x FROM ddl_once_a;

BEGIN;
CREATE TABLE ddl_once_txn (id INT PRIMARY KEY, name TEXT);
INSERT INTO ddl_once_txn (id, name) VALUES (1, 'Tobie') RETURNING id, name;
COMMIT;

SELECT id, name FROM ddl_once_txn;
