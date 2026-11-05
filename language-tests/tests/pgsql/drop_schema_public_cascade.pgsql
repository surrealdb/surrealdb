/**
[env]
namespace = true
database = true
imports = ["pgsql/_drop_schema_tables.surql"]

[test]
reason = "DROP SCHEMA public CASCADE removes every table of the database, a view and a table that others reference included, and the database takes tables again afterwards"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The table 'dsc_author' does not exist"

[[test.results]]
error = "The table 'dsc_post' does not exist"

[[test.results]]
error = "The table 'dsc_titles' does not exist"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: 7, name: 'Grace' }]"

[[test.results]]
value = "[{ id: 7, name: 'Grace' }]"
*/

DROP SCHEMA IF EXISTS public CASCADE;

SELECT * FROM dsc_author;

SELECT * FROM dsc_post;

SELECT * FROM dsc_titles;

CREATE SCHEMA public;

CREATE TABLE dsc_author (id INT PRIMARY KEY, name TEXT);

INSERT INTO dsc_author (id, name) VALUES (7, 'Grace');

SELECT id, name FROM dsc_author;
