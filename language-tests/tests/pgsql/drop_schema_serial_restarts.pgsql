/**
[env]
namespace = true
database = true
imports = ["pgsql/_drop_schema_tables.surql"]

[test]
reason = "DROP SCHEMA public CASCADE removes the sequence of a serial column, so a table created again numbers its rows from 1"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: 1, name: 'a' }, { id: 2, name: 'b' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: 1 }]"
*/

CREATE TABLE dsc_serial (id SERIAL PRIMARY KEY, name TEXT);

INSERT INTO dsc_serial (name) VALUES ('a'), ('b');

DROP SCHEMA public CASCADE;

CREATE TABLE dsc_serial (id SERIAL PRIMARY KEY, name TEXT);

INSERT INTO dsc_serial (name) VALUES ('c') RETURNING id;
