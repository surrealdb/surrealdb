/**
[env]
namespace = true
database = true

[test]
reason = "A ROW(...)::type value is written by INSERT ... ON CONFLICT DO UPDATE ... WHERE as by a plain INSERT, and a composite attribute reports the SQL type it was declared with"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ home: { street: 'Main St', zip: 1i32 }, id: 1 }]"

[[test.results]]
value = "[{ home: { street: 'Side Rd', zip: 2i32 }, id: 1 }]"

[[test.results]]
value = "[{ id: 1, street: 'Side Rd', zip: 2i32 }]"

[[test.results]]
value = "[{ attname: 'street', atttypmod: 20, typname: 'varchar' }, { attname: 'zip', atttypmod: -1, typname: 'int4' }]"

*/

CREATE TYPE address AS (street varchar(16), zip int);

CREATE TABLE person (id int PRIMARY KEY, home address);

INSERT INTO person (id, home) VALUES (1, ROW('Main St', 1)::address) ON CONFLICT (id) DO UPDATE SET home = excluded.home WHERE person.id > 0;

INSERT INTO person (id, home) VALUES (1, ROW('Side Rd', 2)::address) ON CONFLICT (id) DO UPDATE SET home = excluded.home WHERE person.id > 0;

SELECT id, (home).street AS street, (home).zip AS zip FROM person;

SELECT a.attname, t.typname, a.atttypmod FROM pg_attribute a JOIN pg_type t ON t.oid = a.atttypid JOIN pg_class c ON c.oid = a.attrelid WHERE c.relname = 'address' ORDER BY a.attnum;
