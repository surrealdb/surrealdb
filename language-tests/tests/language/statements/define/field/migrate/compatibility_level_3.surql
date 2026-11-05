/**
[env]
remote-skip = "asserts the table migration job's progress before the job has run (`finished: false`, its lap and counts), which holds only because the embedded harness builds its datastores without maintenance tasks; a server wakes the job when the statement commits"
compatibility = 3

[test]
reason = "In a database at compatibility level 3 a SCHEMAFULL table accepts the changes level 4 refuses and keeps the values of a removed field. ALTER DATABASE COMPATIBILITY 4 changes only the definition, queues a verification of the stored rows, and holds the table to level 4 from then on; DEFINE DATABASE OVERWRITE without the clause keeps the level, and lowering it to 3 lifts the rules again."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: '1', b: 2, id: p:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: '1', b: 2, id: p:1 }]"

[[test.results]]
value = "{ events: {  }, fields: { a: 'DEFINE FIELD a ON p TYPE int PERMISSIONS FULL', c: 'DEFINE FIELD c ON p TYPE int PERMISSIONS FULL' }, indexes: {  }, lives: {  }, tables: {  } }"

[[test.results]]
value = "3"

[[test.results]]
value = "NONE"

[[test.results]]
value = "4"

[[test.results]]
value = "[{ a: '1', b: 2, id: p:1 }]"

[[test.results]]
value = "{ events: {  }, fields: { a: 'DEFINE FIELD a ON p TYPE int PERMISSIONS FULL', c: 'DEFINE FIELD c ON p TYPE int PERMISSIONS FULL' }, indexes: {  }, lives: {  }, migrations: { generation: 6, guaranteed: [], migrations: [], undeclared: { failures: 0, finished: false }, verification: [{ failures: 0, field: 'a', finished: false }, { failures: 0, field: 'c', finished: false }] }, tables: {  } }"

[[test.results]]
error = "Cannot change field `a` of table `p`: the table is SCHEMAFULL in a database at compatibility level 4 and has rows, and the new definition does not accept every value the old one did: add MIGRATE EXISTING ROWS"

[[test.results]]
error = "Cannot change field `f` of table `p`: the table is SCHEMAFULL in a database at compatibility level 4 and has rows, and the field's TYPE does not accept NONE: add MIGRATE EXISTING ROWS to say what existing rows hold"

[[test.results]]
value = "NONE"

[[test.results]]
value = "4"

[[test.results]]
error = "Cannot change field `a` of table `p`: the table is SCHEMAFULL in a database at compatibility level 4 and has rows, and the new definition does not accept every value the old one did: add MIGRATE EXISTING ROWS"

[[test.results]]
value = "NONE"

[[test.results]]
value = "3"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ accesses: {  }, buckets: {  }, databases: { test: 'DEFINE DATABASE test COMPATIBILITY 3' }, users: {  } }"

[[test.results]]
value = "{ accesses: [], buckets: [], databases: [{ compatibility: 3, id: 0, name: 'test' }], users: [] }"
*/
DEFINE TABLE p SCHEMAFULL;
DEFINE FIELD a ON p TYPE string;
DEFINE FIELD b ON p TYPE int;
CREATE p:1 SET a = '1', b = 2;
ALTER FIELD a ON p TYPE int;
DEFINE FIELD c ON p TYPE int;
REMOVE FIELD b ON p;
SELECT * FROM p;
INFO FOR TABLE p;
(INFO FOR DB).compatibility;
-- Raising the level rewrites nothing: the rows stored at level 3 are verified.
ALTER DATABASE COMPATIBILITY 4;
(INFO FOR DB).compatibility;
SELECT * FROM p;
INFO FOR TABLE p;
ALTER FIELD a ON p TYPE string;
DEFINE FIELD f ON p TYPE int;
-- A redefinition without the clause keeps the level.
DEFINE DATABASE OVERWRITE test;
(INFO FOR DB).compatibility;
ALTER FIELD a ON p TYPE string;
-- Lowering it only sets the level.
ALTER DATABASE test COMPATIBILITY 3;
(INFO FOR DB).compatibility;
ALTER FIELD a ON p TYPE string;
INFO FOR NS;
INFO FOR NS STRUCTURE;
