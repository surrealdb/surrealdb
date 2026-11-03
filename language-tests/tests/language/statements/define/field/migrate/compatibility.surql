/**
[env]
remote-skip = "asserts the table migration job's progress before the job has run (`finished: false`, its lap and counts), which holds only because the embedded harness builds its datastores without maintenance tasks; a server wakes the job when the statement commits"

[test]
reason = "In a database at compatibility level 4 a SCHEMAFULL table refuses changes its rows might not satisfy unless they carry MIGRATE, exempts an empty table, and REMOVE FIELD drops values; a SCHEMALESS table is held to none of it, and a table made SCHEMAFULL again has its rows verified."

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
value = "[{ a: '1', b: 2, id: p:1 }]"

[[test.results]]
error = "Cannot change field `a` of table `p`: the table is SCHEMAFULL in a database at compatibility level 4 and has rows, and the new definition does not accept every value the old one did: add MIGRATE EXISTING ROWS"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot change field `c` of table `p`: the table is SCHEMAFULL in a database at compatibility level 4 and has rows, and the field's TYPE does not accept NONE: add MIGRATE EXISTING ROWS to say what existing rows hold"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 1, b: 2, e: 5, id: p:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 1, e: 5, id: p:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: loose:1, x: 'x' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot change field `c` of table `p`: the table is SCHEMAFULL in a database at compatibility level 4 and has rows, and the new definition does not accept every value the old one did: add MIGRATE EXISTING ROWS"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ events: {  }, fields: { a: 'DEFINE FIELD a ON p TYPE int PERMISSIONS FULL', c: 'DEFINE FIELD c ON p TYPE string PERMISSIONS FULL', d: 'DEFINE FIELD d ON p TYPE none | int PERMISSIONS FULL', e: 'DEFINE FIELD e ON p TYPE int PERMISSIONS FULL' }, indexes: {  }, lives: {  }, migrations: { generation: 17, guaranteed: [], migrations: [{ field: 'a', generation: 6, name: 'a_int' }, { field: 'e', generation: 10, name: 'add_e' }, { field: 'b', generation: 12, name: 'remove b after 11' }], progress: { fallbacks: 0, lap: 17, rewritten: 0, scanned: 0 }, undeclared: { failures: 0, finished: false }, verification: [{ failures: 0, field: 'b', finished: false }, { failures: 0, field: 'a', finished: false }, { failures: 0, field: 'c', finished: false }, { failures: 0, field: 'd', finished: false }, { failures: 0, field: 'e', finished: false }] }, tables: {  } }"

[[test.results]]
value = "[{ a: 1, e: 5, id: p:1 }]"

[[test.results]]
value = "{ accesses: {  }, analyzers: {  }, apis: {  }, buckets: {  }, compatibility: 4, configs: {  }, extensions: {  }, functions: {  }, models: {  }, params: {  }, sequences: {  }, tables: { loose: 'DEFINE TABLE loose TYPE ANY SCHEMALESS PERMISSIONS NONE', p: 'DEFINE TABLE p TYPE NORMAL SCHEMAFULL PERMISSIONS NONE' }, types: {  }, users: {  } }"
*/
DEFINE TABLE p SCHEMAFULL;
DEFINE FIELD a ON p TYPE string;
DEFINE FIELD b ON p TYPE int;
-- Empty table: no migration needed.
ALTER FIELD a ON p TYPE int;
ALTER FIELD a ON p TYPE string;
CREATE p:1 SET a = '1', b = 2;
ALTER FIELD a ON p TYPE int;
ALTER FIELD a ON p TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value FALLBACK 0;
ALTER FIELD b ON p TYPE number;
DEFINE FIELD c ON p TYPE int;
DEFINE FIELD d ON p TYPE option<int>;
DEFINE FIELD e ON p TYPE int MIGRATE EXISTING ROWS AS add_e USING 5;
SELECT * FROM p;
REMOVE FIELD b ON p;
SELECT * FROM p;
-- A SCHEMALESS table is held to no such rule.
DEFINE TABLE loose SCHEMALESS;
DEFINE FIELD x ON loose TYPE string;
CREATE loose:1 SET x = 'x';
DEFINE FIELD OVERWRITE x ON loose TYPE int;
-- Neither is a table made SCHEMALESS, by OVERWRITE or by ALTER. Made
-- SCHEMAFULL again, its rows are verified, none is rewritten, and it is held
-- to the rules from then on.
DEFINE TABLE OVERWRITE p TYPE NORMAL SCHEMALESS;
DEFINE FIELD c ON p TYPE int;
ALTER TABLE p SCHEMAFULL;
ALTER FIELD c ON p TYPE string;
ALTER TABLE p SCHEMALESS;
ALTER FIELD c ON p TYPE string;
ALTER TABLE p SCHEMAFULL;
INFO FOR TABLE p;
SELECT * FROM p;
INFO FOR DB;
