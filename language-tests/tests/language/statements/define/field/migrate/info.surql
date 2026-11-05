/**
[env]
remote-skip = "asserts the table migration job's progress before the job has run (`finished: false`, its lap and counts), which holds only because the embedded harness builds its datastores without maintenance tasks; a server wakes the job when the statement commits"
compatibility = 3

[test]
reason = "A migration leaves the field definition's rendering unchanged, and INFO FOR TABLE lists it by name in its own section."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ events: {  }, fields: { a: 'DEFINE FIELD a ON `tb` TYPE int PERMISSIONS FULL' }, indexes: {  }, lives: {  }, migrations: { generation: 4, guaranteed: [], migrations: [{ field: 'a', generation: 3, name: 'a_to_int' }], progress: { fallbacks: 0, lap: 2, rewritten: 0, scanned: 0 }, verification: [{ failures: 0, field: 'a', finished: false }] }, tables: {  } }"

[[test.results]]
value = "{ a: 'DEFINE FIELD a ON `tb` TYPE int PERMISSIONS FULL' }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ events: {  }, fields: { a: 'DEFINE FIELD a ON plain TYPE string PERMISSIONS FULL' }, indexes: {  }, lives: {  }, tables: {  } }"
*/
DEFINE TABLE tb SCHEMAFULL;
DEFINE FIELD a ON tb TYPE string;
CREATE tb:1 SET a = '5' RETURN NONE;
ALTER FIELD a ON tb TYPE int MIGRATE EXISTING ROWS AS a_to_int USING <int>$value FALLBACK 0;
INFO FOR TABLE tb;
(INFO FOR TABLE tb).fields;
DEFINE TABLE plain SCHEMAFULL;
DEFINE FIELD a ON plain TYPE string;
INFO FOR TABLE plain;
