/**
[test]
reason = "REMOVE UNDECLARED FIELDS clears values no field declares from every row stored before it, keeps id, re-runs as a no-op, and refuses an index on an undeclared path while pending."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 1, id: p:1, legacy: 'x', other: { y: 1 } }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 1, id: p:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 2, id: p:2, legacy: 'new' }]"

[[test.results]]
value = "[{ a: 1, id: p:1 }, { a: 2, id: p:2, legacy: 'new' }]"

[[test.results]]
error = "Cannot index `legacy` on table `p`: migration `drop_legacy` removes undeclared fields from the rows stored before it, and has not finished rewriting them"
*/
DEFINE TABLE p SCHEMALESS;
CREATE p:1 SET a = 1, legacy = 'x', other = { y: 1 };
DEFINE FIELD a ON p TYPE int;
ALTER TABLE p MIGRATE EXISTING ROWS AS drop_legacy REMOVE UNDECLARED FIELDS;
SELECT * FROM p;
ALTER TABLE p MIGRATE EXISTING ROWS AS drop_legacy REMOVE UNDECLARED FIELDS;
CREATE p:2 SET a = 2, legacy = 'new';
SELECT * FROM p ORDER BY id;
DEFINE INDEX by_legacy ON p FIELDS legacy;
