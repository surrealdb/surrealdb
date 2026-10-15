/**
[test]
reason = "A plain index defined over a field whose rows are behind a migration step holds the migrated values, not the stored ones: it is the access path of the lookups and ranges that name those values, writes to a row behind the step move its entry from the migrated value, and an index defined before the migration on another field keeps working."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[tb:1, tb:3, tb:5]"

[[test.results]]
match = '''string::contains(<string>$result, "operator: 'IndexScan'") && string::contains(<string>$result, "index: 'by_a'") && !string::contains(<string>$result, 'TableScan')'''

[[test.results]]
value = "[tb:4]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[tb:2]"

[[test.results]]
match = '''string::contains(<string>$result, "operator: 'IndexScan'") && string::contains(<string>$result, "index: 'by_a'") && !string::contains(<string>$result, 'TableScan')'''

[[test.results]]
value = "[tb:1, tb:2, tb:3, tb:5]"

[[test.results]]
match = '''string::contains(<string>$result, "operator: 'UnionIndexScan'") && string::contains(<string>$result, "index: 'by_a'") && !string::contains(<string>$result, 'TableScan')'''

[[test.results]]
value = "[tb:2, tb:4]"

[[test.results]]
match = '''string::contains(<string>$result, "operator: 'IndexScan'") && string::contains(<string>$result, "index: 'by_b'") && !string::contains(<string>$result, 'TableScan')'''

[[test.results]]
value = "[]"

[[test.results]]
value = "[tb:3, tb:5]"

[[test.results]]
value = "[tb:1]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[tb:5]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[tb:2]"

[[test.results]]
value = "[tb:4, tb:5]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[7, 10, 0, 5]"

[[test.results]]
value = "[true, true, true, true]"

[[test.results]]
value = "[tb:2]"

[[test.results]]
value = "{ events: {  }, fields: { a: 'DEFINE FIELD a ON `tb` TYPE int PERMISSIONS FULL', b: 'DEFINE FIELD b ON `tb` TYPE string PERMISSIONS FULL' }, indexes: { by_a: 'DEFINE INDEX by_a ON `tb` FIELDS a', by_b: 'DEFINE INDEX by_b ON `tb` FIELDS b' }, lives: {  }, migrations: { generation: 5, guaranteed: [], migrations: [{ field: 'a', generation: 4, name: 'a_int' }], progress: { fallbacks: 0, lap: 2, rewritten: 0, scanned: 0 }, verification: [{ failures: 0, field: 'a', finished: false }] }, tables: {  } }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ database: 'lvl3', namespace: 'test' }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[tb:1]"

[[test.results]]
match = '''string::contains(<string>$result, "operator: 'IndexScan'") && string::contains(<string>$result, "index: 'by_a'") && !string::contains(<string>$result, 'TableScan')'''

[[test.results]]
value = "[tb:2]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[tb:1]"
*/
DEFINE TABLE tb SCHEMAFULL;
DEFINE FIELD a ON tb TYPE string;
DEFINE FIELD b ON tb TYPE string;
CREATE tb:1 SET a = '5', b = 'x' RETURN NONE;
CREATE tb:2 SET a = '10', b = 'y' RETURN NONE;
CREATE tb:3 SET a = '05', b = 'x' RETURN NONE;
CREATE tb:4 SET a = 'junk', b = 'y' RETURN NONE;
DEFINE INDEX by_b ON tb FIELDS b;
ALTER FIELD a ON tb TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value FALLBACK 0;
CREATE tb:5 SET a = 5, b = 'z' RETURN NONE;
DEFINE INDEX by_a ON tb FIELDS a;
SELECT VALUE id FROM tb WHERE a = 5 ORDER BY id;
SELECT VALUE id FROM tb WHERE a = 5 EXPLAIN;
SELECT VALUE id FROM tb WHERE a = 0 ORDER BY id;
SELECT VALUE id FROM tb WHERE a = '5' ORDER BY id;
SELECT VALUE id FROM tb WHERE a > 5 ORDER BY id;
SELECT VALUE id FROM tb WHERE a > 5 EXPLAIN;
SELECT VALUE id FROM tb WHERE a IN [5, 10] ORDER BY id;
SELECT VALUE id FROM tb WHERE a IN [5, 10] EXPLAIN;
SELECT VALUE id FROM tb WHERE b = 'y' ORDER BY id;
SELECT VALUE id FROM tb WHERE b = 'y' EXPLAIN;
-- A write to a row behind the step moves the index entry of its migrated value.
UPDATE tb:1 SET a = 7 RETURN NONE;
SELECT VALUE id FROM tb WHERE a = 5 ORDER BY id;
SELECT VALUE id FROM tb WHERE a = 7 ORDER BY id;
DELETE tb:3;
SELECT VALUE id FROM tb WHERE a = 5 ORDER BY id;
UPDATE tb:4 SET b = 'z' RETURN NONE;
SELECT VALUE id FROM tb WHERE b = 'y' ORDER BY id;
SELECT VALUE id FROM tb WHERE b = 'z' ORDER BY id;
UPDATE tb:2 SET b = 'q' RETURN NONE;
SELECT VALUE a FROM tb ORDER BY id;
SELECT VALUE type::is_int(a) FROM tb ORDER BY id;
SELECT VALUE id FROM tb WHERE a = 10 ORDER BY id;
INFO FOR TABLE tb;
-- The same at compatibility level 3.
DEFINE DATABASE lvl3 COMPATIBILITY 3;
USE DB lvl3;
DEFINE TABLE tb SCHEMAFULL;
DEFINE FIELD a ON tb TYPE string;
CREATE tb:1 SET a = '5' RETURN NONE;
CREATE tb:2 SET a = 'junk' RETURN NONE;
ALTER FIELD a ON tb TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value FALLBACK 0;
DEFINE INDEX by_a ON tb FIELDS a;
SELECT VALUE id FROM tb WHERE a = 5 ORDER BY id;
SELECT VALUE id FROM tb WHERE a = 5 EXPLAIN;
SELECT VALUE id FROM tb WHERE a = 0 ORDER BY id;
UPDATE tb:1 SET a = 6 RETURN NONE;
SELECT VALUE id FROM tb WHERE a = 5 ORDER BY id;
SELECT VALUE id FROM tb WHERE a = 6 ORDER BY id;
