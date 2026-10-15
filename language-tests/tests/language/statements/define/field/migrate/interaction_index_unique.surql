/**
[test]
reason = "A UNIQUE index defined over rows behind a migration step judges the migrated values: values that are distinct as stored but equal once migrated are refused, values that are equal as stored but distinct once migrated are accepted, and a later write collides with the migrated value of a row it never rewrote."

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
value = "NONE"

[[test.results]]
error = "Database index `ua` already contains 5, with record ``tb`:1`"

[[test.results]]
value = "{  }"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
match = '''string::contains(<string>$result, "operator: 'IndexScan'") && string::contains(<string>$result, "index: 'ua'") && !string::contains(<string>$result, 'TableScan')'''

[[test.results]]
value = "[tb:1]"

[[test.results]]
value = "[tb:2]"

[[test.results]]
error = "Database index `ua` already contains 5, with record ``tb`:1`"

[[test.results]]
error = "Database index `ua` already contains 5, with record ``tb`:1`"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[tb:9]"

[[test.results]]
value = "[tb:2]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[tb:1]"

[[test.results]]
match = '''string::contains(<string>$result, "operator: 'IndexScan'") && string::contains(<string>$result, "index: 'ua'") && !string::contains(<string>$result, 'TableScan')'''

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
error = "Database index `u0` already contains 1f, with record `t2:1`"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['1', '1f']"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[t2:1]"

[[test.results]]
value = "[t2:2]"

[[test.results]]
match = '''string::contains(<string>$result, "operator: 'IndexScan'") && string::contains(<string>$result, "index: 'ua'") && !string::contains(<string>$result, 'TableScan')'''

[[test.results]]
error = "Database index `ua` already contains '1', with record `t2:1`"

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
value = "NONE"

[[test.results]]
error = "Database index `ug` already contains ['x', 5], with record `tc:1`"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[tc:1]"

[[test.results]]
match = '''string::contains(<string>$result, "operator: 'IndexScan'") && string::contains(<string>$result, "index: 'ug'") && !string::contains(<string>$result, 'TableScan')'''

[[test.results]]
error = "Database index `ug` already contains ['x', 5], with record `tc:1`"

[[test.results]]
value = "[]"

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
error = "Database index `ua` already contains 5, with record ``tb`:1`"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Database index `ua` already contains 5, with record ``tb`:1`"

[[test.results]]
match = '''string::contains(<string>$result, "operator: 'IndexScan'") && string::contains(<string>$result, "index: 'ua'") && !string::contains(<string>$result, 'TableScan')'''
*/
DEFINE TABLE tb SCHEMAFULL;
DEFINE FIELD a ON tb TYPE string;
CREATE tb:1 SET a = '5' RETURN NONE;
CREATE tb:2 SET a = '10' RETURN NONE;
CREATE tb:3 SET a = '05' RETURN NONE;
ALTER FIELD a ON tb TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value FALLBACK 0;
-- '5' and '05' differ as stored and are both 5 once migrated.
DEFINE INDEX ua ON tb FIELDS a UNIQUE;
(INFO FOR TABLE tb).indexes;
DELETE tb:3;
DEFINE INDEX ua ON tb FIELDS a UNIQUE;
SELECT VALUE id FROM tb WHERE a = 5 EXPLAIN;
SELECT VALUE id FROM tb WHERE a = 5;
SELECT VALUE id FROM tb WHERE a = 10;
-- A write collides with the migrated value of a row behind the step.
CREATE tb:9 SET a = 5 RETURN NONE;
UPDATE tb:2 SET a = 5 RETURN NONE;
UPDATE tb:2 SET a = 11 RETURN NONE;
CREATE tb:9 SET a = 10 RETURN NONE;
SELECT VALUE id FROM tb WHERE a = 10;
SELECT VALUE id FROM tb WHERE a = 11;
REBUILD INDEX ua ON tb;
SELECT VALUE id FROM tb WHERE a = 5;
SELECT VALUE id FROM tb WHERE a = 5 EXPLAIN;
-- Equal as stored (1 and 1.0), distinct once migrated ('1' and '1f').
DEFINE TABLE t2 SCHEMAFULL;
DEFINE FIELD a ON t2 TYPE any;
CREATE t2:1 SET a = 1 RETURN NONE;
CREATE t2:2 SET a = 1.0f RETURN NONE;
DEFINE INDEX u0 ON t2 FIELDS a UNIQUE;
ALTER FIELD a ON t2 TYPE string MIGRATE EXISTING ROWS AS a_str USING <string>$value FALLBACK '';
SELECT VALUE a FROM t2 ORDER BY id;
DEFINE INDEX ua ON t2 FIELDS a UNIQUE;
SELECT VALUE id FROM t2 WHERE a = '1';
SELECT VALUE id FROM t2 WHERE a = '1f';
SELECT VALUE id FROM t2 WHERE a = '1f' EXPLAIN;
UPDATE t2:2 SET a = '1' RETURN NONE;
-- A composite UNIQUE index judges the migrated column too.
DEFINE TABLE tc SCHEMAFULL;
DEFINE FIELD g ON tc TYPE string;
DEFINE FIELD a ON tc TYPE string;
CREATE tc:1 SET g = 'x', a = '5' RETURN NONE;
CREATE tc:2 SET g = 'x', a = '05' RETURN NONE;
CREATE tc:3 SET g = 'y', a = '5' RETURN NONE;
ALTER FIELD a ON tc TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value FALLBACK 0;
DEFINE INDEX ug ON tc FIELDS g, a UNIQUE;
DELETE tc:2;
DEFINE INDEX ug ON tc FIELDS g, a UNIQUE;
SELECT VALUE id FROM tc WHERE g = 'x' AND a = 5;
SELECT VALUE id FROM tc WHERE g = 'x' AND a = 5 EXPLAIN;
CREATE tc:4 SET g = 'x', a = 5 RETURN NONE;
CREATE tc:4 SET g = 'y', a = 6 RETURN NONE;
-- The same at compatibility level 3.
DEFINE DATABASE lvl3 COMPATIBILITY 3;
USE DB lvl3;
DEFINE TABLE tb SCHEMAFULL;
DEFINE FIELD a ON tb TYPE string;
CREATE tb:1 SET a = '5' RETURN NONE;
CREATE tb:3 SET a = '05' RETURN NONE;
ALTER FIELD a ON tb TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value FALLBACK 0;
DEFINE INDEX ua ON tb FIELDS a UNIQUE;
DELETE tb:3;
DEFINE INDEX ua ON tb FIELDS a UNIQUE;
CREATE tb:9 SET a = 5 RETURN NONE;
SELECT VALUE id FROM tb WHERE a = 5 EXPLAIN;
