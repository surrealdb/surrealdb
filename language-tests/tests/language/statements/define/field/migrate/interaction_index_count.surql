/**
[test]
reason = "A COUNT index, with or without a WHERE guard, defined over rows behind a migration step counts the migrated values: the build judges the guard on the migrated value, and writes to rows behind the step move the count from the migrated value. A COUNT index defined before the migration whose guard reads another field does not stand in the migration's way."

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
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ count: 6 }]"

[[test.results]]
match = '''string::contains(<string>$result, "operator: 'CountScan'") && !string::contains(<string>$result, 'TableScan')'''

[[test.results]]
value = "[{ count: 4 }]"

[[test.results]]
match = '''string::contains(<string>$result, "operator: 'IndexCountScan'") && string::contains(<string>$result, "condition: 'a > 3'") && !string::contains(<string>$result, 'TableScan')'''

[[test.results]]
value = "4"

[[test.results]]
value = "[tb:1, tb:2, tb:5, tb:6]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 5 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 4 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = "[{ count: 5 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 4 }]"

[[test.results]]
value = "[{ count: 6 }]"

[[test.results]]
value = "4"

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
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
match = '''string::contains(<string>$result, "operator: 'IndexCountScan'") && string::contains(<string>$result, "source: 'tg'") && !string::contains(<string>$result, 'TableScan')'''

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[50, 7]"

[[test.results]]
value = "NONE"

[[test.results]]
match = "$result.database == 'lvl3'"

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
value = "[{ count: 1 }]"

[[test.results]]
match = '''string::contains(<string>$result, "operator: 'IndexCountScan'") && string::contains(<string>$result, "condition: 'a > 3'") && !string::contains(<string>$result, 'TableScan')'''

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 2 }]"
*/
DEFINE TABLE tb SCHEMAFULL;
DEFINE FIELD a ON tb TYPE string;
CREATE tb:1 SET a = '5' RETURN NONE;
CREATE tb:2 SET a = '10' RETURN NONE;
CREATE tb:3 SET a = 'junk' RETURN NONE;
CREATE tb:4 SET a = '1' RETURN NONE;
CREATE tb:5 SET a = '05' RETURN NONE;
ALTER FIELD a ON tb TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value FALLBACK 0;
CREATE tb:6 SET a = 9 RETURN NONE;
DEFINE INDEX cnt_all ON tb COUNT;
DEFINE INDEX cnt_big ON tb COUNT WHERE a > 3;
SELECT count() FROM tb GROUP ALL;
SELECT count() FROM tb GROUP ALL EXPLAIN;
SELECT count() FROM tb WHERE a > 3 GROUP ALL;
SELECT count() FROM tb WHERE a > 3 GROUP ALL EXPLAIN;
-- The guard compares migrated integers, not the stored strings: the scan agrees.
array::len(SELECT VALUE id FROM tb WHERE a > 3);
SELECT VALUE id FROM tb WHERE a > 3 ORDER BY id;
UPDATE tb:3 SET a = 20 RETURN NONE;
SELECT count() FROM tb WHERE a > 3 GROUP ALL;
UPDATE tb:1 SET a = 0 RETURN NONE;
SELECT count() FROM tb WHERE a > 3 GROUP ALL;
DELETE tb:2;
SELECT count() FROM tb WHERE a > 3 GROUP ALL;
SELECT count() FROM tb GROUP ALL;
CREATE tb:7 SET a = 100 RETURN NONE;
SELECT count() FROM tb WHERE a > 3 GROUP ALL;
SELECT count() FROM tb GROUP ALL;
array::len(SELECT VALUE id FROM tb WHERE a > 3);
-- A guard that reads another field is no obstacle to the migration, and counts rows behind the step correctly.
DEFINE TABLE tg SCHEMAFULL;
DEFINE FIELD a ON tg TYPE string;
DEFINE FIELD b ON tg TYPE string;
CREATE tg:1 SET a = '5', b = 'x' RETURN NONE;
CREATE tg:2 SET a = '6', b = 'x' RETURN NONE;
CREATE tg:3 SET a = '7', b = 'y' RETURN NONE;
DEFINE INDEX cnt_x ON tg COUNT WHERE b = 'x';
DEFINE INDEX cnt_every ON tg COUNT;
ALTER FIELD a ON tg TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value FALLBACK 0;
SELECT count() FROM tg WHERE b = 'x' GROUP ALL;
SELECT count() FROM tg WHERE b = 'x' GROUP ALL EXPLAIN;
SELECT count() FROM tg GROUP ALL;
UPDATE tg:3 SET b = 'x' RETURN NONE;
UPDATE tg:1 SET a = 50 RETURN NONE;
SELECT count() FROM tg WHERE b = 'x' GROUP ALL;
DELETE tg:2;
SELECT count() FROM tg WHERE b = 'x' GROUP ALL;
SELECT count() FROM tg GROUP ALL;
SELECT VALUE a FROM tg ORDER BY id;
-- The same at compatibility level 3.
DEFINE DATABASE lvl3 COMPATIBILITY 3;
USE DB lvl3;
DEFINE TABLE tb SCHEMAFULL;
DEFINE FIELD a ON tb TYPE string;
CREATE tb:1 SET a = '5' RETURN NONE;
CREATE tb:2 SET a = 'junk' RETURN NONE;
ALTER FIELD a ON tb TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value FALLBACK 0;
DEFINE INDEX cnt_big ON tb COUNT WHERE a > 3;
SELECT count() FROM tb WHERE a > 3 GROUP ALL;
SELECT count() FROM tb WHERE a > 3 GROUP ALL EXPLAIN;
UPDATE tb:2 SET a = 4 RETURN NONE;
SELECT count() FROM tb WHERE a > 3 GROUP ALL;
