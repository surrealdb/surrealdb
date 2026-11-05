/**
[test]
reason = "A COUNT index whose WHERE guard reads a field is an index that reads it: a migration of that field is refused until the index is removed, exactly as for an index column, because the count the index holds was judged on the stored values and the guard would be judged on the migrated ones from then on."

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
value = "[{ count: 1 }]"

[[test.results]]
error = "Cannot migrate field `a` of table `tb`: the index `cnt_five` reads it: remove the index, migrate, then define it again"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "1"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
match = '''string::contains(<string>$result, "operator: 'IndexCountScan'") && string::contains(<string>$result, "condition: 'a = 5'") && !string::contains(<string>$result, 'TableScan')'''

[[test.results]]
value = "1"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "2"
*/
DEFINE TABLE tb SCHEMAFULL;
DEFINE FIELD a ON tb TYPE string;
CREATE tb:1 SET a = '5' RETURN NONE;
CREATE tb:2 SET a = '6' RETURN NONE;
DEFINE INDEX cnt_five ON tb COUNT WHERE a = '5';
SELECT count() FROM tb WHERE a = '5' GROUP ALL;
ALTER FIELD a ON tb TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value FALLBACK 0;
-- Whatever the migration did, the count the index serves is the count a scan computes.
SELECT count() FROM tb WHERE a = '5' GROUP ALL;
array::len(SELECT VALUE id FROM tb WHERE a = '5');
-- The way the refusal asks for.
REMOVE INDEX cnt_five ON tb;
ALTER FIELD a ON tb TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value FALLBACK 0;
DEFINE INDEX cnt_five ON tb COUNT WHERE a = 5;
SELECT count() FROM tb WHERE a = 5 GROUP ALL;
SELECT count() FROM tb WHERE a = 5 GROUP ALL EXPLAIN;
array::len(SELECT VALUE id FROM tb WHERE a = 5);
UPDATE tb:2 SET a = 5 RETURN NONE;
SELECT count() FROM tb WHERE a = 5 GROUP ALL;
array::len(SELECT VALUE id FROM tb WHERE a = 5);
