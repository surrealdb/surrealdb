/**
[test]
reason = "A view's maintenance writes and removes the rows of its view table whatever the table's DROP clause says, so the view table's indexes hold an entry for exactly the rows it stores: a COUNT index agrees with a scan, and a UNIQUE index refuses a second row with a value a stored row holds."

[[test.results]]
value = "NONE"

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
value = "[{ count: 2 }]"

[[test.results]]
value = "2"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "1"

[[test.results]]
error = "Database index `v_u` already contains 1, with record `v:1`"

[[test.results]]
value = "[{ count: 1 }]"

*/

DEFINE TABLE src SCHEMALESS;
DEFINE TABLE v DROP AS SELECT a FROM src;
DEFINE INDEX v_u ON v FIELDS a UNIQUE;
DEFINE INDEX v_c ON v COUNT;
CREATE src:1 SET a = 1 RETURN NONE;
CREATE src:2 SET a = 2 RETURN NONE;
-- The COUNT index answers the first of each pair, and a scan the second: they agree.
SELECT count() FROM v GROUP ALL;
array::len(SELECT * FROM v);
DELETE src:2 RETURN NONE;
SELECT count() FROM v GROUP ALL;
array::len(SELECT * FROM v);
-- The view row `v:3` would hold the value `v:1` holds.
CREATE src:3 SET a = 1 RETURN NONE;
SELECT count() FROM v GROUP ALL;
